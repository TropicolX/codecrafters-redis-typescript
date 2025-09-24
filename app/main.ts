import net, { Socket } from 'node:net';
import { RESPDecoder } from './resp';

// Store can hold strings (with optional expiry) or lists.
type Entry =
  | { type: 'string'; value: string; expiresAt?: number }
  | { type: 'list'; value: string[]; expiresAt?: number };

type StreamEntry = { id: string; fields: Array<[string, string]> };
type Stream = { entries: StreamEntry[] };

const streams = new Map<string, Stream>(); // key -> stream

const store = new Map<string, Entry>();

// Time helpers
const nowMs = () => Date.now();
const isExpired = (e?: Entry) =>
  !!(e && e.expiresAt !== undefined && nowMs() >= e.expiresAt);

// RESP helpers
function simpleString(s: string): string {
  return `+${s}\r\n`;
}
function errorString(s: string): string {
  return `-${s}\r\n`;
}
function bulkString(s: string): string {
  const n = Buffer.byteLength(s);
  return `$${n}\r\n${s}\r\n`;
}
const NULL_BULK = '$-1\r\n';
const NULL_ARRAY = '*-1\r\n';
function integerReply(n: number): string {
  return `:${n}\r\n`;
}

// Get string value if present and not expired; lazily delete when expired
function getFreshString(key: string): string | undefined {
  const e = store.get(key);
  if (!e || e.type !== 'string') return undefined;
  if (isExpired(e)) {
    store.delete(key); // optional cleanup
    return undefined;
  }
  return e.value;
}

type Waiter = {
  socket: Socket;
  keys: string[]; // keys this waiter is watching
  timer?: ReturnType<typeof setTimeout>; // timeout handle if any
  createdAt: number; // for FIFO (longest waiting first)
};

// key -> FIFO of waiters
const blockedByKey = new Map<string, Waiter[]>();
// one waiter per socket (simplify)
const waiterBySocket = new Map<Socket, Waiter>();

function encodeArrayOfBulk(...items: string[]): string {
  let resp = `*${items.length}\r\n`;
  for (const s of items) resp += bulkString(s);
  return resp;
}

function removeWaiterFromKey(key: string, w: Waiter) {
  const q = blockedByKey.get(key);
  if (!q) return;
  const i = q.indexOf(w);
  if (i >= 0) q.splice(i, 1);
  if (q.length === 0) blockedByKey.delete(key);
}

function cleanupWaiter(w: Waiter) {
  // remove from all key queues
  for (const k of w.keys) removeWaiterFromKey(k, w);
  if (w.timer) clearTimeout(w.timer);
  waiterBySocket.delete(w.socket);
}

function serveWaitersForKey(key: string) {
  const entry = store.get(key);
  if (!entry || entry.type !== 'list') return;

  // As long as we have elements and waiters, serve oldest first (FIFO)
  let q = blockedByKey.get(key);
  while (q && q.length > 0 && entry.value.length > 0) {
    const w = q.shift()!; // longest waiting for this key
    // Fully remove this waiter from other keys & timer
    cleanupWaiter(w);

    // Pop from the left
    const elem = entry.value.shift()!;
    if (entry.value.length === 0) store.delete(key); // optional cleanup

    // Reply: ["key","elem"] as RESP array of 2 bulk strings
    w.socket.write(encodeArrayOfBulk(key, elem));

    // refresh queue ref (it may have been removed in cleanup if empty)
    q = blockedByKey.get(key);
  }
}

const MAX_SEQ = 18446744073709551615n; // Redis' max seq

function parseIdRaw(id: string): { ms: bigint; seq: bigint } {
  const [msStr, seqStr] = id.split('-', 2);
  const ms = BigInt(msStr);
  const seq = seqStr != null ? BigInt(seqStr) : 0n;
  return { ms, seq };
}

function normalizeStart(id: string): ParsedId {
  // "123" -> 123-0 ; "123-4" stays
  const [msStr, seqStr] = id.split('-', 2);
  return { ms: BigInt(msStr), seq: seqStr != null ? BigInt(seqStr) : 0n };
}

function normalizeEnd(id: string): ParsedId {
  // "123" -> 123-MAX ; "123-4" stays
  const [msStr, seqStr] = id.split('-', 2);
  return { ms: BigInt(msStr), seq: seqStr != null ? BigInt(seqStr) : MAX_SEQ };
}

function array(items: string[]): string {
  // items are already RESP-encoded chunks (bulk strings, nested arrays, etc.)
  return `*${items.length}\r\n${items.join('')}`;
}

type ParsedId = { ms: bigint; seq: bigint };

function cmpId(a: ParsedId, b: ParsedId): number {
  if (a.ms < b.ms) return -1;
  if (a.ms > b.ms) return 1;
  if (a.seq < b.seq) return -1;
  if (a.seq > b.seq) return 1;
  return 0;
}

function parseIdStrict(id: string): ParsedId {
  // explicit IDs only in this stage: "<ms>-<seq>"
  const [msStr, seqStr] = id.split('-', 2);
  if (!msStr || !seqStr) throw new Error('bad-id');
  const ms = BigInt(msStr);
  const seq = BigInt(seqStr);
  return { ms, seq };
}

function isZeroZero(id: ParsedId): boolean {
  return id.ms === 0n && id.seq === 0n;
}

function isMsStar(id: string): boolean {
  const [msStr, seqStr] = id.split('-', 2);
  return !!msStr && seqStr === '*';
}

function parseMsOnly(id: string): bigint {
  const [msStr] = id.split('-', 2);
  return BigInt(msStr);
}

function nowMsBigInt(): bigint {
  // current unix time in milliseconds (BigInt)
  return BigInt(Date.now());
}

// --- Blocking waiters ---
type XReadWaiter = {
  socket: import('net').Socket;
  keys: string[];
  starts: ParsedId[]; // one per key, exclusive start
  timer?: NodeJS.Timeout;
};

const waitersByKey = new Map<string, Set<XReadWaiter>>();

function registerWaiter(w: XReadWaiter) {
  for (const k of w.keys) {
    if (!waitersByKey.has(k)) waitersByKey.set(k, new Set());
    waitersByKey.get(k)!.add(w);
  }
}

function unregisterWaiter(w: XReadWaiter) {
  if (w.timer) clearTimeout(w.timer);
  for (const k of w.keys) {
    const set = waitersByKey.get(k);
    if (set) {
      set.delete(w);
      if (set.size === 0) waitersByKey.delete(k);
    }
  }
}

// Build the XREAD array payload (same shape as your non-blocking XREAD).
// Returns `null` if there are NO matches across all provided streams.
function buildXReadResponse(keys: string[], starts: ParsedId[]): string | null {
  const perStream: string[] = [];

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const start = starts[i];

    const s = streams.get(key);
    if (!s) continue;

    const matches = s.entries.filter((e) => cmpId(parseIdRaw(e.id), start) > 0);
    if (matches.length === 0) continue;

    const entryArrays = matches.map((e) => {
      const kv = e.fields.flatMap(([k, v]) => [bulkString(k), bulkString(v)]);
      return array([bulkString(e.id), array(kv)]);
    });

    perStream.push(array([bulkString(key), array(entryArrays)]));
  }

  if (perStream.length === 0) return null;
  return array(perStream);
}

// Notify waiters that something new arrived on `key`
function notifyXAddAppend(key: string) {
  const set = waitersByKey.get(key);
  if (!set || set.size === 0) return;

  // Copy to avoid mutation during iteration
  const pending = Array.from(set);
  for (const w of pending) {
    const resp = buildXReadResponse(w.keys, w.starts);
    if (resp !== null) {
      // Fulfill and remove this waiter
      try {
        w.socket.write(resp);
      } catch {}
      unregisterWaiter(w);
    }
  }
}

function resolveXReadStart(idRaw: string, key: string): ParsedId {
  if (idRaw !== '$') return parseIdRaw(idRaw); // your existing parser

  const s = streams.get(key);
  if (s && s.entries.length > 0) {
    return parseIdRaw(s.entries[s.entries.length - 1].id); // last ID at call time
  }
  // No stream or empty: start from 0-0; since there are no entries now,
  // we'll block, and the *next* append will be > 0-0.
  return { ms: 0n, seq: 0n };
}

const server = net.createServer((socket) => {
  const decoder = new RESPDecoder();

  socket.on('data', (chunk: Buffer) => {
    const frames = decoder.push(chunk); // ["COMMAND", ...args]

    for (const parts of frames) {
      if (!parts || parts.length === 0 || parts[0] == null) continue;
      const cmd = String(parts[0]).toUpperCase();

      switch (cmd) {
        case 'PING': {
          socket.write(simpleString('PONG'));
          break;
        }

        case 'ECHO': {
          const arg = parts[1];
          if (typeof arg !== 'string' || parts.length !== 2) {
            socket.write(
              errorString("ERR wrong number of arguments for 'echo' command")
            );
          } else {
            socket.write(bulkString(arg));
          }
          break;
        }

        // SET <key> <value> [PX <ms>]
        case 'SET': {
          const key = parts[1];
          const val = parts[2];
          if (typeof key !== 'string' || typeof val !== 'string') {
            socket.write(
              errorString("ERR wrong number of arguments for 'set' command")
            );
            break;
          }

          // Default: no expiry
          let expiresAt: number | undefined = undefined;

          // Parse optional options after <value>
          // Accepts: PX <ms> (case-insensitive). Ignore other options in this stage.
          for (let i = 3; i < parts.length; i++) {
            const token = parts[i];
            if (typeof token === 'string' && token.toUpperCase() === 'PX') {
              const msToken = parts[i + 1];
              if (typeof msToken !== 'string') {
                socket.write(errorString('ERR syntax error'));
                // Do not store; stop processing this command
                expiresAt = undefined;
                break;
              }
              const ms = Number(msToken);
              if (!Number.isFinite(ms) || !Number.isInteger(ms) || ms < 0) {
                socket.write(
                  errorString('ERR value is not an integer or out of range')
                );
                expiresAt = undefined;
                break;
              }
              expiresAt = nowMs() + ms;
              i++; // consume value after PX
            } else {
              // Ignore unknown options for this stage
            }
          }

          // Overwrite key with new value/expiry
          store.set(key, { type: 'string', value: val, expiresAt });
          socket.write(simpleString('OK'));
          break;
        }

        case 'GET': {
          const key = parts[1];
          if (typeof key !== 'string' || parts.length !== 2) {
            socket.write(
              errorString("ERR wrong number of arguments for 'get' command")
            );
            break;
          }
          const val = getFreshString(key);
          if (val === undefined) {
            socket.write(NULL_BULK); // missing or expired
          } else {
            socket.write(bulkString(val));
          }
          break;
        }

        // RPUSH <key> <element>  (for this stage: new list with one element)
        case 'RPUSH': {
          const key = parts[1];
          // All remaining args are elements to append
          const elements = parts.slice(2);

          // Validate: need at least one element, and all must be strings
          if (
            typeof key !== 'string' ||
            elements.length === 0 ||
            elements.some((e) => typeof e !== 'string')
          ) {
            socket.write(
              errorString("ERR wrong number of arguments for 'rpush' command")
            );
            break;
          }

          const asStrings = elements as string[];

          const entry = store.get(key);
          if (!entry) {
            // Create new list with all elements
            const list = [...asStrings];
            store.set(key, { type: 'list', value: list });
            socket.write(integerReply(list.length));
            serveWaitersForKey(key);
            break;
          }

          if (entry.type !== 'list') {
            socket.write(
              errorString(
                'WRONGTYPE Operation against a key holding the wrong kind of value'
              )
            );
            break;
          }

          // Append all elements to existing list
          entry.value.push(...asStrings);
          socket.write(integerReply(entry.value.length));
          serveWaitersForKey(key);
          break;
        }

        case 'LRANGE': {
          const key = parts[1];
          const startToken = parts[2];
          const stopToken = parts[3];
          if (
            typeof key !== 'string' ||
            typeof startToken !== 'string' ||
            typeof stopToken !== 'string' ||
            parts.length !== 4
          ) {
            socket.write(
              errorString("ERR wrong number of arguments for 'lrange' command")
            );
            break;
          }
          const start = Number(startToken);
          const stop = Number(stopToken);
          if (
            !Number.isInteger(start) ||
            !Number.isInteger(stop) ||
            Number.isNaN(start) ||
            Number.isNaN(stop)
          ) {
            socket.write(
              errorString('ERR value is not an integer or out of range')
            );
            break;
          }

          const entry = store.get(key);
          if (!entry || entry.type !== 'list' || isExpired(entry)) {
            // Missing, wrong type, or expired
            socket.write('*0\r\n'); // empty list
            break;
          }

          const list = entry.value;
          const n = list.length;

          // Normalize start/stop (allow negatives)
          let iStart = start >= 0 ? start : n + start;
          let iStop = stop >= 0 ? stop : n + stop;

          // If start > stop after normalization -> empty
          // Also handle out-of-range by clamping, but return empty if start >= n or stop < 0
          if (iStart < 0) iStart = 0;
          if (iStop < 0) iStop = -1; // ensures empty if negative after clamp

          if (iStart > iStop || iStart >= n || iStop < 0) {
            socket.write('*0\r\n');
            break;
          }

          if (iStop >= n) iStop = n - 1;

          // Build RESP array
          const slice = list.slice(iStart, iStop + 1);
          let resp = `*${slice.length}\r\n`;
          for (const elem of slice) resp += bulkString(elem);
          socket.write(resp);
          break;
        }

        case 'LPUSH': {
          const key = parts[1];
          const elements = parts.slice(2);

          if (
            typeof key !== 'string' ||
            elements.length === 0 ||
            elements.some((e) => typeof e !== 'string')
          ) {
            socket.write(
              errorString("ERR wrong number of arguments for 'lpush' command")
            );
            break;
          }

          const asStrings = elements as string[];

          const entry = store.get(key);

          if (!entry) {
            // Create new list: push left one-by-one (so reverse the provided order)
            const list = [...asStrings].reverse();
            store.set(key, { type: 'list', value: list });
            socket.write(integerReply(list.length));
            break;
          }

          if (entry.type !== 'list') {
            socket.write(
              errorString(
                'WRONGTYPE Operation against a key holding the wrong kind of value'
              )
            );
            break;
          }

          // Prepend multiple values preserving Redis semantics
          entry.value.unshift(...[...asStrings].reverse());
          socket.write(integerReply(entry.value.length));
          serveWaitersForKey(key);
          break;
        }

        case 'LLEN': {
          const key = parts[1];
          if (typeof key !== 'string' || parts.length !== 2) {
            socket.write(
              errorString("ERR wrong number of arguments for 'llen' command")
            );
            break;
          }

          const entry = store.get(key);
          if (!entry || entry.type !== 'list' || isExpired(entry)) {
            // Missing, wrong type, or expired
            socket.write(integerReply(0));
            break;
          }

          socket.write(integerReply(entry.value.length));
          break;
        }

        case 'LPOP': {
          const key = parts[1];
          const countToken = parts[2]; // optional

          // Validate args
          if (typeof key !== 'string' || parts.length < 2 || parts.length > 3) {
            socket.write(
              errorString("ERR wrong number of arguments for 'lpop' command")
            );
            break;
          }

          // Resolve entry
          const entry = store.get(key);
          // Treat missing / non-list / expired as empty
          const missing = !entry || entry.type !== 'list' || isExpired(entry);
          const hasCount = typeof countToken === 'string';

          if (missing) {
            if (hasCount) {
              // With count, return empty array
              socket.write('*0\r\n');
            } else {
              // Without count, return null bulk string
              socket.write(NULL_BULK);
            }
            break;
          }

          const list = entry.value;

          if (!hasCount) {
            // Pop one
            if (list.length === 0) {
              socket.write(NULL_BULK);
              break;
            }
            const v = list.shift() as string; // length > 0 so safe
            if (list.length === 0) store.delete(key); // optional cleanup
            socket.write(bulkString(v));
            break;
          }

          // Pop multiple
          // Validate count: positive integer
          const count = Number(countToken);
          if (!Number.isInteger(count) || count <= 0) {
            socket.write(
              errorString('ERR value is not an integer or out of range')
            );
            break;
          }

          const k = Math.min(count, list.length);
          const popped: string[] = list.splice(0, k);

          if (list.length === 0) store.delete(key); // optional cleanup

          // Return RESP Array of bulk strings
          const resp = encodeArrayOfBulk(...popped);
          socket.write(resp);
          break;
        }

        case 'BLPOP': {
          // BLPOP key [key ...] timeout
          if (parts.length < 3) {
            socket.write(
              errorString("ERR wrong number of arguments for 'blpop' command")
            );
            break;
          }

          // Last arg is timeout (seconds)
          const timeoutToken = parts[parts.length - 1];
          if (typeof timeoutToken !== 'string') {
            socket.write(errorString('ERR syntax error'));
            break;
          }
          const timeoutSec = Number(timeoutToken);
          if (!Number.isFinite(timeoutSec) || timeoutSec < 0) {
            socket.write(
              errorString('ERR timeout is not a non-negative number')
            );
            break;
          }

          // Keys are everything before timeout
          const keyTokens = parts.slice(1, parts.length - 1);
          const keys: string[] = [];
          for (const t of keyTokens) {
            if (typeof t !== 'string') {
              socket.write(errorString('ERR syntax error'));
              return;
            }
            keys.push(t);
          }

          // 1) Try immediate pop (in key order)
          let poppedKey: string | null = null;
          let poppedVal: string | null = null;
          for (const k of keys) {
            const e = store.get(k);
            if (e && e.type === 'list' && e.value.length > 0) {
              poppedKey = k;
              poppedVal = e.value.shift() as string;
              if (e.value.length === 0) store.delete(k);
              break;
            }
          }
          if (poppedKey !== null && poppedVal !== null) {
            socket.write(encodeArrayOfBulk(poppedKey, poppedVal));
            break;
          }

          // 2) Otherwise, block
          // If this socket already has a waiter, cancel it (one outstanding per socket)
          const existing = waiterBySocket.get(socket);
          if (existing) cleanupWaiter(existing);

          const waiter: Waiter = {
            socket,
            keys,
            createdAt: Date.now(),
          };

          // Register on each key
          for (const k of keys) {
            const q = blockedByKey.get(k);
            if (q) q.push(waiter);
            else blockedByKey.set(k, [waiter]);
          }
          waiterBySocket.set(socket, waiter);

          // Optional timeout (0 = block indefinitely)
          if (timeoutSec > 0) {
            waiter.timer = setTimeout(() => {
              // If still waiting, respond with null bulk string
              if (waiterBySocket.get(socket) === waiter) {
                cleanupWaiter(waiter);
                socket.write(NULL_ARRAY); // as per your spec
              }
            }, Math.ceil(timeoutSec * 1000)); // seconds → ms
          }

          break;
        }

        case 'TYPE': {
          if (parts.length !== 2 || typeof parts[1] !== 'string') {
            socket.write(
              errorString("ERR wrong number of arguments for 'type' command")
            );
            break;
          }
          const key = parts[1] as string;

          if (streams.has(key)) socket.write(simpleString('stream'));
          else if (store.has(key)) socket.write(simpleString('string'));
          else socket.write(simpleString('none'));
          break;
        }

        case 'XADD': {
          // XADD <key> <id> field value [field value ...]
          if (
            parts.length < 5 ||
            typeof parts[1] !== 'string' ||
            typeof parts[2] !== 'string'
          ) {
            socket.write(
              errorString("ERR wrong number of arguments for 'xadd' command")
            );
            break;
          }

          const key = parts[1] as string;
          const idRaw = parts[2] as string;

          // Ensure even number of field/value args
          if ((parts.length - 3) % 2 !== 0) {
            socket.write(errorString('ERR Unbalanced key/value pairs'));
            break;
          }

          // WRONGTYPE if key exists as a non-stream (adjust `db` to your store)
          if (store?.has?.(key) && !streams.has(key)) {
            socket.write(
              errorString(
                'WRONGTYPE Operation against a key holding the wrong kind of value'
              )
            );
            break;
          }

          // Create stream if missing
          if (!streams.has(key)) streams.set(key, { entries: [] });
          const stream = streams.get(key)!;

          let finalId: string;

          if (idRaw === '*') {
            // -------- Auto-generate ms & seq --------
            const now = nowMsBigInt();

            if (stream.entries.length === 0) {
              // first entry: use now-0
              finalId = `${now.toString()}-0`;
            } else {
              const lastParsed = parseIdStrict(
                stream.entries[stream.entries.length - 1].id
              );
              if (now > lastParsed.ms) {
                // clock advanced: start new ms bucket at seq 0
                finalId = `${now.toString()}-0`;
              } else if (now === lastParsed.ms) {
                // same ms: bump seq
                finalId = `${now.toString()}-${(
                  lastParsed.seq + 1n
                ).toString()}`;
              } else {
                // clock went backwards: keep monotonicity -> stick to last.ms and bump seq
                finalId = `${lastParsed.ms.toString()}-${(
                  lastParsed.seq + 1n
                ).toString()}`;
              }
            }
          } else if (idRaw.includes('-') && idRaw.endsWith('*')) {
            // -------- ms-* branch from previous stage (kept as-is) --------
            const [msStr] = idRaw.split('-', 2);
            const ms = BigInt(msStr);

            if (stream.entries.length > 0) {
              const lastParsed = parseIdStrict(
                stream.entries[stream.entries.length - 1].id
              );
              if (ms < lastParsed.ms) {
                socket.write(
                  errorString(
                    'ERR The ID specified in XADD is equal or smaller than the target stream top item'
                  )
                );
                break;
              }
            }

            let seq: bigint;
            if (stream.entries.length > 0) {
              const lastParsed = parseIdStrict(
                stream.entries[stream.entries.length - 1].id
              );
              seq =
                lastParsed.ms === ms
                  ? lastParsed.seq + 1n
                  : ms === 0n
                  ? 1n
                  : 0n;
            } else {
              seq = ms === 0n ? 1n : 0n;
            }
            finalId = `${ms.toString()}-${seq.toString()}`;
          } else {
            // -------- explicit id validation (previous stage, kept as-is) --------
            let parsed;
            try {
              parsed = parseIdStrict(idRaw);
            } catch {
              socket.write(
                errorString('ERR The ID specified in XADD is not valid')
              );
              break;
            }

            if (parsed.ms === 0n && parsed.seq === 0n) {
              socket.write(
                errorString(
                  'ERR The ID specified in XADD must be greater than 0-0'
                )
              );
              break;
            }

            if (stream.entries.length > 0) {
              const lastParsed = parseIdStrict(
                stream.entries[stream.entries.length - 1].id
              );
              if (parsed.ms < lastParsed.ms || cmpId(parsed, lastParsed) <= 0) {
                socket.write(
                  errorString(
                    'ERR The ID specified in XADD is equal or smaller than the target stream top item'
                  )
                );
                break;
              }
            }

            finalId = idRaw;
          }

          // Build fields preserving insertion order
          const fields: Array<[string, string]> = [];
          for (let i = 3; i < parts.length; i += 2) {
            fields.push([String(parts[i]), String(parts[i + 1])]);
          }

          // Append entry & return final ID as BULK STRING
          stream.entries.push({ id: finalId, fields });
          socket.write(bulkString(finalId));

          // Wake any blocked XREADs waiting on this stream
          notifyXAddAppend(key);
          break;
        }

        case 'XRANGE': {
          // XRANGE <key> <start> <end>
          if (
            parts.length !== 4 ||
            typeof parts[1] !== 'string' ||
            typeof parts[2] !== 'string' ||
            typeof parts[3] !== 'string'
          ) {
            socket.write(
              errorString("ERR wrong number of arguments for 'xrange' command")
            );
            break;
          }

          const key = parts[1] as string;
          const startRaw = String(parts[2]);
          const endRaw = String(parts[3]);

          // WRONGTYPE if key is a string value
          if (store?.has?.(key) && !streams.has(key)) {
            socket.write(
              errorString(
                'WRONGTYPE Operation against a key holding the wrong kind of value'
              )
            );
            break;
          }

          // Missing stream => empty array
          if (!streams.has(key)) {
            socket.write(array([]));
            break;
          }

          const start = normalizeStart(startRaw);
          const end = normalizeEnd(endRaw);

          const s = streams.get(key)!;

          // Filter entries inclusively by parsed id
          const result = s.entries.filter((e) => {
            const id = parseIdRaw(e.id);
            return cmpId(start, id) <= 0 && cmpId(id, end) <= 0;
          });

          // RESP: Array of [ BulkString(id), Array( BulkString(field), BulkString(value), ... ) ]
          const resp = result.map((e) => {
            const kv = e.fields.flatMap(([k, v]) => [
              bulkString(k),
              bulkString(v),
            ]);
            return array([bulkString(e.id), array(kv)]);
          });

          socket.write(array(resp));
          break;
        }

        case 'XREAD': {
          // Accept: XREAD [BLOCK <ms>] STREAMS <k1> [k2 ...] <id1> [id2 ...]
          if (parts.length < 4) {
            socket.write(
              errorString("ERR wrong number of arguments for 'xread' command")
            );
            break;
          }

          let idx = 1;
          let blockMs: number | null = null;

          // Optional BLOCK <ms>
          if (String(parts[idx]).toUpperCase() === 'BLOCK') {
            if (parts.length < idx + 2) {
              socket.write(errorString('ERR syntax error'));
              break;
            }
            const msNum = Number(parts[idx + 1]);
            if (!Number.isFinite(msNum) || msNum < 0) {
              socket.write(
                errorString('ERR value is not an integer or out of range')
              );
              break;
            }
            blockMs = msNum;
            idx += 2;
          }

          // STREAMS
          if (String(parts[idx]).toUpperCase() !== 'STREAMS') {
            socket.write(errorString('ERR syntax error'));
            break;
          }
          idx += 1;

          // Remaining args: keys..., ids...
          const remaining = parts.slice(idx).map(String);
          if (remaining.length < 2 || remaining.length % 2 !== 0) {
            socket.write(
              errorString("ERR unmatched keys/ids for 'xread' command")
            );
            break;
          }

          const half = remaining.length / 2;
          const keys = remaining.slice(0, half);
          const idsRaw = remaining.slice(half);

          // WRONGTYPE checks (if you keep strings in a separate store `db`)
          for (const k of keys) {
            if (store?.has?.(k) && !streams.has(k)) {
              socket.write(
                errorString(
                  'WRONGTYPE Operation against a key holding the wrong kind of value'
                )
              );
              return;
            }
          }

          const starts = idsRaw.map((id, i) => resolveXReadStart(id, keys[i]));

          // Try immediate read first
          const immediate = buildXReadResponse(keys, starts);
          if (immediate !== null) {
            socket.write(immediate);
            break;
          }

          // --- No matches right now ---
          if (blockMs === null) {
            // Non-blocking XREAD: return Null Array immediately
            socket.write(NULL_ARRAY); // '*-1\r\n'
            break;
          }

          // BLOCK specified:
          // BLOCK 0 -> wait indefinitely
          // BLOCK >0 -> wait up to blockMs
          const waiter: XReadWaiter = {
            socket,
            keys,
            starts,
            // timer set below (or left undefined for BLOCK 0)
            timer: undefined as unknown as NodeJS.Timeout,
          };

          // optional timeout only when blockMs > 0
          if (blockMs > 0) {
            // @ts-ignore
            waiter.timer = setTimeout(() => {
              try {
                socket.write(NULL_ARRAY);
              } catch {}
              unregisterWaiter(waiter);
            }, blockMs);
          }

          // Cleanup if client disconnects
          const onClose = () => unregisterWaiter(waiter);
          socket.once('close', onClose);
          socket.once('end', onClose);
          socket.once('error', onClose as any);

          // Park the waiter (no timer means BLOCK 0 -> wait forever)
          registerWaiter(waiter);
          break;
        }

        default: {
          socket.write(
            errorString(`ERR unknown command '${String(parts[0])}'`)
          );
          break;
        }
      }
    }
  });

  socket.on('close', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
  socket.on('end', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
  socket.on('error', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
});

server.listen(6379, () => {
  console.log('Mini-Redis listening on 6379');
});
