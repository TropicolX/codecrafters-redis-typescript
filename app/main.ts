import net, { Socket } from 'node:net';
import { RESPDecoder } from './resp';

// Store can hold strings (with optional expiry) or lists.
type Entry =
  | { type: 'string'; value: string; expiresAt?: number }
  | { type: 'list'; value: string[]; expiresAt?: number };

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
          let resp = `*${popped.length}\r\n`;
          for (const elem of popped) resp += bulkString(elem);
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
