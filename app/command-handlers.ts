import type { Socket } from 'node:net';
import {
  array,
  buildXReadResponse,
  bulkString,
  cleanupWaiter,
  cmpId,
  encodeArrayOfBulk,
  errorString,
  getFreshString,
  integerReply,
  isExpired,
  normalizeEnd,
  normalizeStart,
  notifyXAddAppend,
  nowMs,
  nowMsBigInt,
  parseIdRaw,
  parseIdStrict,
  registerWaiter,
  resolveXReadStart,
  serveWaitersForKey,
  simpleString,
  unregisterWaiter,
} from './utils';
import {
  blockedByKey,
  NULL_ARRAY,
  NULL_BULK,
  store,
  streams,
  waiterBySocket,
} from './store';
import type { Parts, ServerState, Waiter, XReadWaiter } from './types';

export const PING = (socket: Socket, parts: Parts) => {
  socket.write(simpleString('PONG'));
};

export const ECHO = (socket: Socket, parts: Parts) => {
  const arg = parts[1];
  if (typeof arg !== 'string' || parts.length !== 2) {
    socket.write(
      errorString("ERR wrong number of arguments for 'echo' command")
    );
  } else {
    socket.write(bulkString(arg));
  }
};

export const SET = (socket: Socket, parts: Parts) => {
  const key = parts[1];
  const val = parts[2];
  if (typeof key !== 'string' || typeof val !== 'string') {
    socket.write(
      errorString("ERR wrong number of arguments for 'set' command")
    );
    return;
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
        return;
      }
      const ms = Number(msToken);
      if (!Number.isFinite(ms) || !Number.isInteger(ms) || ms < 0) {
        socket.write(
          errorString('ERR value is not an integer or out of range')
        );
        expiresAt = undefined;
        return;
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
  return;
};

export const GET = (socket: Socket, parts: Parts) => {
  const key = parts[1];
  if (typeof key !== 'string' || parts.length !== 2) {
    socket.write(
      errorString("ERR wrong number of arguments for 'get' command")
    );
    return;
  }
  const val = getFreshString(key);
  if (val === undefined) {
    socket.write(NULL_BULK); // missing or expired
  } else {
    socket.write(bulkString(val));
  }
  return;
};

export const RPUSH = (socket: Socket, parts: Parts) => {
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
    return;
  }

  const asStrings = elements as string[];

  const entry = store.get(key);
  if (!entry) {
    // Create new list with all elements
    const list = [...asStrings];
    store.set(key, { type: 'list', value: list });
    socket.write(integerReply(list.length));
    serveWaitersForKey(key);
    return;
  }

  if (entry.type !== 'list') {
    socket.write(
      errorString(
        'WRONGTYPE Operation against a key holding the wrong kind of value'
      )
    );
    return;
  }

  // Append all elements to existing list
  entry.value.push(...asStrings);
  socket.write(integerReply(entry.value.length));
  serveWaitersForKey(key);
};

export const LRANGE = (socket: Socket, parts: Parts) => {
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
    return;
  }
  const start = Number(startToken);
  const stop = Number(stopToken);
  if (
    !Number.isInteger(start) ||
    !Number.isInteger(stop) ||
    Number.isNaN(start) ||
    Number.isNaN(stop)
  ) {
    socket.write(errorString('ERR value is not an integer or out of range'));
    return;
  }

  const entry = store.get(key);
  if (!entry || entry.type !== 'list' || isExpired(entry)) {
    // Missing, wrong type, or expired
    socket.write('*0\r\n'); // empty list
    return;
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
    return;
  }

  if (iStop >= n) iStop = n - 1;

  // Build RESP array
  const slice = list.slice(iStart, iStop + 1);
  let resp = `*${slice.length}\r\n`;
  for (const elem of slice) resp += bulkString(elem);
  socket.write(resp);
};

export const LPUSH = (socket: Socket, parts: Parts) => {
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
    return;
  }

  const asStrings = elements as string[];

  const entry = store.get(key);

  if (!entry) {
    // Create new list: push left one-by-one (so reverse the provided order)
    const list = [...asStrings].reverse();
    store.set(key, { type: 'list', value: list });
    socket.write(integerReply(list.length));
    return;
  }

  if (entry.type !== 'list') {
    socket.write(
      errorString(
        'WRONGTYPE Operation against a key holding the wrong kind of value'
      )
    );
    return;
  }

  // Prepend multiple values preserving Redis semantics
  entry.value.unshift(...[...asStrings].reverse());
  socket.write(integerReply(entry.value.length));
  serveWaitersForKey(key);
};

export const LLEN = (socket: Socket, parts: Parts) => {
  const key = parts[1];
  if (typeof key !== 'string' || parts.length !== 2) {
    socket.write(
      errorString("ERR wrong number of arguments for 'llen' command")
    );
    return;
  }

  const entry = store.get(key);
  if (!entry || entry.type !== 'list' || isExpired(entry)) {
    // Missing, wrong type, or expired
    socket.write(integerReply(0));
    return;
  }

  socket.write(integerReply(entry.value.length));
};

export const LPOP = (socket: Socket, parts: Parts) => {
  const key = parts[1];
  const countToken = parts[2]; // optional

  // Validate args
  if (typeof key !== 'string' || parts.length < 2 || parts.length > 3) {
    socket.write(
      errorString("ERR wrong number of arguments for 'lpop' command")
    );
    return;
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
    return;
  }

  const list = entry.value;

  if (!hasCount) {
    // Pop one
    if (list.length === 0) {
      socket.write(NULL_BULK);
      return;
    }
    const v = list.shift() as string; // length > 0 so safe
    if (list.length === 0) store.delete(key); // optional cleanup
    socket.write(bulkString(v));
    return;
  }

  // Pop multiple
  // Validate count: positive integer
  const count = Number(countToken);
  if (!Number.isInteger(count) || count <= 0) {
    socket.write(errorString('ERR value is not an integer or out of range'));
    return;
  }

  const k = Math.min(count, list.length);
  const popped: string[] = list.splice(0, k);

  if (list.length === 0) store.delete(key); // optional cleanup

  // Return RESP Array of bulk strings
  const resp = encodeArrayOfBulk(...popped);
  socket.write(resp);
};

export const BLPOP = (socket: Socket, parts: Parts) => {
  // BLPOP key [key ...] timeout
  if (parts.length < 3) {
    socket.write(
      errorString("ERR wrong number of arguments for 'blpop' command")
    );
    return;
  }

  // Last arg is timeout (seconds)
  const timeoutToken = parts[parts.length - 1];
  if (typeof timeoutToken !== 'string') {
    socket.write(errorString('ERR syntax error'));
    return;
  }
  const timeoutSec = Number(timeoutToken);
  if (!Number.isFinite(timeoutSec) || timeoutSec < 0) {
    socket.write(errorString('ERR timeout is not a non-negative number'));
    return;
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
      return;
    }
  }
  if (poppedKey !== null && poppedVal !== null) {
    socket.write(encodeArrayOfBulk(poppedKey, poppedVal));
    return;
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
};

export const TYPE = (socket: Socket, parts: Parts) => {
  if (parts.length !== 2 || typeof parts[1] !== 'string') {
    socket.write(
      errorString("ERR wrong number of arguments for 'type' command")
    );
    return;
  }
  const key = parts[1] as string;

  if (streams.has(key)) socket.write(simpleString('stream'));
  else if (store.has(key)) socket.write(simpleString('string'));
  else socket.write(simpleString('none'));
};

export const XADD = (socket: Socket, parts: Parts) => {
  // XADD <key> <id> field value [field value ...]
  if (
    parts.length < 5 ||
    typeof parts[1] !== 'string' ||
    typeof parts[2] !== 'string'
  ) {
    socket.write(
      errorString("ERR wrong number of arguments for 'xadd' command")
    );
    return;
  }

  const key = parts[1] as string;
  const idRaw = parts[2] as string;

  // Ensure even number of field/value args
  if ((parts.length - 3) % 2 !== 0) {
    socket.write(errorString('ERR Unbalanced key/value pairs'));
    return;
  }

  // WRONGTYPE if key exists as a non-stream (adjust `db` to your store)
  if (store?.has?.(key) && !streams.has(key)) {
    socket.write(
      errorString(
        'WRONGTYPE Operation against a key holding the wrong kind of value'
      )
    );
    return;
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
        finalId = `${now.toString()}-${(lastParsed.seq + 1n).toString()}`;
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
        return;
      }
    }

    let seq: bigint;
    if (stream.entries.length > 0) {
      const lastParsed = parseIdStrict(
        stream.entries[stream.entries.length - 1].id
      );
      seq = lastParsed.ms === ms ? lastParsed.seq + 1n : ms === 0n ? 1n : 0n;
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
      socket.write(errorString('ERR The ID specified in XADD is not valid'));
      return;
    }

    if (parsed.ms === 0n && parsed.seq === 0n) {
      socket.write(
        errorString('ERR The ID specified in XADD must be greater than 0-0')
      );
      return;
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
        return;
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
  return;
};

export const XRANGE = (socket: Socket, parts: Parts) => {
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
    return;
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
    return;
  }

  // Missing stream => empty array
  if (!streams.has(key)) {
    socket.write(array([]));
    return;
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
    const kv = e.fields.flatMap(([k, v]) => [bulkString(k), bulkString(v)]);
    return array([bulkString(e.id), array(kv)]);
  });

  socket.write(array(resp));
  return;
};

export const XREAD = (socket: Socket, parts: Parts) => {
  // Accept: XREAD [BLOCK <ms>] STREAMS <k1> [k2 ...] <id1> [id2 ...]
  if (parts.length < 4) {
    socket.write(
      errorString("ERR wrong number of arguments for 'xread' command")
    );
    return;
  }

  let idx = 1;
  let blockMs: number | null = null;

  // Optional BLOCK <ms>
  if (String(parts[idx]).toUpperCase() === 'BLOCK') {
    if (parts.length < idx + 2) {
      socket.write(errorString('ERR syntax error'));
      return;
    }
    const msNum = Number(parts[idx + 1]);
    if (!Number.isFinite(msNum) || msNum < 0) {
      socket.write(errorString('ERR value is not an integer or out of range'));
      return;
    }
    blockMs = msNum;
    idx += 2;
  }

  // STREAMS
  if (String(parts[idx]).toUpperCase() !== 'STREAMS') {
    socket.write(errorString('ERR syntax error'));
    return;
  }
  idx += 1;

  // Remaining args: keys..., ids...
  const remaining = parts.slice(idx).map(String);
  if (remaining.length < 2 || remaining.length % 2 !== 0) {
    socket.write(errorString("ERR unmatched keys/ids for 'xread' command"));
    return;
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
    return;
  }

  // --- No matches right now ---
  if (blockMs === null) {
    // Non-blocking XREAD: return Null Array immediately
    socket.write(NULL_ARRAY); // '*-1\r\n'
    return;
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
  return;
};

export function INFO(socket: Socket, parts: Parts, state: ServerState) {
  if (parts.length >= 2 && String(parts[1]).toLowerCase() !== 'replication') {
    socket.write(
      errorString('ERR only replication section is supported in this stage')
    );
    return;
  }
  socket.write(bulkString(`role:${state.role}`)); // ← use state.role
}
