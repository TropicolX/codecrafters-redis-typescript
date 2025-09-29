import net from 'net';
import {
  blockedByKey,
  MAX_SEQ,
  store,
  streams,
  waiterBySocket,
  waitersByKey,
} from './store';
import type {
  Entry,
  Waiter,
  ParsedId,
  XReadWaiter,
  ReplicaOfConfig,
} from './types';
import type { Socket } from 'node:net';

// Time helpers
export const nowMs = () => Date.now();
export const isExpired = (e?: Entry) =>
  !!(e && e.expiresAt !== undefined && nowMs() >= e.expiresAt);

// RESP helpers
export function simpleString(s: string): string {
  return `+${s}\r\n`;
}
export function errorString(s: string): string {
  return `-${s}\r\n`;
}
export function bulkString(s: string): string {
  const n = Buffer.byteLength(s);
  return `$${n}\r\n${s}\r\n`;
}

export function integerReply(n: number): string {
  return `:${n}\r\n`;
}

// Get string value if present and not expired; lazily delete when expired
export function getFreshString(key: string): string | undefined {
  const e = store.get(key);
  if (!e || e.type !== 'string') return undefined;
  if (isExpired(e)) {
    store.delete(key); // optional cleanup
    return undefined;
  }
  return e.value;
}

// key -> FIFO of waiters

export function encodeArrayOfBulk(...items: string[]): string {
  let resp = `*${items.length}\r\n`;
  for (const s of items) resp += bulkString(s);
  return resp;
}

export function removeWaiterFromKey(key: string, w: Waiter) {
  const q = blockedByKey.get(key);
  if (!q) return;
  const i = q.indexOf(w);
  if (i >= 0) q.splice(i, 1);
  if (q.length === 0) blockedByKey.delete(key);
}

export function cleanupWaiter(w: Waiter) {
  // remove from all key queues
  for (const k of w.keys) removeWaiterFromKey(k, w);
  if (w.timer) clearTimeout(w.timer);
  waiterBySocket.delete(w.socket);
}

export function serveWaitersForKey(key: string) {
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

export function parseIdRaw(id: string): { ms: bigint; seq: bigint } {
  const [msStr, seqStr] = id.split('-', 2);
  const ms = BigInt(msStr);
  const seq = seqStr != null ? BigInt(seqStr) : 0n;
  return { ms, seq };
}

export function normalizeStart(id: string): ParsedId {
  // "123" -> 123-0 ; "123-4" stays
  const [msStr, seqStr] = id.split('-', 2);
  return { ms: BigInt(msStr), seq: seqStr != null ? BigInt(seqStr) : 0n };
}

export function normalizeEnd(id: string): ParsedId {
  // "123" -> 123-MAX ; "123-4" stays
  const [msStr, seqStr] = id.split('-', 2);
  return { ms: BigInt(msStr), seq: seqStr != null ? BigInt(seqStr) : MAX_SEQ };
}

export function array(items: string[]): string {
  // items are already RESP-encoded chunks (bulk strings, nested arrays, etc.)
  return `*${items.length}\r\n${items.join('')}`;
}

export function cmpId(a: ParsedId, b: ParsedId): number {
  if (a.ms < b.ms) return -1;
  if (a.ms > b.ms) return 1;
  if (a.seq < b.seq) return -1;
  if (a.seq > b.seq) return 1;
  return 0;
}

export function parseIdStrict(id: string): ParsedId {
  // explicit IDs only in this stage: "<ms>-<seq>"
  const [msStr, seqStr] = id.split('-', 2);
  if (!msStr || !seqStr) throw new Error('bad-id');
  const ms = BigInt(msStr);
  const seq = BigInt(seqStr);
  return { ms, seq };
}

export function nowMsBigInt(): bigint {
  // current unix time in milliseconds (BigInt)
  return BigInt(Date.now());
}

// --- Blocking waiters ---

export function registerWaiter(w: XReadWaiter) {
  for (const k of w.keys) {
    if (!waitersByKey.has(k)) waitersByKey.set(k, new Set());
    waitersByKey.get(k)!.add(w);
  }
}

export function unregisterWaiter(w: XReadWaiter) {
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
export function buildXReadResponse(
  keys: string[],
  starts: ParsedId[]
): string | null {
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
export function notifyXAddAppend(key: string) {
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

export function resolveXReadStart(idRaw: string, key: string): ParsedId {
  if (idRaw !== '$') return parseIdRaw(idRaw); // your existing parser

  const s = streams.get(key);
  if (s && s.entries.length > 0) {
    return parseIdRaw(s.entries[s.entries.length - 1].id); // last ID at call time
  }
  // No stream or empty: start from 0-0; since there are no entries now,
  // we'll block, and the *next* append will be > 0-0.
  return { ms: 0n, seq: 0n };
}

// ---- cli.ts (or top of your main file) ----
export function resolvePortFromArgv(argv: string[], fallback = 6379): number {
  let port: number | undefined;

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--port' || arg === '-p') {
      const next = argv[i + 1];
      port = Number(next);
      break;
    }
    if (arg.startsWith('--port=')) {
      port = Number(arg.split('=')[1]);
      break;
    }
  }

  if (!Number.isFinite(port!) || port! < 1 || port! > 65535) return fallback;
  return port!;
}

// utils.ts
function stripQuotes(s: string) {
  return s.replace(/^["']|["']$/g, '');
}

function parseHostPort(one: string, two?: string) {
  const first = stripQuotes(one).trim();
  if (two != null) {
    const host = stripQuotes(first).trim();
    const port = Number(stripQuotes(two).trim());
    if (host && Number.isFinite(port) && port >= 1 && port <= 65535)
      return { host, port };
    return undefined;
  }
  // single token that may contain space or colon
  const val = first;
  let host = '';
  let portStr = '';
  if (val.includes(' ')) {
    [host, portStr] = val.split(/\s+/, 2);
  } else if (val.includes(':')) {
    [host, portStr] = val.split(':', 2);
  } else {
    return undefined;
  }
  const port = Number(stripQuotes(portStr).trim());
  if (host && Number.isFinite(port) && port >= 1 && port <= 65535)
    return { host, port };
  return undefined;
}

export function resolveReplicaOfFromArgv(argv: string[]) {
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];

    // --replicaof=...
    if (arg.startsWith('--replicaof=')) {
      const v = arg.slice('--replicaof='.length);
      return parseHostPort(v);
    }

    if (arg === '--replicaof') {
      const next = argv[i + 1];
      const next2 = argv[i + 2];

      // cases:
      //  - --replicaof "localhost 6379"   -> next = "localhost 6379" (single token)
      //  - --replicaof localhost 6379     -> next = "localhost", next2 = "6379"
      if (!next) return undefined;

      // try two-token form first
      if (next2) {
        const parsed = parseHostPort(next, next2);
        if (parsed) return parsed;
      }
      // fall back to single-token split
      return parseHostPort(next);
    }
  }
  return undefined;
}

export function startReplicaLink(replicaOf: ReplicaOfConfig) {
  const master = net.createConnection(
    { host: replicaOf.host, port: replicaOf.port },
    () => {
      // Send PING as RESP Array: *1\r\n$4\r\nPING\r\n
      const ping = array([bulkString('PING')]);
      master.write(ping);
    }
  );

  // (Optional) you can log/ignore the response; not needed for the test
  master.on('data', () => {
    /* ignore master replies for this stage */
  });

  master.on('error', () => {
    /* ignore / optionally retry in later stages */
  });
  master.on('close', () => {
    /* optional: handle reconnect later */
  });

  // Small nicety for latency
  master.setNoDelay(true);

  return master; // keep a reference on state later if you want
}
