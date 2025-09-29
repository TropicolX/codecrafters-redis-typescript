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
  ServerState,
  Parts,
} from './types';
import type { Socket } from 'node:net';
import { RESPDecoder } from './resp';
import { GET, INFO, PSYNC, REPLCONF, SET } from './command-handlers';

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

export function dispatchCommand(
  socket: Socket,
  parts: Parts,
  state: ServerState,
  source: 'client' | 'replication' = 'client'
) {
  if (!parts || parts.length === 0) return;

  const cmd = String(parts[0]).toUpperCase();
  switch (cmd) {
    case 'PING' /* your existing PING inline or handler */:
      // If source === 'replication', master will never send us PING after sync in this challenge,
      // but blackhole socket would swallow replies anyway.
      // socket.write(simpleString('PONG'));
      break;

    case 'INFO':
      INFO(socket, parts, state);
      break;
    case 'REPLCONF':
      REPLCONF(socket, parts, state);
      break;
    case 'PSYNC':
      PSYNC(socket, parts, state);
      break;

    // Writes
    case 'SET':
      SET(socket, parts, state);
      break;
    case 'DEL':
      // DEL?.(socket, parts, state);
      break;
    case 'XADD':
      // XADD?.(socket, parts, state);
      break;
    // ... add other commands you support

    // Reads
    case 'GET':
      GET(socket, parts);
      break;
    // ...

    default:
      // optionally ignore unknowns
      break;
  }
}

export function encodeArrayRESP(parts: Parts): Buffer {
  const chunks: Buffer[] = [];
  // Array header
  chunks.push(Buffer.from(`*${parts.length}\r\n`, 'ascii'));

  for (const p of parts) {
    const s = typeof p === 'string' ? p : String(p);
    // Encode argument in UTF-8, compute byte length precisely
    const data = Buffer.from(s, 'utf8');
    chunks.push(Buffer.from(`$${data.length}\r\n`, 'ascii'));
    chunks.push(data);
    chunks.push(Buffer.from('\r\n', 'ascii'));
  }

  return Buffer.concat(chunks as any);
}

function startReplicaStreamConsumer(sock: net.Socket, state: ServerState) {
  let phase:
    | 'await_fullresync'
    | 'await_rdb_header'
    | 'await_rdb_body'
    | 'stream_cmds' = 'await_fullresync';
  let buf = Buffer.alloc(0);
  let rdbLen = 0;

  // Decoder for the command stream phase
  const decoder = new RESPDecoder();
  const sink = blackholeSocket(); // handlers will write to this, which discards replies

  sock.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk] as any);
    processBuffer();
  });

  function handleDecoded(
    parts: Parts,
    rawLen: number,
    state: ServerState,
    sock: import('net').Socket
  ) {
    if (!parts || parts.length === 0) return;

    const cmd = String(parts[0]).toUpperCase();

    if (
      cmd === 'REPLCONF' &&
      parts.length >= 3 &&
      String(parts[1]).toUpperCase() === 'GETACK'
    ) {
      // Reply with offset BEFORE counting this GETACK
      const ack = array([
        bulkString('REPLCONF'),
        bulkString('ACK'),
        bulkString(String(state.replicaOffset ?? 0)),
      ]);
      sock.write(ack);

      // Now include this GETACK command's bytes in the offset
      state.replicaOffset = (state.replicaOffset ?? 0) + rawLen;
      return;
    }

    // For any other command (e.g., PING, SET, etc.):
    // Count it first, then apply silently (no reply to master).
    state.replicaOffset = (state.replicaOffset ?? 0) + rawLen;
    dispatchCommand(sink, parts, state, 'replication');
  }

  function processBuffer() {
    while (true) {
      if (phase === 'await_fullresync') {
        const i = indexOfCrlf(buf);
        if (i < 0) return; // need more data
        const line = buf.slice(0, i).toString('utf8'); // e.g., +FULLRESYNC <id> 0
        buf = buf.slice(i + 2);
        // (We can optionally validate it starts with +FULLRESYNC)
        phase = 'await_rdb_header';
      }

      if (phase === 'await_rdb_header') {
        // Need: $<len>\r\n
        if (buf.length < 4) return;
        if (buf[0] !== 0x24 /* '$' */) return; // malformed, but for this challenge assume correct
        const crlfAt = indexOfCrlf(buf);
        if (crlfAt < 0) return;
        const lenStr = buf.slice(1, crlfAt).toString('ascii');
        rdbLen = Number(lenStr);
        buf = buf.slice(crlfAt + 2);
        phase = 'await_rdb_body';
      }

      if (phase === 'await_rdb_body') {
        if (buf.length < rdbLen) return; // wait for full RDB body
        // Consume the RDB bytes (we ignore content; state is assumed empty)
        buf = buf.slice(rdbLen);
        phase = 'stream_cmds';
        // loop to process any remaining bytes as commands
      }

      if (phase === 'stream_cmds') {
        // Feed whatever we have into the RESP decoder
        const frames = decoder.push(buf);
        buf = Buffer.alloc(0);
        for (const parts of frames) {
          const raw = encodeArrayRESP(parts); // re-encode *exactly* what we parsed
          handleDecoded(parts, raw.length, state, sock);
        }
        return; // nothing else to do until more data arrives
      }
    }
  }
}

function indexOfCrlf(b: Buffer): number {
  for (let i = 0; i + 1 < b.length; i++) {
    if (b[i] === 13 && b[i + 1] === 10) return i; // \r\n
  }
  return -1;
}

export function startReplicaLink(
  replicaOf: ReplicaOfConfig,
  listeningPort: number,
  state: ServerState
) {
  const master = net.createConnection(
    { host: replicaOf.host, port: replicaOf.port },
    () => {
      // 1) PING
      master.write(array([bulkString('PING')]));
    }
  );
  master.setNoDelay(true);

  // After PING response
  master.once('data', () => {
    // 2a) REPLCONF listening-port <PORT>
    master.write(
      array([
        bulkString('REPLCONF'),
        bulkString('listening-port'),
        bulkString(String(listeningPort)),
      ])
    );

    // After OK to listening-port
    master.once('data', () => {
      // 2b) REPLCONF capa eof capa psync2
      master.write(
        array([
          bulkString('REPLCONF'),
          bulkString('capa'),
          bulkString('eof'),
          bulkString('capa'),
          bulkString('psync2'),
        ])
      );

      // After OK to capa
      master.once('data', () => {
        // 3) PSYNC ? -1
        master.write(
          array([bulkString('PSYNC'), bulkString('?'), bulkString('-1')])
        );

        // --- From here: receive +FULLRESYNC <id> 0, then $<len>\r\n<bytes>, then commands ---
        startReplicaStreamConsumer(master, state);
      });
    });
  });

  master.on('error', () => {});
  master.on('close', () => {});

  return master;
}

export function bufferFromHex(hex: string): Buffer {
  const clean = hex.replace(/\s+/g, '').toLowerCase();
  return Buffer.from(clean, 'hex');
}

export function bufferFromBase64(b64: string): Buffer {
  return Buffer.from(b64.replace(/\s+/g, ''), 'base64');
}

/**
 * Send a raw-binary payload as: $<len>\r\n<bytes>
 * Note: no trailing CRLF after the bytes (this is NOT a RESP bulk string terminator).
 */
export function sendBulkBytes(socket: import('net').Socket, bytes: Buffer) {
  const hdr = Buffer.from(`$${bytes.length}\r\n`);
  socket.write(Buffer.concat([hdr, bytes] as any) as any);
}

export function encodeCommandForReplication(parts: Parts): string {
  return array(
    parts.map((p, i) =>
      bulkString(i === 0 ? String(p).toUpperCase() : String(p))
    )
  );
}

export function propagateToReplicas(
  state: ServerState,
  parts: Parts,
  source?: Socket
) {
  if (state.role !== 'master') return; // <- important
  if (!state.replicas || state.replicas.size === 0) return;

  const payload = encodeCommandForReplication(parts);
  for (const sock of state.replicas) {
    if (source && sock === source) continue;
    try {
      sock.write(payload);
    } catch {}
  }
}
export function isWriteCommand(cmd: string): boolean {
  switch (cmd) {
    case 'SET':
    case 'DEL':
    case 'XADD':
      // add others as you implement replication for them
      return true;
    default:
      return false;
  }
}

export function blackholeSocket(): Socket {
  // Only write() is used by our handlers; the rest can be no-ops.
  return {
    write: (_chunk: any) => true,
  } as Socket;
}
