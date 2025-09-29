import type { Socket } from 'node:net';

// Store can hold strings (with optional expiry) or lists.
export type Entry =
  | { type: 'string'; value: string; expiresAt?: number }
  | { type: 'list'; value: string[]; expiresAt?: number };

export type StreamEntry = { id: string; fields: Array<[string, string]> };
export type Stream = { entries: StreamEntry[] };

export type Waiter = {
  socket: Socket;
  keys: string[]; // keys this waiter is watching
  timer?: ReturnType<typeof setTimeout>; // timeout handle if any
  createdAt: number; // for FIFO (longest waiting first)
};

export type ParsedId = { ms: bigint; seq: bigint };

export type XReadWaiter = {
  socket: import('net').Socket;
  keys: string[];
  starts: ParsedId[]; // one per key, exclusive start
  timer?: NodeJS.Timeout;
};

export type Parts = (string | null)[];

export type Role = 'master' | 'slave';

export interface ServerState {
  role: Role; // default 'master' for now
}
