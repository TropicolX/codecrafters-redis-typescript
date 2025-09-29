import type { Socket } from 'node:net';
import type { Entry, Stream, Waiter, XReadWaiter } from './types';

export const streams = new Map<string, Stream>(); // key -> stream

export const store = new Map<string, Entry>();

export const waitersByKey = new Map<string, Set<XReadWaiter>>();

export const blockedByKey = new Map<string, Waiter[]>();
// one waiter per socket (simplify)
export const waiterBySocket = new Map<Socket, Waiter>();

export const MAX_SEQ = 18446744073709551615n; // Redis' max seq

export const NULL_BULK = '$-1\r\n';
export const NULL_ARRAY = '*-1\r\n';
