import type { Socket } from 'node:net';
import type { Entry, Stream, Waiter, XReadWaiter } from './types';
import { bufferFromHex } from './utils';

export const streams = new Map<string, Stream>(); // key -> stream

export const store = new Map<string, Entry>();

export const waitersByKey = new Map<string, Set<XReadWaiter>>();

export const blockedByKey = new Map<string, Waiter[]>();
// one waiter per socket (simplify)
export const waiterBySocket = new Map<Socket, Waiter>();

export const MAX_SEQ = 18446744073709551615n; // Redis' max seq

export const NULL_BULK = '$-1\r\n';
export const NULL_ARRAY = '*-1\r\n';

const EMPTY_RDB_HEX =
  // REDIS0009 + AUX fields + EOF + CRC64
  '524544495330303039fa0972656469732d76657205362e302e39fa0b72656469732d626974733c00ff' +
  'f09f35a6d9ab0f49'; // CRC64

export const EMPTY_RDB_BYTES = bufferFromHex(EMPTY_RDB_HEX);
