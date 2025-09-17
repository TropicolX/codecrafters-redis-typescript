import net from 'node:net';
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

  socket.on('error', () => {
    /* ignore for this stage */
  });
});

server.listen(6379, () => {
  console.log('Mini-Redis listening on 6379');
});
