import net from 'node:net';
import { RESPDecoder } from './resp';

// Store can hold either strings (for SET/GET) or lists (for RPUSH)
type Entry =
  | { type: 'string'; value: string }
  | { type: 'list'; value: string[] };
const store = new Map<string, Entry>();

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

const server = net.createServer((socket) => {
  const decoder = new RESPDecoder();

  socket.on('data', (chunk: Buffer) => {
    const frames = decoder.push(chunk); // ["COMMAND", ...args]

    for (const parts of frames) {
      if (!parts || parts.length === 0 || parts[0] == null) continue;
      const cmd = String(parts[0]).toUpperCase();

      switch (cmd) {
        // ---- PING ----
        case 'PING': {
          socket.write(simpleString('PONG'));
          break;
        }

        // ---- ECHO ----
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

        // ---- SET ----
        case 'SET': {
          const key = parts[1];
          const val = parts[2];
          if (typeof key !== 'string' || typeof val !== 'string') {
            socket.write(
              errorString("ERR wrong number of arguments for 'set' command")
            );
            break;
          }
          store.set(key, { type: 'string', value: val });
          socket.write(simpleString('OK'));
          break;
        }

        // ---- GET ----
        case 'GET': {
          const key = parts[1];
          if (typeof key !== 'string') {
            socket.write(
              errorString("ERR wrong number of arguments for 'get' command")
            );
            break;
          }
          const entry = store.get(key);
          if (!entry || entry.type !== 'string') {
            socket.write(NULL_BULK);
          } else {
            socket.write(bulkString(entry.value));
          }
          break;
        }

        // ---- RPUSH ----
        case 'RPUSH': {
          const key = parts[1];
          const element = parts[2];
          if (typeof key !== 'string' || typeof element !== 'string') {
            socket.write(
              errorString("ERR wrong number of arguments for 'rpush' command")
            );
            break;
          }

          let entry = store.get(key);
          if (!entry) {
            // Stage requirement: only need to handle creating a new list with one element
            const newList = [element];
            store.set(key, { type: 'list', value: newList });
            socket.write(integerReply(newList.length)); // :1\r\n
          } else if (entry.type === 'list') {
            // Sneak peek at later stage: append to existing list
            entry.value.push(element);
            socket.write(integerReply(entry.value.length));
          } else {
            socket.write(
              errorString(
                'WRONGTYPE Operation against a key holding the wrong kind of value'
              )
            );
          }
          break;
        }

        // ---- Unknown ----
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
