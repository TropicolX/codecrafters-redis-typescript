import * as net from 'net';
import { RESPDecoder } from './resp';

const store = new Map<string, string>(); // simple KV store (no expiry in this stage)

function bulkString(s: string): string {
  const n = Buffer.byteLength(s);
  return `$${n}\r\n${s}\r\n`;
}

// Uncomment this block to pass the first stage
const server = net.createServer((socket: net.Socket) => {
  // Handle connection
  const decoder = new RESPDecoder();

  socket.on('data', (chunk) => {
    const frames = decoder.push(chunk); // each frame is ["COMMAND", ...args]
    console.log(frames);
    for (const parts of frames) {
      if (!parts || parts.length === 0 || parts[0] == null) continue;

      const cmd = String(parts[0]).toUpperCase();

      if (cmd === 'PING') {
        // For this stage, simple PONG is fine.
        socket.write('+PONG\r\n');
        continue;
      }

      if (cmd === 'ECHO') {
        // ECHO <message> -> return <message> as RESP Bulk String
        const arg = parts[1]; // may be undefined or null
        if (typeof arg !== 'string') {
          socket.write("-ERR wrong number of arguments for 'echo' command\r\n");
        } else {
          const byteLen = Buffer.byteLength(arg);
          socket.write(`$${byteLen}\r\n${arg}\r\n`);
        }
        continue;
      }

      if (cmd === 'SET') {
        // SET <key> <value>
        const key = parts[1];
        const val = parts[2];
        if (typeof key !== 'string' || typeof val !== 'string') {
          socket.write("-ERR wrong number of arguments for 'set' command\r\n");
          continue;
        }
        // Ignore EX/PX and other options in this stage.
        store.set(key, val);
        socket.write('+OK\r\n'); // Simple String
        continue;
      }

      if (cmd === 'GET') {
        // GET <key>
        const key = parts[1];
        if (typeof key !== 'string') {
          socket.write("-ERR wrong number of arguments for 'get' command\r\n");
          continue;
        }
        const val = store.get(key);
        if (val === undefined) {
          socket.write('$-1\r\n'); // Null Bulk String
        } else {
          socket.write(bulkString(val)); // Bulk String
        }
        continue;
      }

      // Unknown command
      socket.write(`-ERR unknown command '${parts[0]}'\r\n`);
    }
  });
});

server.listen(6379, '127.0.0.1');
