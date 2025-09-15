import * as net from 'net';
import { RESPDecoder } from './resp';

// Uncomment this block to pass the first stage
const server = net.createServer((socket: net.Socket) => {
  // Handle connection
  const decoder = new RESPDecoder();

  socket.on('data', (chunk) => {
    const frames = decoder.push(chunk); // each frame is ["COMMAND", ...args]

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

      // You can expand here with SET/GET later
      socket.write(`-ERR unknown command '${parts[0]}'\r\n`);
    }
  });
});

server.listen(6379, '127.0.0.1');
