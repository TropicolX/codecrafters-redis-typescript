import net from 'node:net';
import { RESPDecoder } from './resp';
import { cleanupWaiter, errorString, resolvePortFromArgv } from './utils';
import { waiterBySocket } from './store';
import {
  BLPOP,
  ECHO,
  GET,
  LLEN,
  LPOP,
  LPUSH,
  LRANGE,
  PING,
  RPUSH,
  SET,
  TYPE,
  XADD,
  XRANGE,
  XREAD,
} from './command-handlers';

const server = net.createServer((socket) => {
  const decoder = new RESPDecoder();

  socket.on('data', (chunk: Buffer) => {
    const frames = decoder.push(chunk);

    for (const parts of frames) {
      if (!parts || parts.length === 0 || parts[0] == null) continue;
      const cmd = String(parts[0]).toUpperCase();

      switch (cmd) {
        case 'PING': {
          PING(socket, parts);
          break;
        }

        case 'ECHO': {
          ECHO(socket, parts);
          break;
        }

        case 'SET': {
          SET(socket, parts);
          break;
        }

        case 'GET': {
          GET(socket, parts);
          break;
        }

        case 'RPUSH': {
          RPUSH(socket, parts);
          break;
        }

        case 'LRANGE': {
          LRANGE(socket, parts);
          break;
        }

        case 'LPUSH': {
          LPUSH(socket, parts);
          break;
        }

        case 'LLEN': {
          LLEN(socket, parts);
          break;
        }

        case 'LPOP': {
          LPOP(socket, parts);
          break;
        }

        case 'BLPOP': {
          BLPOP(socket, parts);
          break;
        }

        case 'TYPE': {
          TYPE(socket, parts);
          break;
        }

        case 'XADD': {
          XADD(socket, parts);
          break;
        }

        case 'XRANGE': {
          XRANGE(socket, parts);
          break;
        }

        case 'XREAD': {
          XREAD(socket, parts);
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

  socket.on('close', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
  socket.on('end', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
  socket.on('error', () => {
    const w = waiterBySocket.get(socket);
    if (w) cleanupWaiter(w);
  });
});

const PORT = resolvePortFromArgv(process.argv, 6379);

server.listen(PORT, () => {
  console.log(`[your_program] Mini-Redis listening on ${PORT}`);
});
