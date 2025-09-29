import net from 'node:net';
import { RESPDecoder } from './resp';
import {
  cleanupWaiter,
  errorString,
  resolvePortFromArgv,
  resolveReplicaOfFromArgv,
  startReplicaLink,
} from './utils';
import { waiterBySocket } from './store';
import {
  BLPOP,
  ECHO,
  GET,
  INFO,
  LLEN,
  LPOP,
  LPUSH,
  LRANGE,
  PING,
  REPLCONF,
  RPUSH,
  SET,
  TYPE,
  XADD,
  XRANGE,
  XREAD,
} from './command-handlers';
import type { ServerState } from './types';

const replicaOf = resolveReplicaOfFromArgv(process.argv);
const PORT = resolvePortFromArgv(process.argv, 6379);

const state: ServerState = {
  role: replicaOf ? ('slave' as const) : 'master',
  replicaOf,
};

if (state.role === 'master') {
  // Hardcode a 40-char ID per stage requirements (any alphanumeric 40 chars is fine)
  state.masterReplId = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb';
  state.masterReplOffset = 0;
}

if (state.role === 'slave' && state.replicaOf) {
  startReplicaLink(state.replicaOf, PORT);
}

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

        case 'REPLCONF': {
          REPLCONF(socket, parts, state);
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

        case 'INFO':
          INFO(socket, parts, state);
          break;

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

server.listen(PORT, () => {
  console.log(
    `[your_program] Mini-Redis listening on ${PORT} (role=${state.role})`
  );
});
