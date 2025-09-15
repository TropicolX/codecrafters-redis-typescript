import net from 'node:net';
import { RESPDecoder } from './resp';

// Store value + optional expiry time (epoch ms)
type Entry = { value: string; expiresAt?: number };
const store = new Map<string, Entry>();

function nowMs() {
  return Date.now();
}

function isExpired(e: Entry | undefined): boolean {
  return !!(e && e.expiresAt !== undefined && nowMs() >= e.expiresAt);
}

function getFresh(key: string): string | undefined {
  const e = store.get(key);
  if (!e) return undefined;
  if (isExpired(e)) {
    store.delete(key);
    return undefined;
  }
  return e.value;
}

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

const server = net.createServer((socket) => {
  const decoder = new RESPDecoder();

  socket.on('data', (chunk) => {
    const frames = decoder.push(chunk); // each: ["COMMAND", ...args]

    for (const parts of frames) {
      if (!parts || parts.length === 0 || parts[0] == null) continue;

      const cmd = String(parts[0]).toUpperCase();

      // --- PING ---
      if (cmd === 'PING') {
        socket.write(simpleString('PONG'));
        continue;
      }

      // --- ECHO <message> ---
      if (cmd === 'ECHO') {
        const arg = parts[1];
        if (typeof arg !== 'string' || parts.length !== 2) {
          socket.write(
            errorString("ERR wrong number of arguments for 'echo' command")
          );
        } else {
          socket.write(bulkString(arg));
        }
        continue;
      }

      // --- SET <key> <value> [PX <ms>] ---
      if (cmd === 'SET') {
        const key = parts[1];
        const val = parts[2];
        if (typeof key !== 'string' || typeof val !== 'string') {
          socket.write(
            errorString("ERR wrong number of arguments for 'set' command")
          );
          continue;
        }

        // Default: no expiry
        let expiresAt: number | undefined = undefined;

        // Parse optional PX (case-insensitive), allow it anywhere after value in this stage
        // e.g., ["SET","k","v","PX","100"] or ["SET","k","v","px","100"]
        for (let i = 3; i < parts.length; i++) {
          const token = parts[i];
          if (typeof token === 'string' && token.toUpperCase() === 'PX') {
            const next = parts[i + 1];
            if (typeof next !== 'string') {
              socket.write(errorString('ERR syntax error'));
              continue;
            }
            const ms = Number(next);
            if (!Number.isFinite(ms) || ms < 0 || !Number.isInteger(ms)) {
              socket.write(
                errorString('ERR value is not an integer or out of range')
              );
              continue;
            }
            expiresAt = nowMs() + ms;
            i++; // skip the value we just consumed
          } else {
            // Ignore any other options in this stage
          }
        }

        store.set(key, { value: val, expiresAt });
        socket.write(simpleString('OK'));
        continue;
      }

      // --- GET <key> ---
      if (cmd === 'GET') {
        const key = parts[1];
        if (typeof key !== 'string' || parts.length !== 2) {
          socket.write(
            errorString("ERR wrong number of arguments for 'get' command")
          );
          continue;
        }
        const val = getFresh(key);
        if (val === undefined) {
          socket.write(NULL_BULK);
        } else {
          socket.write(bulkString(val));
        }
        continue;
      }

      // --- Unknown command ---
      socket.write(errorString(`ERR unknown command '${String(parts[0])}'`));
    }
  });

  socket.on('error', () => {
    /* ignore for this stage */
  });
});

server.listen(6379, () => {
  console.log('Mini-Redis listening on 6379');
});
