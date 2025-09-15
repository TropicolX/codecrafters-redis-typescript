import * as net from 'net';

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on('data', (data: Buffer) => {
    const message = data.toString();
    if (message.startsWith('PING')) {
      socket.write('+PONG\r\n');
    }
  });
});

server.listen(6379, '127.0.0.1');
