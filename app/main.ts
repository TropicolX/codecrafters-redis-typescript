import * as net from 'net';

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on('data', (data: Buffer) => {
    const message = data.toString();
    console.log(message);
  });
});

server.listen(6379, '127.0.0.1');
