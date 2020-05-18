import { RedisReadableStream } from './resp3/readable.js';
export async function connect() {
  const stream = new RedisReadableStream('helloStream');
  stream.on('error', console.error.bind(console));
  let i = 0;
  for await (const data of stream) {
    i++;
    console.log(data);
    if (i == 3) break;
  }
  stream.destroy();
}

connect();
