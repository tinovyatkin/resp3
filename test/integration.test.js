import { RedisReadableStream } from '../resp3/readable.js';
import { RedisWritableStream } from '../resp3/writable.js';
import assert from 'assert';
import { once } from 'events';
import { randomBytes } from 'crypto';

it('Redis Streams RESP3 clients works together', async () => {
  const STREAM_NAME = 'test_stream_' + randomBytes(10).toString('hex');
  const TEST_OBJ = {
    a: 'bValue',
    b: 1,
    c: [1, 22, 3],
    d: {
      e: 'eB',
    },
  };

  const reader = new RedisReadableStream(STREAM_NAME);
  await once(reader, 'connect');
  reader.once('error', (err) => {
    throw err;
  });

  const writer = new RedisWritableStream(STREAM_NAME);
  await once(writer, 'connect');
  writer.once('error', (err) => {
    throw err;
  });
  writer.write(TEST_OBJ);

  let called = [];
  reader.on('data', (data) => called.push(data));
  writer.write({ byaka: 'buka' });

  await new Promise((resolve) => setTimeout(resolve, 5000));
  writer.destroy();
  reader.destroy();
  assert.strictEqual(called.length, 2);
});
