import { RedisReadableStream, RedisWritableStream } from '../index.js';
import { runAndReport } from './lib/measure-and-report.js';
import { createReadStream, createWriteStream } from 'fs';
import { createInterface } from 'readline';
import { randomBytes } from 'crypto';
import { Transform, pipeline } from 'stream';
import { promisify } from 'util';
import { once } from 'events';

async function run() {
  const STREAM_NAME = 'test_stream_' + randomBytes(10).toString('hex');

  const reader = new RedisReadableStream(STREAM_NAME);
  await once(reader, 'connect');
  // reads, stringifies and stores in files
  const flushing = promisify(pipeline)(
    reader,
    new Transform({
      writableObjectMode: true,
      readableObjectMode: false,
      transform(chunk, encoding, callback) {
        this.push(JSON.stringify(chunk));
        this.push('\r\n');
        callback();
        if (chunk.seq == 10000) {
          setImmediate(() => reader.end());
        }
      },
    }),
    createWriteStream('./benchmark/native-streams-result.json', {
      encoding: 'utf-8',
    })
  );

  // write objects to Redis stream
  const writer = new RedisWritableStream(STREAM_NAME);
  for await (const line of createInterface(
    createReadStream('./benchmark/data/convertcsv.json', 'utf-8')
  )) {
    const res = /^\s*(?<json>{.+}),?\s*$/.exec(line);
    if (res) writer.write(JSON.parse(res.groups.json));
  }
  writer.once('drain', () => writer.end());

  return flushing;
}

runAndReport('native stream implementation', run);
