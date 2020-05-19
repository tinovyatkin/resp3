/**
 * Based on IORedis author comment
 * @see {@link https://github.com/luin/ioredis/issues/747#issuecomment-500735545}
 */

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

  // write objects to Redis stream
  for await (const line of createInterface(
    createReadStream('./benchmark/data/convertcsv.json', 'utf-8')
  )) {
    const res = /^\s*(?<json>{.+}),?\s*$/.exec(line);
    if (res) JSON.parse(res.groups.json);
  }
}

runAndReport('native stream implementation', run);
