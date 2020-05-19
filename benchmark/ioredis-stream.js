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
import Redis from 'ioredis';
import { createHash } from 'crypto';
import { hostname, userInfo } from 'os';

async function reading(
  reader,
  consumerName,
  consumerGroup,
  STREAM_NAME,
  output
) {
  const reply = await reader.xreadgroup(
    'GROUP',
    consumerGroup,
    consumerName,
    'BLOCK',
    '0',
    'COUNT',
    '1',
    'STREAMS',
    STREAM_NAME,
    '>'
  );
  /*
  console.dir(reply, { depth: 4, color: true });

    The response is completely unparsed, like this:
    [
      [
        'test_stream_8a5ef84df6033ca95ccd',
        [
          [
            '1589863106037-0',
            [
              'seq',
              '16',
              'name',
              '{"first":"Virgie","last":"Walsh"}',
              'age',
              '18',
              'street',
              'Mifde Park',
              'city',
              'Leduuc',
              'state',
              'GA',
              'zip',
              '29763',
              'dollar',
              '$764.34',
              'pick',
              'WHITE',
              'date',
              '01/31/1978'
            ]
          ]
        ]
      ]
    ]

  */

  // this code is from our Readable stream, so, performance is the same
  const chunk = reply[0][1][0][1];
  // console.dir(chunk, { depth: 4, color: true });
  const obj = Object.fromEntries(
    chunk
      // split into pairs
      .reduce((result, value, index, array) => {
        if (index % 2 === 0) result.push(array.slice(index, index + 2));
        return result;
      }, [])
      // unwrap JSON in values
      .map(([k, v]) => {
        if (/^\s*(\{|\[)/.test(v)) {
          try {
            return [k, JSON.parse(v)];
          } catch {}
        }
        return [k, v];
      })
  );
  output.write(JSON.stringify(obj));
  output.write('\r\n');
  if (obj.seq < 10000)
    return reading(reader, consumerName, consumerGroup, STREAM_NAME, output);
}

async function run() {
  const STREAM_NAME = 'test_stream_' + randomBytes(10).toString('hex');

  // creating consumer group and channel
  const consumerGroup = createHash('md5').update(hostname()).digest('hex');
  const consumerName = createHash('md5')
    .update(userInfo({ encoding: 'utf-8' }).username + new Date().toISOString())
    .digest('hex');
  const reader = new Redis();
  const writer = reader.duplicate();
  await reader.xgroup('CREATE', STREAM_NAME, consumerGroup, '$', 'MKSTREAM');
  const flushing = reading(
    reader,
    consumerName,
    consumerGroup,
    STREAM_NAME,
    createWriteStream('./benchmark/ioredis-result.json', {
      encoding: 'utf-8',
    })
  );

  // write objects to Redis stream
  for await (const line of createInterface(
    createReadStream('./benchmark/data/convertcsv.json', 'utf-8')
  )) {
    const res = /^\s*(?<json>{.+}),?\s*$/.exec(line);
    if (res)
      writer.xadd(
        STREAM_NAME,
        '*',
        ...Object.entries(JSON.parse(res.groups.json)).flatMap(([k, v]) => [
          k,
          typeof v === 'string' ? v : JSON.stringify(v),
        ])
      );
  }
  writer.quit();
  await flushing;
  reader.quit();
}

runAndReport('IORedis stream implementation', run);
