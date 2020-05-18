import { createConnection } from 'net';
import { hostname, userInfo } from 'os';
import { once } from 'events';
import { createHash } from 'crypto';
import { Transform } from 'stream';

export class RedisReadableStream extends Transform {
  #streamName;
  #host;
  #port;
  #password;
  #username;
  /** @type {import('net').Socket} */
  #socket;
  /** Client ID on Redis for unblocking */
  #clientId;
  #consumerGroup;
  #consumerName;
  /**
   * Flag indicating whether client should auto reconnect
   */
  autoReconnect = true;
  /**
   *
   * @param {string} streamName
   * @param {string} [host]
   * @param {number} [port]
   * @param {string} [password]
   * @param {string} [username]
   */
  constructor(
    streamName,
    consumerGroup = createHash('md5').update(hostname()).digest('hex'),
    consumerName = createHash('md5')
      .update(
        userInfo({ encoding: 'utf-8' }).username + new Date().toISOString()
      )
      .digest('hex'),
    host = '127.0.0.1',
    port = 6379,
    password,
    username = 'default'
  ) {
    super({
      objectMode: true,
      highWaterMark: 16,
      readableHighWaterMark: 1000,
    });
    this.#consumerGroup = consumerGroup;
    this.#consumerName = consumerName;
    this.#streamName = streamName;
    this.#host = host;
    this.#port = port;
    if (password) {
      this.#password = password;
      this.#username = username;
    }
    this._connect();
  }

  _connect() {
    this.#socket = createConnection({
      host: this.#host,
      port: this.#port,
    })
      .setEncoding('utf8')
      .setKeepAlive(true)
      // re-emit errors
      .on('data', (data) => {
        if (data.startsWith('-ERR'))
          this.emit('error', new Error(data.slice(5)));
      })
      .on('error', (err) => this.emit('error', err))
      // auto reconnect
      .once('end', () => {
        if (this.autoReconnect) this._connect();
      })
      .once('ready', async () => {
        // send 'HELLO 3 AUTH username password' command
        // https://github.com/antirez/RESP3/blob/master/spec.md#the-hello-command-and-connection-handshake
        this.#socket.write(
          `HELLO 3${
            this.#password ? ` AUTH ${this.#username} ${this.#password}}` : ''
          }\r\n`
        ); // <- 3 is protocol version here, RESP3
        await once(this.#socket, 'data');

        // Get out client id for unblocking
        this.#socket.write(`CLIENT ID\r\n`);
        const [clientId] = await once(this.#socket, 'data'); // it will be number like :440
        this.#clientId = parseInt(
          /^:(?<id>\d+)/.exec(clientId)?.groups?.id,
          10
        );

        // Create group
        // https://redis.io/topics/streams-intro#creating-a-consumer-group
        this.#socket.write(
          `XGROUP CREATE ${this.#streamName} ${
            this.#consumerGroup
          } $ MKSTREAM\r\n`
        );

        // plumbing ourself into socket
        this.#socket.pipe(this);
        this.emit('connect', this.#socket);

        // and start reading in blocking mode with infinite timeout, only new messages
        this.#socket.once('data', () => this._readMore());
      });
  }

  _readMore() {
    if (!this.autoReconnect) return;
    if (!this.#socket || this.#socket.connecting) {
      this.once('connect', () => this._readMore());
      return;
    }
    setImmediate(() => {
      this.#socket.write(
        `XREADGROUP GROUP ${this.#consumerGroup} ${
          this.#consumerName
        } BLOCK 0 COUNT 1 STREAMS ${this.#streamName} >\r\n`
      );
    });
  }

  _transform(data, encoding, callback) {
    // converting data
    /*
    @see {@link https://github.com/antirez/RESP3/blob/master/spec.md}
    %1 - % is map
    $11
    helloStream
    *1 - * is array
    *2
    $15
    1589759476040-0
    *2
    $4
    test
    $6
    value1
     */

    //convert strings first
    const chunk = data.split('\r\n').filter((v, i, arr) => {
      const t = /^\$(?<len>\d+)$/.exec(v);
      if (!t) return true;
      // make sure length is correct
      if (arr[i + 1].length !== parseInt(t.groups?.len, 10))
        this.emit(
          'error',
          `Wrong length of string ${arr[i + 1]}, expected ${t.groups.len}`
        );
      return false;
    });
    // as we are reading by 1 message always, then we can use optimized parsing
    // length of [field, value] array is at #5
    const msg = /^\*(?<len>\d+)$/.exec(chunk[5]);
    // just make sure that stream is ours
    if (msg?.groups?.len > '0' && chunk[1] === this.#streamName) {
      const len = parseInt(msg.groups?.len, 10);
      const obj = Object.fromEntries(
        chunk
          .slice(6, 6 + len)
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
      this.push(obj);
      this._readMore();
    }
    setImmediate(callback);
  }

  _destroy(error, callback) {
    this.autoReconnect = false;
    if (!this.#socket || this.#socket.destroyed) return;
    // unblocking out client from another connection
    if (Number.isInteger(this.#clientId)) {
      const unblockingSocket = createConnection({
        host: this.#host,
        port: this.#port,
      })
        .setEncoding('utf-8')
        .once('ready', () => {
          unblockingSocket.write(`CLIENT UNBLOCK ${this.#clientId}\r\n`);
        })
        .once('data', () => {
          unblockingSocket.write(`QUIT\r\n`);
        });
    }

    // gracefully shutdown
    Promise.race([
      once(this.#socket, 'data'),
      once(this.#socket, 'error'),
      once(this.#socket, 'timeout'),
    ])
      .then(() => {
        // sending QUIT command
        this.#socket.write('QUIT\r\n');
        super.destroy();
        setImmediate(callback);
      })
      .catch((err) => {
        this.emit('error', err);
        callback(err);
      });
  }
}
