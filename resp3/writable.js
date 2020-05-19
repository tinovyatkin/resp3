import { createConnection } from 'net';
import { Transform } from 'stream';

/**
 * Writable stream to write into Redis served stream
 */
export class RedisWritableStream extends Transform {
  #streamName;
  #host;
  #port;
  #password;
  #username;
  /** @type {import('net').Socket} */
  #socket;
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
    host = '127.0.0.1',
    port = 6379,
    password,
    username = 'default'
  ) {
    super({
      objectMode: true,
    });
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
      .once('connect', () => {
        // plumbing ourself into socket
        this.pipe(this.#socket);

        // send 'HELLO 3 AUTH username password' command
        // https://github.com/antirez/RESP3/blob/master/spec.md#the-hello-command-and-connection-handshake
        this.#socket.write(
          `HELLO 3${
            this.#password ? ` AUTH ${this.#username} ${this.#password}}` : ''
          }\r\n`
        ); // <- 3 is protocol version here, RESP3
      })
      .once('data', () => this.emit('connect', this.#socket))
      // re-emit errors
      .on('data', (data) => {
        if (data.startsWith('-ERR'))
          this.emit('error', new Error(data.slice(5)));
      })
      .on('error', (err) => this.emit('error', err))
      // auto reconnect
      .once('end', () => {
        if (this.autoReconnect) this._connect();
      });
  }

  _transform(data, encoding, callback) {
    // waiting for socket to be ready, automatic offline queue
    if (!this.#socket || this.#socket.connecting) {
      this.once('connect', () => this._transform(data, encoding, callback));
      return;
    }

    const cmd = [
      'XADD',
      this.#streamName,
      '*',
      ...Object.entries(data).flatMap(([k, v]) => [
        k,
        typeof v === 'string' ? v : JSON.stringify(v),
      ]),
    ];
    this.push(
      [
        `*${cmd.length}`,
        // convert all into blob strings
        ...cmd.flatMap((v) => [`$${Buffer.byteLength(v)}`, v]),
        '',
      ].join('\r\n')
    );

    setImmediate(callback);
  }

  _destroy() {
    this.autoReconnect = false;
    if (!this.#socket || this.#socket.destroyed) return;
    // sending QUIT command
    if (this.#socket.writable) {
      this.#socket.end('QUIT\r\n');
    }
    // consume response
    this.#socket.once('data', () => {
      this.#socket.destroy();
      super.destroy();
    });
  }
}
