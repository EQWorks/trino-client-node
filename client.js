const http = require('https')
const { URL } = require('url')
const { createGunzip } = require('zlib')

const TrinoBodyStreamer = require('./body')
const { propagateDestroy } = require('./utils')


class TrinoClient {
  constructor({ host, port = 8446, username, password, name = 'QL' } = {}) {
    if (!host || !username || !password) {
      throw new Error('One, or several, of host, username or password missing!')
    }
    this.name = name
    // make auth not "console loggable"
    Object.defineProperty(this, 'auth', {
      enumerable: false,
      configurable: false,
      get: () => `${username}:${password}`,
    })
    this.host = host
    this.port = port
    // connection pool
    this.httpAgent = new http.Agent({
      keepAlive: true, // will keep socket arounf and send keep-alive header
      maxSockets: 1, // max sockets per host:port
      timeout: 30000, // will set timeout when the socket is created
      host,
      port,
    })
  }

  _request(query, stream, nextUri, isCancelled) {
    return new Promise((resolve, reject) => {
      const options = {
        agent: this.httpAgent,
        host: this.host,
        port: this.port,
        auth: this.auth,
        protocol: 'https:',
        timeout: 1000, // timeout before the socket is connected
        // signal: , // abort signal - from node v14
      }
      if (nextUri) {
        Object.assign(options, {
          path: (new URL(nextUri)).pathname,
          method: isCancelled ? 'DELETE' : 'GET',
        })
      } else {
        Object.assign(options, {
          headers: {
            'Content-Type': 'text/plain',
            'Accept-Encoding': 'gzip, identify',
            'X-Trino-Source': this.name,
          },
          path: '/v1/statement',
          method: 'POST',
        })
      }
      const req = http.request(options)
      req.once('error', (err) => {
        propagateDestroy(err, req)
        reject(err)
      })
      if (!nextUri) {
        req.write(query)
      }
      req.once('close', () => {
        if (req.destroyed) {
          return
        }
        // Indicates that the request is completed, or its underlying connection was
        // terminated prematurely (before the response completion).
        // reject if promise in pending status
        reject(new Error('Connection terminated'))
      })
      req.once('response', (res) => {
        res.once('error', (err) => {
          err.statusCode = res.statusCode
          propagateDestroy(err, res, [req])
        })
        req.once('error', (err) => {
          propagateDestroy(err, req, [res])
        })
        let uncompressedRes = res
        if (res.headers['content-encoding'] === 'gzip') {
          // TODO: use pipeline
          uncompressedRes = res.pipe(createGunzip())
          res.once('error', (err) => propagateDestroy(err, res, [uncompressedRes]))
          uncompressedRes.once('error', (err) => propagateDestroy(err, uncompressedRes, [res]))
        }
        const meta = {}
        const populateMeta = (key, value) => meta[key] = value
        const bubbleUpError = (err) => propagateDestroy(err, stream, [uncompressedRes])
        uncompressedRes.once('close', () => {
        // uncompressedRes.once('end', () => {
          uncompressedRes.unpipe(stream)
          stream.off('error', bubbleUpError)
          stream.off('meta', populateMeta)
          if (!res.complete) {
            return res.destroy(new Error('The connection was terminated while the message was still being sent'))
          }
          // error without body
          if (res.statusCode < 200 || res.statusCode >= 300) {
            // return reject(new Error(`Server returned status code: ${res.statusCode}`))
            return res.destroy(new Error(`Server returned: ${res.statusMessage}`))
          }
          if (isCancelled) {
            // return reject(new Error('Query successfully cancelled by user!'))
            return res.destroy(new Error('Query successfully cancelled by user!'))
          }
          resolve(meta)
        })
        // uncompressedRes.once('error', forwardError)
        uncompressedRes.pipe(stream, { end: false })
        stream.once('error', bubbleUpError)
        stream.on('meta', populateMeta)
      })
      // timeout after the socket is created and the req is sent
      req.setTimeout(10000)
      req.once('timeout', () => {
        const err = new Error('ETIMEDOUT')
        req.destroy(err)
      })
      // req.on('finish', () => console.log('request sent'))
      req.end()
    })
  }

  query(query) {
    const stream = new TrinoBodyStreamer()
    let isCancelled = false
    stream.cancel = () => isCancelled = true;
    (async () => {
      try {
        let i = 0
        let nextUri
        do {
          try {
            const meta = await this._request(query, stream, nextUri, isCancelled)
            i = 0
            nextUri = meta.nextUri
          } catch (err) {
            // If the client request returns an HTTP 503, that means the server was busy,
            // and the client should try again in 50-100 milliseconds
            if (err.statusCode === 503 && i < 10) {
              // retry w/ exp backoff and jitter. max 10s
              await new Promise(resolve => setTimeout(resolve, Math.random() * Math.min(10000, 100 * (2 ** i))))
              i += 1
              continue
            }
            throw err
          }
        } while (nextUri)
      } catch (err) {
        stream.destroy(err)
      }
      stream.end()
    })()
    return stream
  }
}

module.exports = TrinoClient
