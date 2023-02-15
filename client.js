const http = require('https')
const { URL } = require('url')
const { createGunzip } = require('zlib')
const { pipeline } = require('stream')

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

  _request(query, bodyStream, nextUri, isCancelled, headers) {
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
            ...headers,
          },
          path: '/v1/statement',
          method: 'POST',
        })
      }
      const req = http.request(options)
      req.once('error', (err) => {
        propagateDestroy(err, { src: req })
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
          propagateDestroy(err, { src: res, dest: [req, bodyStream] })
        })
        req.once('error', (err) => {
          propagateDestroy(err, { src: req, dest: [res] })
        })
        let uncompressedRes = res
        if (res.headers['content-encoding'] === 'gzip') {
          uncompressedRes = pipeline(res, createGunzip(), () => {})
        }
        const meta = {}
        const populateMeta = (key, value) => meta[key] = value
        const bubbleUpError = (err) => propagateDestroy(err, { src: bodyStream, dest: [uncompressedRes] })
        uncompressedRes.once('close', () => {
          uncompressedRes.unpipe(bodyStream)
          bodyStream.off('error', bubbleUpError)
          if (!res.complete) {
            return res.destroy(new Error('The connection was terminated while the message was still being sent'))
          }
          // error without body
          if (res.statusCode < 200 || res.statusCode >= 300) {
            return res.destroy(new Error(`Server returned: ${res.statusMessage}`))
          }
          if (isCancelled) {
            return res.destroy(new Error('Query successfully cancelled by user!'))
          }
        })
        bodyStream.once('error', bubbleUpError)
        bodyStream.on('meta', populateMeta)
        bodyStream.once('done', () => {
          bodyStream.off('meta', populateMeta)
          resolve(meta)
        })
        uncompressedRes.pipe(bodyStream, { end: false })
      })
      // timeout after the socket is created and the req is sent
      req.setTimeout(10000)
      req.once('timeout', () => {
        const err = new Error('ETIMEDOUT')
        req.destroy(err)
      })
      req.end()
    })
  }

  query(opts) {
    let query
    let meta_callback
    let columns_callback
    let error_callback
    let headers = {}
    if (typeof opts === 'object') {
      query = opts.query
      meta_callback = opts.meta
      columns_callback = opts.columns
      error_callback = opts.error
      if (opts.catalog) {
        headers['X-Trino-Catalog'] = opts.catalog;
        if (opts.schema) {
          headers['X-Trino-Schema'] = opts.schema;
        }
      }
    }else {
      query = opts
    }
    const bodyStream = new TrinoBodyStreamer()
    let isCancelled = false
    bodyStream.cancel = () => isCancelled = true;
    (async () => {
      try {
        let i = 0
        let nextUri
        let meta
        do {
          try {
            meta = await this._request(query, bodyStream, nextUri, isCancelled, headers)
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
        if (meta_callback) {
          meta_callback(meta)
        }
        if (columns_callback && meta.columns) {
          columns_callback(meta.columns)
        }
        if (error_callback && meta.error) {
          error_callback(meta.error)
        }
      } catch (err) {
        bodyStream.destroy(err)
      }
      bodyStream.end()
    })()
    return bodyStream
  }
}

module.exports = TrinoClient
