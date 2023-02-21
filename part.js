const { createGzip } = require('zlib')
const { Readable, Writable } = require('stream')

const { propagateDestroy } = require('./utils')


class PartStreamer extends Writable {
  constructor({ partHandler, partSizeMB }) {
    super({ objectMode: true })
    this._partHandler = partHandler // cb
    this._targetPartBytes = partSizeMB * (2 ** 20) // in MB
    this._parts = []
    this._partsSize = []
    this._isPartInit = false
    this._reject = undefined
  }

  _writeToPart(chunk, isEnd = false) {
    chunk = Buffer.from(chunk)
    const part = this._parts[this._parts.length - 1]
    return new Promise((resolve, reject) => {
      this._reject = reject
      const readableHander = () => {
        let data
        let readableHasCapacity = false
        while ((data = part.gzipStream.read()) !== null) {
          part.partBytes += data.length
          readableHasCapacity = part.partStream.push(data)
          if (!readableHasCapacity) {
            break
          }
        }
        if (part.gzipStream.writableLength === 0 && part.gzipStream.readableLength === 0) {
          if (!isEnd) {
            part.gzipStream.off('readable', readableHander)
            this._reject = undefined
            resolve()
          }
          return
        }
        // handle backpressure
        if (!readableHasCapacity) {
          part.gzipStream.off('readable', readableHander)
          part.partStream.once('_read', () => {
            part.gzipStream.on('readable', readableHander)
          })
        }
      }
      part.gzipStream.on('readable', readableHander)
      if (isEnd) {
        part.gzipStream.once('end', () => {
          part.gzipStream.off('readable', readableHander)
          part.partStream.push(null)
          // in case not already resolved (i.e. no data)
          this._reject = undefined
          resolve()
        })
        part.gzipStream.end(chunk)
      } else {
        part.gzipStream.write(chunk)
      }
    })
  }

  async _initPart() {
    if (this._isPartInit) {
      return
    }
    const gzipStream = createGzip({ chunkSize: 1024 * 32, flush: 1 })
    const partStream = new Readable({
      // highWaterMark: 1024,
      read() {
        this.emit('_read')
      },
    })
    gzipStream.once('error', (err) => {
      if (this._reject) {
        this._reject(err)
        this._reject = undefined
      }
      propagateDestroy(err, { src: gzipStream, dest: [partStream, this] })
    })
    partStream.once('error', (err) => {
      if (this._reject) {
        this._reject(err)
        this._reject = undefined
      }
      propagateDestroy(err, { src: partStream, dest: [gzipStream, this] })
    })
    const part = {
      gzipStream,
      partStream,
      partBytes: 0,
      partIndex: this._parts.length,
      partStart: this._parts.length ? this._parts[this._parts.length - 1].partEnd + 1 : 0,
      partEnd: undefined,
    }
    this._parts.push(part)
    // call handler (sync so setup can happen before pushing data to stream) with part stream
    this._partHandler(part.partIndex, part.partStream, this._partsSize)
    await this._writeToPart('[')
    this._isPartInit = true
  }

  async _endPart() {
    if (!this._isPartInit) {
      return
    }
    const { partStream, partIndex, partBytes, partStart, partEnd } = this._parts[this._parts.length - 1]
    await this._writeToPart(']', true)
    this._isPartInit = false
    partStream.emit('done', { partIndex, partBytes, partStart, partEnd })
  }

  async _write(chunk, _, cb) {
    try {
      if (!this._isPartInit) {
        this._partsSize[this._parts.length] = 0
        await this._initPart()
      } else {
        await this._writeToPart(',')
      }
      await this._writeToPart(JSON.stringify(chunk))
      const part = this._parts[this._parts.length - 1]
      this._partsSize[this._parts.length - 1] += 1
      part.partEnd = part.partEnd === undefined ? part.partStart : part.partEnd + 1
      if (part.partBytes >= this._targetPartBytes) {
        await this._endPart()
      }
      cb(null)
    } catch (err) {
      cb(err)
    }
  }

  async _final(cb) {
    try {
      await this._endPart()
      cb(null)
    } catch (err) {
      cb(err)
    }
  }

  _destroy(err, cb) {
    if (!err) {
      return cb(null)
    }
    if (this._reject) {
      this._reject(err)
      this._reject = undefined
    }
    // will propagate to all streams
    this._parts.forEach(({ gzipStream, partStream }) => {
      propagateDestroy(err, { dest: [gzipStream, partStream] })
    })
    cb(err)
  }
}

module.exports = PartStreamer
