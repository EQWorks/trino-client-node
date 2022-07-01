const { Duplex, pipeline } = require('stream')

const Assembler = require('stream-json/Assembler')
const { parser: JSONParser } = require('stream-json/Parser')
const { streamArray } = require('stream-json/streamers/StreamArray')

const { propagateDestroy } = require('./utils')


class TrinoBodyStreamer extends Duplex {
  constructor({ rowMode = false } = {}) {
    super({ readableObjectMode: true, allowHalfOpen: false })
    this.rowMode = rowMode
    this.depth = 0
    this.current = undefined
    this.columns = undefined
    this.assembler = new Assembler()

    this._inStream = JSONParser({ packKeys: true, jsonStreaming: true })
    const _this = this
    const transforms = [
      this._inStream,
      async function* (src) {
        for await (const token of src) {
          const data = _this._processToken(token)
          if (data) {
            yield data
          }
        }
      },
      streamArray(),
      async function* (src) {
        for await (const { value } of src) {
          if (_this.rowMode) {
            yield value
            continue
          }
          if (!_this.columns) {
            throw new Error('columns missing')
          }
          // transform into object { columnName: value }
          yield value.reduce((acc, v, i) => {
            acc[_this.columns[i].name] = v
            return acc
          }, {})
        }
      },
    ]
    this._outStream = pipeline(
      transforms,
      (err) => {
        if (err) {
          return propagateDestroy(err, { dest: [this] })
        }
        this.push(null)
      },
    )
    this._outStream.on('data', (data) => {
      // handle back pressure
      if (!this.push(data)) {
        this._outStream.pause()
      }
    })
  }

  _write(chunk, _, cb) {
    // handle backpressure
    if (this._inStream.write(chunk)) {
      return cb(null)
    }
    this._inStream.once('drain', () => cb(null))
  }

  _read() {
    // ask for new data
    if (this._outStream.isPaused()) {
      this._outStream.resume()
    }
  }

  // return data chunk
  _processToken(token) {
    let data
    if (token.name === 'startObject') {
      this.depth += 1
      // object root, no further processing
      if (this.depth === 1) {
        return data
      }
    }
    if (this.depth === 0) {
      return data
    }

    // next top-level key
    if (this.depth === 1 && token.name === 'keyValue') {
      // push previous top-level key
      if (this.current && this.current !== 'data') {
        if (!this.rowMode && this.current === 'columns') {
          this.columns = this.assembler.current
        }
        this.emit('meta', this.current, this.assembler.current)
      }
      this.current = token.value
      return data
    }

    if (!this.current) {
      return data
    }

    // push data to assembler or to read buffer
    if (this.current === 'data') {
      data = token
    } else {
      this.assembler.consume(token)
    }

    if (token.name === 'endObject') {
      this.depth -= 1
      // we're done - push last key
      if (this.depth === 0 && this.current !== 'data') {
        if (!this.rowMode && this.current === 'columns') {
          this.columns = this.assembler.current
        }
        this.emit('meta', this.current, this.assembler.current)
        this.current = undefined
        this.emit('done')
      }
    }
    return data
  }

  _final(cb) {
    this._outStream.end()
    cb(null)
  }

  _destroy(err, cb) {
    if (!err) {
      return cb(null)
    }
    propagateDestroy(err, { dest: [this._outStream] })
    cb(err)
  }
}

module.exports = TrinoBodyStreamer
