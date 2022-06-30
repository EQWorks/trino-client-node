const { Duplex } = require('stream')

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
    
    this.parser = JSONParser({ packKeys: true, jsonStreaming: true })
    this.parser.on('data', (chunk) => {
      if (!this._handleToken(chunk)) {
        this.parser.pause()
        this.streamer.on('drain', () => this.parser.resume())
      }
    })
    this.parser.once('end', () => this.streamer.end())
    this.parser.once('error', (err) =>
      propagateDestroy(err, this.parser, [this, this.streamer]))
    
    this.streamer = streamArray()
    this.streamer.on('data', ({ value }) => {
      console.log('reading from streamer')
      let val = value
      if (!rowMode) {
        if (!this.columns) {
          return this.destroy(new Error('columns missing'))
        }
        val = value.reduce((acc, v, i) => {
          acc[this.columns[i].name] = v
          return acc
        }, {})
      }
      // handle back pressure
      if (!this.push(val)) {
        console.log('pausing body streamer')
        this.streamer.pause()
      }
    })
    this.streamer.once('error', (err) =>
      propagateDestroy(err, this.streamer, [this, this.parser]))
    this.streamer.once('end', () => this.push(null))
  }

  _write(chunk, _, cb) {
    // handle backpressure
    if (this.parser.write(chunk)) {
      return cb(null)
    }
    this.parser.once('drain', () => cb(null))
  }

  _read() {
    // push new data
    if (this.streamer.isPaused()) {
      console.log('resuming body stream')
      this.streamer.resume()
    }
  }

  // return false if write buffer is full
  _handleToken(token) {
    let readableHasCapacity = true
    try {
      if (token.name === 'startObject') {
        this.depth += 1
        // object root, no further processing
        if (this.depth === 1) {
          return readableHasCapacity
        }
      }
      if (this.depth === 0) {
        return readableHasCapacity
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
        return readableHasCapacity
      }

      if (!this.current) {
        return readableHasCapacity
      }

      // push data to assembler or to read buffer
      if (this.current === 'data') {
        readableHasCapacity = this.streamer.write(token)
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
        }
      }
      if (!readableHasCapacity) {
        console.log('readable buffer is full')
      }
      return readableHasCapacity
    } catch (err) {
      this.destroy(err)
    }
  }

  _final(cb) {
    this.parser.end()
    cb(null)
  }

  _destroy(err, cb) {
    // TODO: put any cleanup here
    console.log('destroying body stream', err)
    cb(err)
  }
}

module.exports = TrinoBodyStreamer
