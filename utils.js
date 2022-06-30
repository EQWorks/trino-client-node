const { Readable, Writable } = require('stream')


const propagateDestroy = (err, src, destinations = []) => {
  [src, ...destinations].forEach((stream) => {
    if (
      !stream.destroyed
      && (
        stream instanceof Readable && !stream.readableEnded && !stream.closed
        || stream instanceof Writable && !stream.writableFinished && !stream.closed
      )
    ) {
      stream.destroy(stream === src ? null : err)
    }
  })
}

module.exports = { propagateDestroy }
