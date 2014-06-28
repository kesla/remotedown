var Transform = require('stream').Transform

  , numbersToBuffer = require('numbers-to-buffer')

  , Server = function (db) {
      if (!(this instanceof Server))
        return new Server(db)

      Transform.call(this)
      this._transformBuffer = null
      this._db = db
    }

require('inherits')(Server, Transform)

Server.prototype._parse = function (batch) {
  var self = this
    , ptr = 0
    , ids = []
    , dataLength
    , decodePut = function () {
        var keyLength = self._transformBuffer.readUInt32LE(ptr)
          , key = self._transformBuffer.slice(ptr + 4, ptr + keyLength + 4)
          , valueLength = self._transformBuffer.readUInt32LE(ptr + keyLength + 4)
          , value = self._transformBuffer.slice(
                ptr + keyLength + 8
              , ptr + keyLength + valueLength + 8
            )

        ptr += keyLength + valueLength + 8
        batch.put(key, value)
        dataLength -= (keyLength + valueLength + 8)
      }
    , decodeDel = function () {
        var keyLength = self._transformBuffer.readUInt32LE(ptr)
          , key = self._transformBuffer.slice(ptr + 4, ptr + keyLength + 4)

        ptr += keyLength + 4
        batch.del(key)
        dataLength -= (keyLength + 4)
      }

  while(ptr < this._transformBuffer.length - 8) {
    dataLength = this._transformBuffer.readUInt32LE(ptr)
    if (ptr + dataLength > this._transformBuffer.length) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return ids
    }

    ptr += 4
    ids.push(this._transformBuffer.readUInt32LE(ptr))
    ptr += 4

    while(dataLength > 0) {
      if (this._transformBuffer[ptr] === 1) {
        ptr++
        dataLength--
        decodePut()
      }
      if (this._transformBuffer[ptr] === 0) {
        ptr++
        dataLength--
        decodeDel()
      }
    }
  }
  this._transformBuffer = this._transformBuffer.slice(ptr)
  return ids
}

Server.prototype._parseAndWrite = function (callback) {
  var batch = this._db.batch()
    , ids = this._parse(batch)
    , self = this

  batch.write(function () {
    self.push(numbersToBuffer.UInt32LE(ids))
    callback()
  })
}

Server.prototype._transform = function (chunk, encoding, callback) {
  this._transformBuffer = this._transformBuffer ?
    Buffer.concat([ this._transformBuffer, chunk ]) : chunk

  if (this._transformBuffer.length >= 8)
    this._parseAndWrite(callback)
  else
    callback()
}

module.exports = Server