var Transform = require('stream').Transform

  , numbersToBuffer = require('numbers-to-buffer')

  , Server = function (db) {
      if (!(this instanceof Server))
        return new Server(db)

      Transform.call(this)
      this._transformBuffer = null
      this._db = db
    }

  , IS_ERROR_ID = new Buffer([ 1 ])
  , IS_OK_ID = new Buffer([ 0 ])

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
    // must have space for ptr + data + header
    if (ptr + dataLength + 8 > this._transformBuffer.length) {
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
      } else if (this._transformBuffer[ptr] === 0) {
        ptr++
        dataLength--
        decodeDel()
      }
    }
  }
  this._transformBuffer = this._transformBuffer.slice(ptr)
  return ids
}

Server.prototype._handleError = function (ids, err) {
  var message = new Buffer(err.message)
    , response = ids.reduce(function (response, id) {
        response
          .push(
            numbersToBuffer.UInt32LE([
              message.length, id
            ])
          )

        response
          .push(IS_ERROR_ID)

        response
          .push(
            message
          )

        return response
      }, [])

  this.push(Buffer.concat(response))
}

Server.prototype._handleOk = function (ids) {
  var response = ids.reduce(function (response, id) {
        response
          .push(
            numbersToBuffer.UInt32LE([
              0, id
            ])
          )

        response
          .push(IS_OK_ID)

        return response
      }, [])

  this.push(Buffer.concat(response))
}

Server.prototype._parseAndWrite = function (callback) {
  var batch = this._db.batch()
    , ids = this._parse(batch)
    , self = this

  batch.write(function (err) {
    if (err) {
      self._handleError(ids, err)
    } else {
      self._handleOk(ids)
    }
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