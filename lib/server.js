var Transform = require('stream').Transform

  , JSONB = require('json-buffer')
  , numbersToBuffer = require('numbers-to-buffer')

  , Server = function (db) {
      if (!(this instanceof Server))
        return new Server(db)

      Transform.call(this)
      this._transformBuffer = null
      this._db = db
      this._iterators = require('./data-by-id')()
    }

  , encoders = require('./encoders')

  , IS_ERROR_ID = new Buffer([ 1 ])
  , IS_OK_ID = new Buffer([ 0 ])

  , isObj = function (obj) {
      return typeof(obj) === 'object' && obj !== null
    }

require('inherits')(Server, Transform)

Server.prototype._batch = function (callback) {
  var self = this
    , ptr = 0
    , ids = []
    , batch = this._db.batch()
    , endPtr
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
      }
    , decodeDel = function () {
        var keyLength = self._transformBuffer.readUInt32LE(ptr)
          , key = self._transformBuffer.slice(ptr + 4, ptr + keyLength + 4)

        ptr += keyLength + 4
        batch.del(key)
      }

  while(ptr < this._transformBuffer.length - 9) {
    // only merge together if it is multiple batches after each other
    if (this._transformBuffer[ptr] !== 100) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return this._finishBatch(ids, batch, callback)
    }

    endPtr = ptr + this._transformBuffer.readUInt32LE(ptr + 1) + 9
    // must have space for ptr + data + header
    if (endPtr > this._transformBuffer.length) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return this._finishBatch(ids, batch, callback)
    }

    // dataLength + type
    ptr += 5
    ids.push(this._transformBuffer.readUInt32LE(ptr))
    ptr += 4

    while(endPtr - ptr > 0) {
      if (this._transformBuffer[ptr] === 1) {
        ptr++
        decodePut()
      } else if (this._transformBuffer[ptr] === 0) {
        ptr++
        decodeDel()
      }
    }
  }
  this._transformBuffer = this._transformBuffer.slice(ptr)
  return this._finishBatch(ids, batch, callback)
}

Server.prototype._handleError = function (ids, err) {
  var dataLength = err.message.length
    , buffer = new Buffer(ids.length * (9 + dataLength))

  encoders.errorResponse(buffer, ids, err.message, 0)

  this.push(buffer)
}

Server.prototype._handleOk = function (ids, data) {
  var dataLength = data ? data.length : 0
    , buffer = new Buffer(ids.length * (9 + dataLength))

  encoders.okResponse(buffer, ids, data, 0)

  this.push(buffer)
}

Server.prototype._finishBatch = function (ids, batch, callback) {
  var self = this

  if (ids.length === 0) {
    batch.clear()
    callback()
  } else {
    batch.write(function (err) {
      if (err) {
        self._handleError(ids, err)
      } else {
        self._handleOk(ids)
      }
      self._parse(callback)
    })
  }
}

Server.prototype._newIterator = function (callback) {
  var ptr = 1
    , dataSize = this._transformBuffer.readUInt32LE(ptr)
    , id = this._transformBuffer.readUInt32LE(ptr + 4)
    , options
    , iteratorId

  if (ptr + dataSize + 8 > this._transformBuffer.length) {
    return callback()
  }

  ptr += 8
  options = JSONB.parse(this._transformBuffer.slice(ptr, ptr + dataSize))

  ptr += dataSize

  iteratorId = this._iterators.add(this._db.iterator(options))
  this._handleOk( [ id ], numbersToBuffer.UInt32LE([iteratorId]))
  this._transformBuffer = this._transformBuffer.slice(ptr)
  this._parse(callback)
}

Server.prototype._iteratorNext = function (callback) {
  var self = this
    , ptr = 1
    , id = this._transformBuffer.readUInt32LE(ptr)
    , iteratorId = this._transformBuffer.readUInt32LE(ptr + 4)
    , iterator = this._iterators.get(iteratorId)

  self._transformBuffer = self._transformBuffer.slice(ptr + 8)

  iterator.next(function (err, key, value) {

    if (err)
      self._handleError([ id ], err)
    else if (arguments.length === 0) {

      self._handleOk([ id ])
      self._parse(callback)

    } else {
      var dataLength = (key? key.length : 0) + (value ? value.length : 0) + 9
        , buffer = new Buffer(dataLength)

      buffer[0] = 1
      encoders.keyValue(buffer, key, value, 1)

      self._handleOk([ id ], buffer)
      self._parse(callback)
    }
  })
}

Server.prototype._iteratorEnd = function (callback) {
  var self = this
    , ptr = 1
    , id = this._transformBuffer.readUInt32LE(ptr)
    , iteratorId = this._transformBuffer.readUInt32LE(ptr + 4)
    , iterator = this._iterators.remove(iteratorId)

  self._transformBuffer = self._transformBuffer.slice(ptr + 8)

  iterator.end(function (err) {
    if (err)
      self._handleError([ id ], err)
    else
      self._handleOk([ id ])

    self._parse(callback)
  })
}

Server.prototype._parse = function (callback) {
  var self = this

  if (this._transformBuffer.length >= 9) {
    if (this._transformBuffer[0] === 100) {
      this._batch(callback)
    } else if (this._transformBuffer[0] === 101) {
      this._newIterator(callback)
    } else if (this._transformBuffer[0] === 102) {
      this._iteratorNext(callback)
    } else if (this._transformBuffer[0] === 103) {
      this._iteratorEnd(callback)
    }
  } else {
    callback()
  }
}

Server.prototype._transform = function (chunk, encoding, callback) {
  this._transformBuffer = this._transformBuffer ?
    Buffer.concat([ this._transformBuffer, chunk ]) : chunk

  this._parse(callback)
}

Server.prototype.open = function (options, callback) {
  this._db.open(options, callback)
}

module.exports = Server