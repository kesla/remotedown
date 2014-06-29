var Transform = require('stream').Transform

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

Server.prototype._parseBatch = function (batch) {
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

  while(ptr < this._transformBuffer.length - 9) {
    // only merge together if it is multiple batches after each other
    if (this._transformBuffer[ptr] !== 100) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return ids
    }

    dataLength = this._transformBuffer.readUInt32LE(ptr + 1)
    // must have space for ptr + data + header
    if (ptr + dataLength + 9 > this._transformBuffer.length) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return ids
    }

    // dataLength + type
    ptr += 5
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
        response.push(numbersToBuffer.UInt32LE([
          message.length, id
        ]))
        response.push(IS_ERROR_ID)
        response.push(message)

        return response
      }, [])

  this.push(Buffer.concat(response))
}

Server.prototype._handleOk = function (ids, data) {
  var dataLength = data ? data.length : 0
    , response = ids.reduce(function (response, id) {
        response
          .push(
            numbersToBuffer.UInt32LE([
              dataLength, id
            ])
          )

        response
          .push(IS_OK_ID)

        if (dataLength)
          response.push(data)

        return response
      }, [])

  this.push(Buffer.concat(response))
}

Server.prototype._batch = function (callback) {
  var self = this
    , batch = this._db.batch()
    , ids = this._parseBatch(batch)
    , self = this

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
  // TODO: parse/stringify this in another way?
  options = JSON.parse(this._transformBuffer.slice(ptr, ptr + dataSize))

  Object.keys(options).forEach(function (key) {
    if (isObj(options[key]) && options[key].type === 'Buffer')
      options[key] = new Buffer(options[key].data)
  })
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

module.exports = Server