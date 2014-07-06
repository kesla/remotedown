var Transform = require('stream').Transform

  , bufferedTransform = require('buffered-transform')
  , JSONB = require('json-buffer')
  , numbersToBuffer = require('numbers-to-buffer')

  , types = require('./types')

  , Server = function (db) {
      var self = this

      if (!(this instanceof Server))
        return new Server(db)

      Transform.call(this)
      this._db = db
      this._iterators = require('./data-by-id')()

      this._transform = bufferedTransform(function (chunks, callback) {
        self._parse(chunks, callback)
      })
    }

  , encoders = require('./encoders')

  , IS_ERROR_ID = new Buffer([ 1 ])
  , IS_OK_ID = new Buffer([ 0 ])

  , isObj = function (obj) {
      return typeof(obj) === 'object' && obj !== null
    }

require('inherits')(Server, Transform)

Server.prototype._batch = function (chunks, callback) {
  var self = this
    , ids = []
    , batch = this._db.batch()
    , array
    , endPtr
    , chunk

  while(chunks.length > 0) {
    // only merge together if it is multiple batches after each other
    if (chunks[0][0] !== 100) {
      return this._finishBatch(chunks ,ids, batch, callback)
    }

    var chunk = chunks.shift()

    // after id-byte
    ids.push(chunk.readUInt32LE(1))

    array = JSONB.parse(chunk.slice(5))

    array.forEach(function (row) {
      if (row.length === 2)
        batch.put(row[0], row[1])
      if (row.length === 1)
        batch.del(row[0])
    })
  }

  this._finishBatch(chunks, ids, batch, callback)
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

Server.prototype._finishBatch = function (chunks, ids, batch, callback) {
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
      self._parse(chunks, callback)
    })
  }
}

Server.prototype._newIterator = function (chunks, callback) {
  var chunk = chunks.shift()
    , id = chunk.readUInt32LE(1)
    , options = JSONB.parse(chunk.slice(5).toString())
    , iteratorId

  iteratorId = this._iterators.add(this._db.iterator(options))
  this._handleOk( [ id ], numbersToBuffer.UInt32LE([iteratorId]))
  this._parse(chunks, callback)
}

Server.prototype._iteratorNext = function (chunks, callback) {
  var self = this
    , chunk = chunks.shift()
    , id = chunk.readUInt32LE(1)
    , iteratorId = chunk.readUInt32LE(5)
    , iterator = this._iterators.get(iteratorId)

  iterator.next(function (err, key, value) {

    if (err)
      self._handleError([ id ], err)
    else if (arguments.length === 0) {

      self._handleOk([ id ])
      self._parse(chunks, callback)

    } else {
      var dataLength = (key? key.length : 0) + (value ? value.length : 0) + 8
        , buffer = new Buffer(dataLength)

      encoders.keyValue(buffer, key, value, 0)

      self._handleOk([ id ], buffer)
      self._parse(chunks, callback)
    }
  })
}

Server.prototype._iteratorEnd = function (chunks, callback) {
  var self = this
    , chunk = chunks.shift()
    , id = chunk.readUInt32LE(1)
    , iteratorId = chunk.readUInt32LE(5)
    , iterator = this._iterators.remove(iteratorId)

  iterator.end(function (err) {
    if (err)
      self._handleError([ id ], err)
    else
      self._handleOk([ id ])

    self._parse(chunks, callback)
  })
}

Server.prototype._get = function (chunks, callback) {
  var self = this
    , chunk = chunks.shift()
    , id = chunk.readUInt32LE(1)
    , key

  key = chunk.slice(5)

  this._db.get(key, function (err, value) {
    if (err)
      self._handleError([ id ], err)
    else
      self._handleOk([ id ], value)

    self._parse(chunks, callback)
  })
}

Server.prototype._parse = function (chunks, callback) {
  var type

  if (chunks.length > 0) {
    type = chunks[0][0]

    if (type === types.batch) {
      this._batch(chunks, callback)
    } else if (type === types.newIterator) {
      this._newIterator(chunks, callback)
    } else if (type === types.iteratorNext) {
      this._iteratorNext(chunks, callback)
    } else if (type === types.iteratorEnd) {
      this._iteratorEnd(chunks, callback)
    } else if (type === types.get) {
      this._get(chunks, callback)
    }
  } else {
    callback()
  }
}

Server.prototype.open = function (options, callback) {
  this._db.open(options, callback)
}

Server.prototype.close = function (callback) {
  this._db.close(callback)
}

module.exports = Server