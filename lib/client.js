var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

  , ClientIterator = require('./client-iterator')
  , ClientStream = require('./client-stream')
  , DataById = require('./data-by-id')
  , encoders = require('./encoders')
  , formatBatchArray = require('./format-batch-array')
  , types = require('./types')

  , Client = function () {
      if (!(this instanceof Client))
        return new Client()

      AbstractLevelDOWN.call(this, '/does/not/matter')

      this._callbacks = new DataById()
      this.stream = null
    }

require('inherits')(Client, AbstractLevelDOWN)

Client.prototype.createRpcStream = function () {
  this.stream = new ClientStream(this)
  return this.stream
}

// called from the client-stream
Client.prototype._parse = function (chunks, callback) {
  var self = this

  chunks.forEach(function (chunk) {
    var id = chunk.readUInt32LE(0)
      , isError = chunk.readUInt8(4) === types.errorResponse
      , data = chunk.slice(5)

    if (isError) {
      self._callbacks.remove(id)(new Error(data.toString()))
    } else {
      if (data.length)
        self._callbacks.remove(id)(null, data)
      else
        self._callbacks.remove(id)()
    }
  })

  callback()
}

Client.prototype._batch = function (array, options, callback) {
  if (array.length === 0)
    return setImmediate(callback)

  array = formatBatchArray(array)

  var id = this._callbacks.add(callback)
    , data = require('json-buffer').stringify(array)


  this.stream.push(encoders.batch(data, id))
}

Client.prototype.iterator = function (options) {
  options = options || {}

  return new ClientIterator(this, this._callbacks, options)
}

Client.prototype._del = function (key, options, callback) {
  this._batch([ { key: key, type: 'del' } ], options, callback)
}

Client.prototype._put = function (key, value, options, callback) {
  this._batch([ { key: key, value: value, type: 'put' } ], options, callback)
}

Client.prototype._get = function (key, options, callback) {
  var id = this._callbacks.add(function (err, value) {
      if (err)
        callback(err)
      else
        callback(
            null
          , options.asBuffer === false ? value.toString() : value
        )
    })

  this.stream.push(encoders.get(key, id))
}

module.exports = Client