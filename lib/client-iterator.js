var AbstractIterator = require('abstract-leveldown').AbstractIterator
  , JSONB = require('json-buffer')

  , encoders = require('./encoders')

  , ClientIterator = function (client, callbacks, options) {
      AbstractIterator.call(this)
      this._client = client
      this._callbacks = client._callbacks
      this._id = null
      this._newIterator(options)
      this._idCallback = null
      this._keyAsBuffer = !(options.keyAsBuffer === false)
      this._valueAsBuffer = !(options.valueAsBuffer === false)
    }

require('inherits')(ClientIterator, AbstractIterator)

ClientIterator.prototype._newIterator = function (options) {
  var self = this
    , data = JSONB.stringify(options)
    , buffer = new Buffer(data.length + 9)
    , id = this._callbacks.add(function (err, response) {
        if (err) throw err

        self._id = response.readUInt32LE(0)
        if (self._idCallback) {
          self._idCallback(self._id)
          self._idCallback = null
        }
      })

  encoders.newIterator(buffer, data, id, 0)
  this._client.stream.push(buffer)
}

ClientIterator.prototype._getId = function (callback) {
  if (this._id !== null)
    callback(this._id)
  else
    this._idCallback = callback
}

ClientIterator.prototype._end = function (callback) {
  var self = this
    , buffer = new Buffer(13)
    , id = this._callbacks.add(callback)

  this._getId(function (iteratorId) {
    encoders.iteratorEnd(buffer, iteratorId, id, 0)
    self._client.stream.push(buffer)
  })
}

ClientIterator.prototype._next = function (callback) {
  var self = this
    , buffer = new Buffer(13)
    , id = this._callbacks.add(function (err, response) {
        if (err)
          callback(err)

        else if (!response)
          callback()
        else {
          var keyLength = response.readUInt32LE(0)
            , key = response.slice(4, keyLength + 4)
            , valueLength = response.readUInt32LE(keyLength + 4)
            , value = response.slice(keyLength + 8, keyLength + valueLength + 8)

          if (!self._keyAsBuffer)
            key = key.toString()

          if (!self._valueAsBuffer)
            value = value.toString()

          callback(null, key, value)
        }
      })

  this._getId(function (iteratorId) {
    encoders.iteratorNext(buffer, iteratorId, id, 0)
    self._client.stream.push(buffer)
  })
}

module.exports = ClientIterator