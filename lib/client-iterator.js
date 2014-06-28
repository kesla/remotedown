var AbstractIterator = require('abstract-leveldown').AbstractIterator

  , encoders = require('./encoders')

  , ClientIterator = function (client, callbacks, options) {
      AbstractIterator.call(this)
      this._client = client
      this._callbacks = client._callbacks
      this._id = null
      this._newIterator(options)
      this._idCallback = null
    }

require('inherits')(ClientIterator, AbstractIterator)

ClientIterator.prototype._newIterator = function (options) {
  var self = this
    , data = JSON.stringify(options)
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
  if (typeof(this._id) !== 'undefined')
    callback(this._id)
  else
    this._idCallback = callback
}

ClientIterator.prototype._end = function (callback) {
  var self = this
    , buffer = new Buffer(9)
    , id = this._callbacks.add(callback)

  this._getId(function (iteratorId) {
    encoders.iteratorEnd(buffer, iteratorId, id, 0)
    self._client.stream.push(buffer)
  })
}

ClientIterator.prototype._next = function (callback) {
  var self = this
    , buffer = new Buffer(9)
    , id = this._callbacks.add(function (err, response) {
        if (err)
          callback(err)

        else if (!response)
          callback()
        else {
          var keyLength = response.readUInt32LE(1)
            , key = response.slice(5, keyLength + 5)
            , valueLength = response.readUInt32LE(keyLength + 5)
            , value = response.slice(keyLength + 9, keyLength + valueLength + 9)

            callback(null, key, value)
        }
      })

  this._getId(function (iteratorId) {
    encoders.iteratorNext(buffer, iteratorId, id, 0)
    self._client.stream.push(buffer)
  })
}

module.exports = ClientIterator