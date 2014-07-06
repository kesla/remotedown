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
      var keys = !(options.keys === false)
        , values = !(options.values === false)
      this._callNextCallback =
        keys && values ?
          function (data, callback) {
            callback(null, data[0], data[1])
          }
          :
        keys ?
          function (data, callback) {
            callback(null, data, null)
          }
          :
        values ?
          function (data, callback) {
            callback(null, null, data)
          }
          :
          function (data, callback) {
            callback(null, null, null)
          }

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

          self._callNextCallback(JSONB.parse(response), callback)
        }
      })

  this._getId(function (iteratorId) {
    encoders.iteratorNext(buffer, iteratorId, id, 0)
    self._client.stream.push(buffer)
  })
}

module.exports = ClientIterator