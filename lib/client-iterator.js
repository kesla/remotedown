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
    , id = this._callbacks.add(function (err, response) {
        if (err) throw err

        self._id = response.readUInt32LE(0)
        if (self._idCallback) {
          self._idCallback(self._id)
          self._idCallback = null
        }
      })

  this._client.stream.push(encoders.newIterator(data, id))
}

ClientIterator.prototype._getId = function (callback) {
  if (this._id !== null)
    callback(this._id)
  else
    this._idCallback = callback
}

ClientIterator.prototype._end = function (callback) {
  var self = this
    , id = this._callbacks.add(callback)

  this._getId(function (iteratorId) {
    self._client.stream.push(encoders.iteratorEnd(iteratorId, id))
  })
}

ClientIterator.prototype._next = function (callback) {
  var self = this
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
    self._client.stream.push(encoders.iteratorNext(iteratorId, id))
  })
}

module.exports = ClientIterator