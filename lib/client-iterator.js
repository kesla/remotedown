var AbstractIterator = require('abstract-leveldown').AbstractIterator

  , encoders = require('./encoders')

  , ClientIterator = function (client, callbacks, options) {
      AbstractIterator.call(this)
      this._client = client
      this._callbacks = client._callbacks
      this._id = null
      this._newIterator(options)
    }

require('inherits')(ClientIterator, AbstractIterator)

ClientIterator.prototype._newIterator = function (options) {
  var data = JSON.stringify(options)
     , buffer = new Buffer(data.length + 9)
     , id = this._callbacks.add(function (err, response) {

      })

  encoders.newIterator(buffer, data, id, 0)
  this._client.stream.push(buffer)
}

module.exports = ClientIterator