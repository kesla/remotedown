var Duplex = require('stream').Duplex

  , bufferedTransform = require('buffered-transform')

  , ClientStream = function (client) {
      if (!(this instanceof ClientStream))
        return new ClientStream(client)

      Duplex.call(this)

      this._write = bufferedTransform(function (chunks, callback) {
        client._parse(chunks, callback)
      })
    }

require('inherits')(ClientStream, Duplex)

// empty cause right now we just push data to stream directly when available
ClientStream.prototype._read = function () {}

module.exports = ClientStream