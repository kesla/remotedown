var Duplex = require('stream').Duplex

  , ClientStream = function (client) {
      if (!(this instanceof ClientStream))
        return new ClientStream(client)

      Duplex.call(this)

      this._client = client
    }

require('inherits')(ClientStream, Duplex)

ClientStream.prototype._write = function (chunk, encoding, callback) {
  this._client._write(chunk, encoding, callback)
}

ClientStream.prototype._read = function () {
  return this._client._read()
}

module.exports = ClientStream