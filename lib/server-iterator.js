var JSONB = require('json-buffer')
  , ServerIterator = function (iterator, options) {
      this._iterator = iterator
      this._stringify =
        !(options.keys === false) && !(options.values === false) ?
          function (key, value) {
            return JSONB.stringify([ key, value ])
          }
        :
        !(options.keys === false) ?
          function (key, value) {
            return JSONB.stringify(key)
          }
        :
        !(options.values === false) ?
          function (key, value) {
            return JSONB.stringify(value)
          }
        :
          function () {
            return "null"
          }
    }

ServerIterator.prototype.next = function (callback) {
  var self = this

  this._iterator.next(function (err, key, value) {
    if (err) {
      callback(err)

    } else if (arguments.length === 0) {
      callback()

    } else {
      callback(null, self._stringify(key, value))
    }
  })
}

ServerIterator.prototype.end = function (callback) {
  this._iterator.end(callback)
}

module.exports = ServerIterator