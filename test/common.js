var through2 = require('through2')

  , remoteDOWN = require('../remotedown')
  , leveldown = require(process.env.LEVELDOWN || 'leveldown')

  , createBufferingStream = function () {
      var array = []
        , stream = through2(function (chunk, enc, callback) {
            array.push(chunk)
            callback()
          })

      stream.flush = function () {
        var buffer = Buffer.concat(array)
        stream.push(Buffer.concat(array))
        array.length = 0
      }
      return stream
    }

  , createSplittingStream = function () {
      return through2(function (buffer, enc, callback) {
        for(var i = 0; i < buffer.length; ++i) {
          this.push(buffer.slice(i, i + 1))
        }

        callback()
      })
    }
  , factory = function (name) {

      var server = remoteDOWN.server(leveldown(name))
        , client = remoteDOWN.client()

      server
        .pipe(createSplittingStream())
        .pipe(client.createRpcStream())
        .pipe(createSplittingStream())
        .pipe(server)

      client.open = server.open.bind(server)

      return client
    }

module.exports.factory = factory
module.exports.leveldown = leveldown
module.exports.createSplittingStream = createSplittingStream
module.exports.createBufferingStream = createBufferingStream