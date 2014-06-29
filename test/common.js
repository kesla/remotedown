var through2 = require('through2')

  , remoteDOWN = require('../remotedown')
  , leveldown = require(process.env.LEVELDOWN || 'leveldown')

  , createSplittingStream = function () {
      return through2(function (buffer, enc, callback) {
        for(var i = 0; i < buffer.length; ++i) {
          this.push(buffer.slice(i, i + 1))
        }

        callback()
      })
    }
  , common = {
        factory: function (name) {

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
    }

module.exports = common
module.exports.leveldown = leveldown