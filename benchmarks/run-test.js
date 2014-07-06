var levelup = require('levelup')
  , leveldown = require('leveldown')

  , remotedown = require('../remotedown')
  , length = 1000

  , dir = __dirname + '/test-db'

  , round = function (num) {
      return Math.round(num * 100) / 100;
    }

  , setup = function (test, callback) {
      leveldown.destroy(dir, function () {
        var server = remotedown.server(leveldown(dir))
          , client = remotedown.client()
          , clientStream = client.createRpcStream()
          , db = levelup(function () { return client })
          , streamCount = {
                client: 0
              , server: 0
            }

        server.pipe(clientStream).pipe(server)

        server.on('data', function (chunk) { streamCount.server += chunk.length })
        clientStream.on('data', function (chunk) { streamCount.client += chunk.length })

        server.open(function () {
          test.setup(db, length, function () {
            callback(null, db, streamCount)
          })
        })
      })
    }
  , run = function (name) {
      var test = require('level-benchmarks/tests/' + name)

      setup(test, function (err, db, streamCount) {
        var start = Date.now()
          , count = 0
          , callback = function () {
              count += 1

              var timeDiff = Date.now() - start

              if (timeDiff > 3000) {
                console.log(
                    '%s ran for %sms %s opts/sec (%s runs sampled)'
                  , name, timeDiff, round(count/(timeDiff/1000)), count
                )
                console.log(
                    'traffic %s bytes from server %s bytes from client'
                  , Math.round(streamCount.server / count)
                  , Math.round(streamCount.client / count)
                )
              } else {
                test(db, length, callback)
              }
            }

        test(db, length, callback)

      })
    }

module.exports = run