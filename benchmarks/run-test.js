var levelup = require('levelup')
  , leveldown = require('leveldown')

  , remotedown = require('../remotedown')
  , length = 1000

  , dir = __dirname + '/test-db'

  , setup = function (test, callback) {
      leveldown.destroy(dir, function () {
        var server = remotedown.server(leveldown(dir))
          , client = remotedown.client()
          , db = levelup(function () { return client })

        server.pipe(client.createRpcStream()).pipe(server)

        server.open(function () {
          test.setup(db, length, function () {
            callback(null, db)
          })
        })
      })
    }
  , run = function (name) {
      var test = require('level-benchmarks/tests/' + name)

      setup(test, function (err, db) {
        var start = Date.now()
          , count = 0
          , callback = function () {
              count += 1

              var diff = Date.now() - start

              if (diff > 3000)
                console.log(
                    '%s ran for %sms %s opts/sec (%s runs sampled)'
                  , name, diff, Math.round(count/(diff/1000)), count
                )
              else
                test(db, length, callback)
            }

        test(db, length, callback)

      })
    }

module.exports = run