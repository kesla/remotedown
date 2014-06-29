var levelup = require('levelup')
  , memdown = require('memdown')

  , remotedown = require('../remotedown')
  , length = 1000

  , setup = function (test, callback) {
      var server = remotedown.server(memdown())
        , client = remotedown.client()
        , db = levelup(function () { return client })

      server.pipe(client.createRpcStream()).pipe(server)

      test.setup(db, length, function () {
        callback(null, db)
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
                    '%s ran for %sms %sops/sec (%s runs sampled)'
                  , name, diff, count/(diff/1000), count
                )
              else
                test(db, length, callback)
            }

        test(db, length, callback)

      })
    }

module.exports = run