var bytes = require('bytes')
  , levelup = require('levelup')
  , leveldown = require('leveldown')

  , remotedown = require('../remotedown')

  , LENGTH = process.env.LENGTH || 1000
  , DIR = __dirname + '/test-db'

  , round = function (num) {
      return Math.round(num * 100) / 100;
    }

  , setupLeveldown = function (test, callback) {
      leveldown.destroy(DIR, function () {
        var server = remotedown.server(leveldown(DIR))
          , db = remotedown.client()
          , clientStream = db.createRpcStream()
          , streamCount = {
                client: 0
              , server: 0
            }

        db.close = server.close.bind(server)

        server.pipe(clientStream).pipe(server)

        server.on('data', function (chunk) { streamCount.server += chunk.length })
        clientStream.on('data', function (chunk) { streamCount.client += chunk.length })

        server.open(function () {
          test.setup(db, LENGTH, function () {
            callback(null, db, streamCount)
          })
        })
      })
    }
  , setupLevelup = function (test, callback) {
      setupLeveldown(test, function (err, client, streamCount) {
        if (err) return callback(err)

        callback(null, levelup(function () { return client }), streamCount)
      })
    }
  , runTest = function (test, name, db, streamCount, callback) {
      var start = Date.now()
        , count = 0
        , done = function () {
            count += 1

            var timeDiff = Date.now() - start
              , serverBytes
              , clientBytes

            if (timeDiff > 3000) {
              console.log(
                  '%s ran for %sms %s opts/sec (%s runs sampled)'
                , name, timeDiff, round(count / (timeDiff / 1000)), count
              )
              serverBytes = Math.round(streamCount.server / count)
              clientBytes = Math.round(streamCount.client / count)

              console.log(
                  'server -> client %s (%s bytes)\nclient -> server %s (%s bytes)'
                , bytes(serverBytes), serverBytes
                , bytes(clientBytes), clientBytes
              )
              if (callback) callback()
            } else {
              test(db, LENGTH, done)
            }
          }

      test(db, LENGTH, done)
    }
  , run = function (name) {
      var test = require('level-benchmarks/tests/' + name)

      console.log('levelup')
      setupLevelup(test, function (err, db, streamCount) {
        runTest(test, name, db, streamCount, function () {
          if (name !== 'read-stream') {
            db.close(function () {
              console.log()
              console.log('leveldown')
              setupLeveldown(test, function (err, db, streamCount) {
                runTest(test, name, db, streamCount)
              })
            })
          }
        })
      })
    }

module.exports = run