var benchmarks = require('level-benchmarks')
  , levelup = require('levelup')
  , memdown = require('memdown')
  , multilevel = require('multilevel')

  , remotedown = require('../remotedown')

  , engines = [
        {
            name: 'levelup + memdown'
          , factory: function (name, callback) {
              callback(null, levelup(memdown))
            }
        }
      , {
            name: 'levelup + remotedown + memdown'
          , factory: function (name, callback) {
              var server = remotedown.server(memdown())
                , client = remotedown.client()

              server.pipe(client.createRpcStream()).pipe(server)

              callback(null, levelup(function () { return client }))
            }
        }
      , {
            name: 'multilevel + memdown'
          , factory: function (name, callback) {
              var server = multilevel.server(levelup(memdown))
                , client = multilevel.client()
                , batch = client.batch

              client.batch = function (array, callback) {
                if (arguments.length === 0) {
                  var opts = []
                  return {
                      put: function (key, value) {
                        opts.push({
                            key: key
                          , value: value
                          , type: 'put'
                        })
                      }
                    , write: function (callback) {
                        batch.call(client, opts, callback)
                      }
                  }
                } else {
                  batch.apply(client, arguments)
                }
              }

              server.pipe(client.createRpcStream()).pipe(server)

              callback(null, client)
            }
        }
    ]
  , lengths = [ 10 ]

benchmarks(engines, lengths, { maxTime: 1 }, function (err, result) {
  Object.keys(result).forEach(function (testName) {
    var lengths = result[testName]

    Object.keys(lengths).forEach(function (length) {
      var engines = lengths[length]

      Object.keys(engines).forEach(function (engine) {
        var benchmark = engines[engine]

        console.log(benchmark.target.toString())
      })
      console.log()
    })
  })
})