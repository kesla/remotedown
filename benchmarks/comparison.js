var benchmarks = require('level-benchmarks')
  , levelup = require('levelup')
  , leveldown = require('leveldown')
  , multilevel = require('multilevel')

  , remotedown = require('../remotedown')
  , dir = __dirname + '/test-db'
  , db = leveldown(dir)

  , setupLeveldown = function (callback) {
      db.close(function () {
        leveldown.destroy(dir, function () {
          callback(null, function () { return db })
        })
      })
    }

  , engines = [
        {
            name: 'levelup + leveldown'
          , factory: function (name, callback) {
              setupLeveldown(function (err, dbFactory) {
                callback(null, levelup(dbFactory))
              })
            }
        }
      , {
            name: 'levelup + remotedown + leveldown'
          , factory: function (name, callback) {
              setupLeveldown(function (err, dbFactory) {
                var server = remotedown.server(dbFactory())
                  , client = remotedown.client()

                client.open = server.open.bind(server)

                server.pipe(client.createRpcStream()).pipe(server)

                callback(null, levelup(function () { return client }))
              })
            }
        }
      , {
            name: 'multilevel + leveldown'
          , factory: function (name, callback) {
              setupLeveldown(function (err, dbFactory) {
                var server = multilevel.server(levelup(dbFactory))
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
              })
            }
        }
    ]
  , lengths = [ 10, 100, 1000 ]

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