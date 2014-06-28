var remoteDOWN = require('./remotedown')
  , memDOWN = require('memdown')
  , test = require('tape')

test('put', function (t) {
  var serverDb = memDOWN('/does/not/matter')

    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()

  server.pipe(client.createRpcStream()).pipe(server)

  client.put(new Buffer('beep'), new Buffer('boop'), function () {
    serverDb.get(new Buffer('beep'), function (err, value) {
      t.deepEqual(value, new Buffer('boop'))
      t.end()
    })
  })
})

test('del', function (t) {
  var serverDb = memDOWN('/does/not/matter')

    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()

  server.pipe(client.createRpcStream()).pipe(server)

  serverDb.put(new Buffer('beep'), new Buffer('boop'), function () {
    client.del(new Buffer('beep'), function () {
      serverDb.get(new Buffer('beep'), function (err, value) {
        t.deepEqual(err.message, 'NotFound')
        t.equal(value, undefined)
        t.end()
      })
    })
  })
})

test('batch', function (t) {
  var serverDb = memDOWN('/does/not/matter')

    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()

  server.pipe(client.createRpcStream()).pipe(server)

  client.batch(
      [
          { key: new Buffer('beep'), value: new Buffer('boop'), type: 'put' }
        , { key: new Buffer('bing'), value: new Buffer('bong'), type: 'put' }
      ]
    , function () {
        serverDb.get(new Buffer('beep'), function (err, value) {
          t.deepEqual(value, new Buffer('boop'))
          serverDb.get(new Buffer('bing'), function (err, value) {
            client.batch(
                [
                    { key: new Buffer('beep'), type: 'del' }
                  , { key: new Buffer('bing'), type: 'del' }
                ]
              , function () {
                  serverDb.get(new Buffer('beep'), function (err, value) {
                    t.deepEqual(err.message, 'NotFound')
                    serverDb.get(new Buffer('bing'), function (err, value) {
                      t.deepEqual(err.message, 'NotFound')
                      t.end()
                    })
                  })
                }
            )
          })
        })
      }
  )
})
