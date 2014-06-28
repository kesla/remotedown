var remoteDOWN = require('./remotedown')
  , serverDb = require('memdown')('/does/not/matter')
  , test = require('tape')

  , server = remoteDOWN.server(serverDb)
  , client = remoteDOWN.client()

client.pipe(server).pipe(client)

test('batch', function (t) {
  client.batch(
      [{ key: new Buffer('beep'), value: new Buffer('boop'), type: 'put' }]
    , function () {
        serverDb.get(new Buffer('beep'), function (err, value) {
          t.deepEqual(value, new Buffer('boop'))
          t.end()
        })
      }
  )
})

test('put', function (t) {
  client.put(new Buffer('hello'), new Buffer('world'), function () {
    serverDb.get(new Buffer('hello'), function (err, value) {
      t.deepEqual(value, new Buffer('world'))
      t.end()
    })
  })
})
