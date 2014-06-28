var remoteDOWN = require('./remotedown')
  , serverDb = require('memdown')('/does/not/matter')

  , server = remoteDOWN.server(serverDb)
  , client = remoteDOWN.client()

server.pipe(client.createRpcStream()).pipe(server)

client.batch(
    [{ key: new Buffer('beep'), value: new Buffer('boop'), type: 'put' }]
  , function () {
      serverDb.get(new Buffer('beep'), function (err, value) {
        console.log('value saved in server:', value.toString())
      })
    }
)