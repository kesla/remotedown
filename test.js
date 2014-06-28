var remoteDOWN = require('./remotedown')
  , memDOWN = require('memdown')
  , test = require('tape')
  , through2 = require('through2')

  , createBufferingStream = function () {
      var array = []
        , stream = through2(function (chunk, enc, callback) {
            array.push(chunk)
            callback()
          })

      stream.flush = function () {
        var buffer = Buffer.concat(array)
        stream.push(Buffer.concat(array))
        array.length = 0
      }
      return stream
    }
  , createSplittingStream = function () {
      return through2(function (buffer, enc, callback) {
        for(var i = 0; i < buffer.length; ++i) {
          this.push(buffer.slice(i, i + 1))
        }

        callback()
      })
    }
  , setup = function (callback) {
      var serverDb = memDOWN('/does/not/matter')

        , server = remoteDOWN.server(serverDb)
        , client = remoteDOWN.client()

      server
        .pipe(createSplittingStream())
        .pipe(client.createRpcStream())
        .pipe(createSplittingStream())
        .pipe(server)

      callback(client, server, serverDb)
    }

test('put', function (t) {
  setup(function (client, server, serverDb) {
    client.put(new Buffer('beep'), new Buffer('boop'), function () {
      serverDb.get(new Buffer('beep'), function (err, value) {
        t.deepEqual(value, new Buffer('boop'))
        t.end()
      })
    })
  })
})

test('del', function (t) {
  setup(function (client, server, serverDb) {
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
})

test('batch', function (t) {
  setup(function (client, server, serverDb) {
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
})

test('multiple batches directly after each other', function (t) {
  t.plan(3)

  var serverDb = memDOWN('/does/not/matter')

    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()
    , bufferingStream = createBufferingStream()

  server
    .pipe(client.createRpcStream())
    .pipe(bufferingStream)
    .pipe(server)

  client.batch()
    .put(new Buffer('beep'), new Buffer('boop'))
    .write(function () {
      serverDb.get(new Buffer('beep'), function (err, value) {
        t.deepEqual(value, new Buffer('boop'))
      })
    })

  client.batch()
    .put(new Buffer('hello'), new Buffer('world'))
    .write(function () {
      serverDb.get(new Buffer('hello'), function (err, value) {
        t.deepEqual(value, new Buffer('world'))
      })
    })

  client.batch()
    .put(new Buffer('foo'), new Buffer('bar'))
    .write(function () {
      serverDb.get(new Buffer('foo'), function (err, value) {
        t.deepEqual(value, new Buffer('bar'))
      })
    })

  setImmediate(function () {
    bufferingStream.flush()
  })
})

test('batch error handling', function (t) {
  var serverDb = {
        batch: function () {
          return {
              put: function () { return this }
            , write: function (callback) {
                callback(new Error('Something bad happened'))
              }
          }
        }
      }
    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()

  server
    .pipe(client.createRpcStream())
    .pipe(server)

  client.batch()
    .put(new Buffer('hello'), new Buffer('world'))
    .write(function (err) {
      t.ok(err instanceof Error)
      if (err instanceof Error)
        t.equal(err.message, 'Something bad happened')
      t.end()
    })
})

test('new iterator', function (t) {
  var server = remoteDOWN.server({
        iterator: function (options) {
          t.deepEqual(
              options
            , { reverse: false }
          )
          t.end()
        }
      })
    , client = remoteDOWN.client()

  server.pipe(client.createRpcStream()).pipe(server)

  client.iterator()
})

return
test('iterator', function (t) {
  var serverDb = memDOWN('/does/not/matter')

    , server = remoteDOWN.server(serverDb)
    , client = remoteDOWN.client()

  server.pipe(client.createRpcStream()).pipe(server)

  serverDb.batch(
      [
          { key: new Buffer([ 0 ]), value: new Buffer('one'), type: 'put' }
        , { key: new Buffer([ 1 ]), value: new Buffer('two'), type: 'put' }
      ]
    , function () {
        var iterator = client.iterator()

        iterator.next(function (error, key, value) {
          t.deepEqual(key, new Buffer([ 0 ]))
          t.deepEqual(value, new Buffer('one'))
          iterator.next(function (err, key, value) {
            t.deepEqual(key, new Buffer([ 1 ]))
            t.deepEqual(value, new Buffer('two'))
            iterator.next(function () {
              t.equal(arguments.length, 0)
              t.end()
            })
          })
        })
      }
  )
})
