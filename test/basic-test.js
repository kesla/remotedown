var test       = require('tape')
  , testCommon = require('abstract-leveldown/testCommon')

  , common = require('./common')
  , remoteDOWN = require('../remotedown')

  , leveldown = common.factory
  , db

test('setUp common', testCommon.setUp)
test('setUp db', function (t) {
  db = require('leveldown')(testCommon.location())
  db.open(t.end.bind(t))
})

test('multiple batches directly after each other', function (t) {
  t.plan(3)

  var server = remoteDOWN.server(db)
    , client = remoteDOWN.client()
    , bufferingStream = common.createBufferingStream()

  server
    .pipe(client.createRpcStream())
    .pipe(bufferingStream)
    .pipe(server)

  client.batch()
    .put(new Buffer('beep'), new Buffer('boop'))
    .write(function () {
      db.get(new Buffer('beep'), function (err, value) {
        t.deepEqual(value, new Buffer('boop'))
      })
    })

  client.batch()
    .put(new Buffer('hello'), new Buffer('world'))
    .write(function () {
      db.get(new Buffer('hello'), function (err, value) {
        t.deepEqual(value, new Buffer('world'))
      })
    })

  client.batch()
    .put(new Buffer('foo'), new Buffer('bar'))
    .write(function () {
      db.get(new Buffer('foo'), function (err, value) {
        t.deepEqual(value, new Buffer('bar'))
      })
    })

  setImmediate(bufferingStream.flush)
})

test('multiple iterators', function (t) {
  var server = remoteDOWN.server(db)
    , client = remoteDOWN.client()

  server
    .pipe(common.createSplittingStream())
    .pipe(client.createRpcStream())
    .pipe(common.createSplittingStream())
    .pipe(server)

  db.batch(
      [
          { key: new Buffer('0'), value: new Buffer('one'), type: 'put' }
      ]
    , function () {
        var iterator = client.iterator()
        iterator.next(function (error, key, value) {
          t.deepEqual(key, new Buffer('0'))
          t.deepEqual(value, new Buffer('one'))
          iterator.end(function () {
            t.equal(arguments.length, 0)

            var iterator2 = client.iterator()
            iterator2.next(function (error, key, value) {
              t.deepEqual(key, new Buffer('0'))
              t.deepEqual(value, new Buffer('one'))
              iterator2.end(function () {
                t.equal(arguments.length, 0)
                t.end()
              })
            })
          })
        })
      }
  )
})

test('tearDown', function (t) {
  db.close(testCommon.tearDown.bind(null, t))
})