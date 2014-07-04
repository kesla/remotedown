var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

  , ClientIterator = require('./client-iterator')
  , ClientStream = require('./client-stream')
  , DataById = require('./data-by-id')
  , encoders = require('./encoders')
  , formatBatchArray = require('./format-batch-array')

  , Client = function () {
      if (!(this instanceof Client))
        return new Client()

      AbstractLevelDOWN.call(this, '/does/not/matter')

      this._callbacks = new DataById()
      this._inputBuffer = null
      this.stream = null
    }

require('inherits')(Client, AbstractLevelDOWN)

Client.prototype.createRpcStream = function () {
  this.stream = new ClientStream(this)
  return this.stream
}

// called from the client-stream
Client.prototype._write = function (chunk, encoding, callback) {
  var dataLength
    , data
    , id
    , isError
    , ptr = 0

  this._inputBuffer = this._inputBuffer ?
    Buffer.concat([ this._inputBuffer, chunk ]) : chunk

  // header is dataLength (UInt64LE), callback id (UInt64LE),
  //  isError id (1 byte) and then a payload
  while(ptr + 9 <= this._inputBuffer.length) {
    dataLength = this._inputBuffer.readUInt32LE(ptr)
    id = this._inputBuffer.readUInt32LE(ptr + 4)
    isError = this._inputBuffer[ptr + 8]

    if (ptr + 9 + dataLength > this._inputBuffer.length)
      break

    ptr += 9
    data = this._inputBuffer.slice(ptr, ptr + dataLength)
    ptr += dataLength

    if (isError) {
      this._callbacks.remove(id)(new Error(data.toString()))
    } else {
      if (dataLength)
        this._callbacks.remove(id)(null, data)
      else
        this._callbacks.remove(id)()
    }
  }

  if (ptr === this._inputBuffer.length)
    this._inputBuffer = null
  else
    this._inputBuffer = this._inputBuffer.slice(ptr)

  callback()
}

// called from the client-stream
// empty cause right now we just push data to stream directly when available
Client.prototype._read = function () {}

Client.prototype._batch = function (array, options, callback) {
  if (array.length === 0)
    return setImmediate(callback)

  array = formatBatchArray(array)

  var id = this._callbacks.add(callback)
    , ptr = 0
    , dataLength = array.reduce(
          function (length, obj) {
            if (obj.type === 'put')
              length += obj.key.length + obj.value.length + 9
            if (obj.type === 'del')
              length += obj.key.length + 5
            return length
          }
        , 0
      )
    // need room for the header
    , buffer = new Buffer(dataLength + 9)

  ptr = encoders.batchHeader(buffer, dataLength, id, ptr)

  array.forEach(function (obj) {
    if (obj.type === 'put')
      ptr = encoders.put(buffer, obj, ptr)
    if (obj.type === 'del')
      ptr = encoders.del(buffer, obj, ptr)
  })

  this.stream.push(buffer)
}

Client.prototype.iterator = function (options) {
  options = options || {}

  return new ClientIterator(this, this._callbacks, options)
}

Client.prototype._del = function (key, options, callback) {
  this._batch([ { key: key, type: 'del' } ], options, callback)
}

Client.prototype._put = function (key, value, options, callback) {
  this._batch([ { key: key, value: value, type: 'put' } ], options, callback)
}

Client.prototype._get = function (key, options, callback) {
  var id = this._callbacks.add(function (err, value) {
      if (err)
        callback(err)
      else
        callback(
            null
          , options.asBuffer === false ? value.toString() : value
        )
    })
    , ptr = 0
    , dataLength = key.length
    // need room for the header
    , buffer = new Buffer(dataLength + 9)


  ptr = encoders.get(buffer, key, id, ptr)

  this.stream.push(buffer)
}

module.exports = Client