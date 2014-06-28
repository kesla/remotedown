var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

  , ClientStream = require('./client-stream')

  , Cache = function () {
      this.array = []
      this.size = 0
    }

  , Client = function () {
      if (!(this instanceof Client))
        return new Client()

      AbstractLevelDOWN.call(this, '/does/not/matter')

      this._nextId = 0
      this._callbacks = []
      this._inputBuffer = null
      this._outputBuffer = new Cache()
      this._waitingForData = false
      this._stream = null
    }

  , writeToBuffer = function (buffer, data, offset) {
      if (typeof(data) === 'string') {
        buffer.write(data, offset)
      } else {
        data.copy(buffer, offset)
      }
    }

require('inherits')(Client, AbstractLevelDOWN)

Cache.prototype.push = function (buffer) {
  this.array.push(buffer)
  this.size += buffer.length
}

Cache.prototype.flush = function () {
  var buffer = Buffer.concat(this.array, this.size)
  this.array.length = 0
  this.size = 0
  return buffer
}

Client.prototype.createRpcStream = function () {
  this._stream = new ClientStream(this)
  return this._stream
}

// called from the client-stream
Client.prototype._write = function (chunk, encoding, callback) {
  var id
    , ptr = 0

  this._inputBuffer = this._inputBuffer ?
    Buffer.concat([ this._inputBuffer, chunk ]) : chunk

  while(ptr <= this._inputBuffer.length - 4) {
    id = this._inputBuffer.readUInt32LE(ptr)

    this._callbacks[id]()
    delete this._callbacks[id]
    ptr += 4
  }

  if (ptr === this._inputBuffer.length)
    this._inputBuffer = null
  else
    this._inputBuffer = this._inputBuffer.slice(ptr)

  callback()
}

// called from the client-stream
Client.prototype._read = function () {
  if (this._outputBuffer.size > 0) {
    this._stream.push(this._outputBuffer.flush())
  } else {
    this._waitingForData = true
  }
}

Client.prototype._batch = function (array, options, callback) {
  var self = this
    , id = this._nextId++
    , dataLength = 0
    , ptr = 0
    , encodePut = function (obj) {
        var key = obj.key
          , value = obj.value

        buffer[ptr] = 1
        ptr++

        buffer.writeUInt32LE(key.length, ptr)
        ptr += 4
        writeToBuffer(buffer, key, ptr)
        ptr += key.length

        buffer.writeUInt32LE(value.length, ptr)
        ptr += 4
        writeToBuffer(buffer, value, ptr)
        ptr += value.length
      }
    , encodeDel = function (obj) {
        var key = obj.key

        buffer[ptr] = 0
        ptr++

        buffer.writeUInt32LE(key.length, ptr)
        ptr += 4
        writeToBuffer(buffer, key, ptr)
        ptr += key.length
      }
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
    , buffer = new Buffer(dataLength + 8)

  this._callbacks[id] = callback

  // write the header
  buffer.writeUInt32LE(dataLength, ptr)
  ptr += 4
  buffer.writeUInt32LE(id, ptr)
  ptr += 4

  array.forEach(function (obj) {
    if (obj.type === 'put')
      encodePut(obj)
    if (obj.type === 'del')
      encodeDel(obj)
  })

  this._outputBuffer.push(buffer)

  if (this._waitingForData) {
    if (!this._stream.push(this._outputBuffer.flush()))
      this._waitingForData = false
  }
}

Client.prototype._del = function (key, options, callback) {
  this._batch([ { key: key, type: 'del' } ], options, callback)
}

Client.prototype._put = function (key, value, options, callback) {
  this._batch([ { key: key, value: value, type: 'put' } ], options, callback)
}

module.exports = Client