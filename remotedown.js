var Duplex = require('stream').Duplex
  , Transform = require('stream').Transform

  , numbersToBuffer = require('numbers-to-buffer')

  , Server = function (db) {
      if (!(this instanceof Server))
        return new Server(db)

      Transform.call(this)
      this._transformBuffer = null
      this._db = db
    }

  , Client = function () {
      if (!(this instanceof Client))
        return new Client()

      Duplex.call(this)

      this._nextId = 0
      this._callbacks = []
      this._inputBuffer = null
      this._outputBuffer = []
      this._waitingForData = false
    }

require('inherits')(Server, Transform)
require('inherits')(Client, Duplex)

Server.prototype._parse = function (batch) {
  var ptr = 0
    , ids = []
    , dataLength
    , keyLength
    , key
    , valueLength
    , value

  while(ptr < this._transformBuffer.length - 8) {
    dataLength = this._transformBuffer.readUInt32LE(ptr)
    if (ptr + dataLength > this._transformBuffer.length) {
      this._transformBuffer = this._transformBuffer.slice(ptr)
      return ids
    }

    ptr += 4
    ids.push(this._transformBuffer.readUInt32LE(ptr))
    ptr += 4

    while(dataLength > 0) {
      keyLength = this._transformBuffer.readUInt32LE(ptr)
      ptr += 4
      key = this._transformBuffer.slice(ptr, ptr + keyLength)
      ptr += keyLength
      valueLength = this._transformBuffer.readUInt32LE(ptr)
      ptr += 4
      value = this._transformBuffer.slice(ptr, ptr + valueLength)
      ptr += valueLength
      batch.put(key, value)
      dataLength -= (keyLength + valueLength + 8)
    }
  }
  this._transformBuffer = this._transformBuffer.slice(ptr)
  return ids
}

Server.prototype._parseAndWrite = function (callback) {
  var batch = this._db.batch()
    , ids = this._parse(batch)
    , self = this

  batch.write(function () {
    self.push(numbersToBuffer.UInt32LE(ids))
    callback()
  })
}

Server.prototype._transform = function (chunk, encoding, callback) {
  this._transformBuffer = this._transformBuffer ?
    Buffer.concat([ this._transformBuffer, chunk ]) : chunk

  if (this._transformBuffer.length >= 8)
    this._parseAndWrite(callback)
  else
    callback()
}

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

Client.prototype._read = function () {
  if (this._outputBuffer.length > 0) {
    this.push(Buffer.concat(this._outputBuffer))
    this._outputBuffer.length = 0
  } else {
    this._waitingForData = true
  }
}

Client.prototype.batch = function (array, callback) {
  var id = this._nextId++
    , dataLength = 0
    , ptr = 0

  this._callbacks[id] = callback

  array.forEach(function (obj) {
    dataLength += obj.key.length + obj.value.length + 8
  })

  var buffer = new Buffer(dataLength + 8)

  buffer.writeUInt32LE(dataLength, ptr)
  ptr += 4
  buffer.writeUInt32LE(id, ptr)
  ptr += 4

  array.forEach(function (obj) {
    var key = obj.key
      , value = obj.value

    buffer.writeUInt32LE(key.length, ptr)

    ptr += 4

    if (typeof(key) === 'string') {
      buffer.write(key, ptr)
    } else {
      key.copy(buffer, ptr)
    }

    ptr += key.length

    buffer.writeUInt32LE(value.length, ptr)

    ptr += 4

    if (typeof(value) === 'string') {
      buffer.write(value, ptr)
    } else {
      value.copy(buffer, ptr)
    }
    ptr += value.length
  })

  this._outputBuffer.push(buffer)


  if (this._waitingForData) {
    var buf = Buffer.concat(this._outputBuffer)
    this._outputBuffer.length = 0

    if (!this.push(buf))
      this._waitingForData = false
  }
}

Client.prototype.put = function (key, value, callback) {
  this.batch([ { key: key, value: value, type: 'put' } ], callback)
}

module.exports = {
    server: Server
  , client: Client
}