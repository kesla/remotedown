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

  , writeToBuffer = function (buffer, data, offset) {
      if (typeof(data) === 'string') {
        buffer.write(data, offset)
      } else {
        data.copy(buffer, offset)
      }
    }

require('inherits')(Server, Transform)
require('inherits')(Client, Duplex)

Server.prototype._parse = function (batch) {
  var self = this
    , ptr = 0
    , ids = []
    , dataLength
    , decodePut = function () {
        var keyLength = self._transformBuffer.readUInt32LE(ptr)
          , key = self._transformBuffer.slice(ptr + 4, ptr + keyLength + 4)
          , valueLength = self._transformBuffer.readUInt32LE(ptr + keyLength + 4)
          , value = self._transformBuffer.slice(
                ptr + keyLength + 8
              , ptr + keyLength + valueLength + 8
            )

        ptr += keyLength + valueLength + 8
        batch.put(key, value)
        dataLength -= (keyLength + valueLength + 8)
      }
    , decodeDel = function () {
        var keyLength = self._transformBuffer.readUInt32LE(ptr)
          , key = self._transformBuffer.slice(ptr + 4, ptr + keyLength + 4)

        ptr += keyLength + 4
        batch.del(key)
        dataLength -= (keyLength + 4)
      }

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
      if (this._transformBuffer[ptr] === 1) {
        ptr++
        dataLength--
        decodePut()
      }
      if (this._transformBuffer[ptr] === 0) {
        ptr++
        dataLength--
        decodeDel()
      }
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

  this._callbacks[id] = callback

  array.forEach(function (obj) {
    if (obj.type === 'put')
      dataLength += obj.key.length + obj.value.length + 9
    if (obj.type === 'del')
      dataLength += obj.key.length + 5
  })

  var buffer = new Buffer(dataLength + 8)

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
    var buf = Buffer.concat(this._outputBuffer)
    this._outputBuffer.length = 0

    if (!this.push(buf))
      this._waitingForData = false
  }
}

Client.prototype.del = function (key, callback) {
  this.batch([ { key: key, type: 'del' } ], callback)
}

Client.prototype.put = function (key, value, callback) {
  this.batch([ { key: key, value: value, type: 'put' } ], callback)
}

module.exports = {
    server: Server
  , client: Client
}