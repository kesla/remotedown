var writeToBuffer = function (buffer, data, offset) {
      if (typeof(data) === 'string') {
        buffer.write(data, offset)
      } else if (Buffer.isBuffer(data)) {
        data.copy(buffer, offset)
      }
    }

  , encoders = {
        keyValue: function (buffer, key, value, ptr) {
          var keyLength = key? key.length : 0
            , valueLength = value? value.length : 0

          buffer.writeUInt32LE(keyLength, ptr)
          ptr += 4
          writeToBuffer(buffer, key, ptr)
          ptr += keyLength

          buffer.writeUInt32LE(valueLength, ptr)
          ptr += 4
          writeToBuffer(buffer, value, ptr)
          ptr += valueLength

          return ptr
        }
      , batchHeader: function (buffer, dataLength, id, ptr) {
          // write the header
          buffer[ptr] = 100
          ptr++
          buffer.writeUInt32LE(dataLength, ptr)
          ptr += 4
          buffer.writeUInt32LE(id, ptr)
          ptr += 4

          return ptr
        }
      , del: function (buffer, obj, ptr) {
          var key = obj.key

          buffer[ptr] = 0
          ptr++

          buffer.writeUInt32LE(key.length, ptr)
          ptr += 4
          writeToBuffer(buffer, key, ptr)
          ptr += key.length

          return ptr
        }
      , put: function (buffer, obj, ptr) {
          var key = obj.key
            , value = obj.value

          buffer[ptr] = 1
          ptr++

          return this.keyValue(buffer, obj.key, obj.value, ptr)
        }
      , newIterator: function (buffer, data, id, ptr) {
          buffer[ptr] = 101
          ptr++

          buffer.writeUInt32LE(data.length, ptr)
          ptr += 4
          buffer.writeUInt32LE(id, ptr)
          ptr += 4
          writeToBuffer(buffer, data, ptr)
          ptr += data.length

          return ptr
        }
      , iteratorNext: function (buffer, iteratorId, id, ptr) {
          buffer[ptr] = 102
          ptr++
          buffer.writeUInt32LE(id, ptr)
          ptr += 4
          buffer.writeUInt32LE(iteratorId, ptr)
          ptr += 4

          return ptr
        }
      , iteratorEnd: function (buffer, iteratorId, id, ptr) {
          buffer[ptr] = 103
          ptr++
          buffer.writeUInt32LE(id, ptr)
          ptr += 4
          buffer.writeUInt32LE(iteratorId, ptr)
          ptr += 4

          return ptr
        }
    }

module.exports = encoders