var writeToBuffer = function (buffer, data, offset) {
      if (typeof(data) === 'string') {
        buffer.write(data, offset)
      } else {
        data.copy(buffer, offset)
      }
    }

  , encoders = {
        batchHeader: function (buffer, dataLength, id, ptr) {
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

          buffer.writeUInt32LE(key.length, ptr)
          ptr += 4
          writeToBuffer(buffer, key, ptr)
          ptr += key.length

          buffer.writeUInt32LE(value.length, ptr)
          ptr += 4
          writeToBuffer(buffer, value, ptr)
          ptr += value.length

          return ptr
        }
    }

module.exports = encoders