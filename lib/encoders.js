var writeToBuffer = function (buffer, data, ptr) {
      if (typeof(data) === 'string') {
        buffer.write(data, ptr)
      } else if (Buffer.isBuffer(data)) {
        data.copy(buffer, ptr)
      }
      return ptr + data.length
    }
  , writeUInt8 = function (buffer, value, ptr) {
      value = +value;
      ptr = ptr >>> 0;
      buffer[ptr] = value;
      return ptr + 1;
    }
  , writeUInt32LE = function (buffer, value, ptr) {
      value = +value;
      ptr = ptr >>> 0;
      buffer[ptr + 3] = (value >>> 24);
      buffer[ptr + 2] = (value >>> 16);
      buffer[ptr + 1] = (value >>> 8);
      buffer[ptr] = value;
      return ptr + 4
    }

  , encoders = {
        keyValue: function (buffer, key, value, ptr) {
          var keyLength = key? key.length : 0
            , valueLength = value? value.length : 0

          ptr = writeUInt32LE(buffer, keyLength, ptr)
          ptr = writeToBuffer(buffer, key, ptr)
          ptr = writeUInt32LE(buffer, valueLength, ptr)
          ptr = writeToBuffer(buffer, value, ptr)

          return ptr
        }
      , batchHeader: function (buffer, dataLength, id, ptr) {
          // write the header
          ptr = writeUInt8(buffer, 100, ptr)
          ptr = writeUInt32LE(buffer, dataLength, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)

          return ptr
        }
      , del: function (buffer, obj, ptr) {
          var key = obj.key

          ptr = writeUInt8(buffer, 0, ptr)
          ptr = writeUInt32LE(buffer, key.length, ptr)
          ptr = writeToBuffer(buffer, key, ptr)

          return ptr
        }
      , put: function (buffer, obj, ptr) {
          var key = obj.key
            , value = obj.value

          ptr = writeUInt8(buffer, 1, ptr)

          return this.keyValue(buffer, obj.key, obj.value, ptr)
        }
      , newIterator: function (buffer, data, id, ptr) {
          ptr = writeUInt8(buffer, 101, ptr)
          ptr = writeUInt32LE(buffer, data.length, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeToBuffer(buffer, data, ptr)

          return ptr
        }
      , iteratorNext: function (buffer, iteratorId, id, ptr) {
          ptr = writeUInt8(buffer, 102, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeUInt32LE(buffer, iteratorId, ptr)

          return ptr
        }
      , iteratorEnd: function (buffer, iteratorId, id, ptr) {
          ptr = writeUInt8(buffer, 103, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeUInt32LE(buffer, iteratorId, ptr)

          return ptr
        }
      , okResponse: function (buffer, ids, data, ptr) {
          var dataLength = data ? data.length : 0

          for (var i = 0; i < ids.length; ++i) {
            ptr = writeUInt32LE(buffer, dataLength, ptr)
            ptr = writeUInt32LE(buffer, ids[i], ptr)
            // is ok id
            ptr = writeUInt8(buffer, 0, ptr)

            if (dataLength)
              ptr = writeToBuffer(buffer, data, ptr)
          }

          return ptr
        }
      , errorResponse: function (buffer, ids, data, ptr) {
          var dataLength = data.length

          for (var i = 0; i < ids.length; ++i) {
            ptr = writeUInt32LE(buffer, dataLength, ptr)
            ptr = writeUInt32LE(buffer, ids[i], ptr)
            // it's an error id
            ptr = writeUInt8(buffer, 1, ptr)
            ptr = writeToBuffer(buffer, data, ptr)
          }

          return ptr
        }
    }

module.exports = encoders