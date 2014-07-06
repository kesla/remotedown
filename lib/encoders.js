var types = require('./types')

  , writeToBuffer = function (buffer, data, ptr) {
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
      , batch: function (buffer, data, id, ptr) {
          // write the header
          ptr = writeUInt32LE(buffer, data.length + 5, ptr)
          ptr = writeUInt8(buffer, types.batch, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          // write the data
          writeToBuffer(buffer, data, ptr)

          return ptr
        }
      , get: function (buffer, key, id, ptr) {
          ptr = writeUInt32LE(buffer, key.length + 5, ptr)
          ptr = writeUInt8(buffer, types.get, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeToBuffer(buffer, key, ptr)

          return ptr
        }
      , newIterator: function (buffer, data, id, ptr) {
          ptr = writeUInt32LE(buffer, data.length + 5, ptr)
          ptr = writeUInt8(buffer, types.newIterator, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeToBuffer(buffer, data, ptr)

          return ptr
        }
      , iteratorNext: function (buffer, iteratorId, id, ptr) {
          ptr = writeUInt32LE(buffer, 9, ptr)
          ptr = writeUInt8(buffer, types.iteratorNext, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeUInt32LE(buffer, iteratorId, ptr)

          return ptr
        }
      , iteratorEnd: function (buffer, iteratorId, id, ptr) {
          ptr = writeUInt32LE(buffer, 9, ptr)
          ptr = writeUInt8(buffer, types.iteratorEnd, ptr)
          ptr = writeUInt32LE(buffer, id, ptr)
          ptr = writeUInt32LE(buffer, iteratorId, ptr)

          return ptr
        }
      , okResponse: function (buffer, ids, data, ptr) {
          var dataLength = (data ? data.length : 0) + 5

          for (var i = 0; i < ids.length; ++i) {
            ptr = writeUInt32LE(buffer, dataLength, ptr)
            ptr = writeUInt32LE(buffer, ids[i], ptr)
            ptr = writeUInt8(buffer, types.okResponse, ptr)

            if (data)
              ptr = writeToBuffer(buffer, data, ptr)
          }

          return ptr
        }
      , errorResponse: function (buffer, ids, data, ptr) {
          var dataLength = data.length + 5

          for (var i = 0; i < ids.length; ++i) {
            ptr = writeUInt32LE(buffer, dataLength, ptr)
            ptr = writeUInt32LE(buffer, ids[i], ptr)
            ptr = writeUInt8(buffer, types.errorResponse, ptr)
            ptr = writeToBuffer(buffer, data, ptr)
          }

          return ptr
        }
    }

module.exports = encoders