var types = require('./types')

  , writeToBuffer = function (buffer, data, ptr) {
      if (typeof(data) === 'string') {
        buffer.write(data, ptr)
      } else if (Buffer.isBuffer(data)) {
        data.copy(buffer, ptr)
      } else if (typeof(data) === 'number') {
        buffer.writeUInt32LE(data, ptr)
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
  , encodeRequest = function (type, data, id) {
      var dataLength = typeof(data) === 'number' ? 4 : data.length
        , buffer = new Buffer(dataLength + 9)

      writeUInt32LE(buffer, dataLength + 5, 0)
      writeUInt8(buffer, type, 4)
      writeUInt32LE(buffer, id, 5)
      writeToBuffer(buffer, data, 9)

      return buffer
    }

  , encoders = {
        batch: function (data, id) {
          return encodeRequest(types.batch, data, id)
        }
      , get: function (key, id) {
          return encodeRequest(types.get, key, id)
        }
      , newIterator: function (data, id) {
          return encodeRequest(types.newIterator, data, id)
        }
      , iteratorNext: function (iteratorId, id) {
          return encodeRequest(types.iteratorNext, iteratorId, id)
        }
      , iteratorEnd: function (iteratorId, id) {
          return encodeRequest(types.iteratorEnd, iteratorId, id)
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