var normalize = function (data) {
      if (typeof(data) !== 'string' && !Buffer.isBuffer(data))
        data = String(data)

      return data
    }
  , formatBatchArray = function (array) {
      return array.map(function (obj) {
        if (obj.type === 'put')
          obj = [ normalize(obj.key), normalize(obj.value) ]

        if (obj.type === 'del')
          obj = [ normalize(obj.key) ]

        return obj
      })
    }

module.exports = formatBatchArray