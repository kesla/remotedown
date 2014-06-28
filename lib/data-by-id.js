var DataById = function () {
      if (!(this instanceof DataById))
        return new DataById()

      this.array = []
      this.nextId = 0
    }

DataById.prototype.add = function (callback) {
  var id = this.nextId++
  this.array[id] = callback
  return id
}

DataById.prototype.get = function (id) {
  return this.array[id]
}

DataById.prototype.remove = function (id) {
  var callback = this.array[id]
  delete this.array[id]
  return callback
}

module.exports = DataById