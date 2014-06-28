var Callbacks = function () {
      this.array = []
      this.nextId = 0
    }

Callbacks.prototype.add = function (callback) {
  var id = this.nextId++
  this.array[id] = callback
  return id
}

Callbacks.prototype.remove = function (id) {
  var callback = this.array[id]
  delete this.array[id]
  return callback
}

module.exports = Callbacks