var Cache = function () {
      this.array = []
      this.size = 0
    }

Cache.prototype.push = function (buffer) {
  this.array.push(buffer)
  this.size += buffer.length
}

Cache.prototype.flush = function () {
  var buffer = Buffer.concat(this.array, this.size)
  this.array.length = 0
  this.size = 0
  return buffer
}

module.exports = Cache