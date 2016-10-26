'use strict';
var Promise = require('bluebird');
var deepEqual = require('deep-equal');
var defaults = require('defaults');
var fs = require('fs-extra');
var readline = require('readline');
var tmp = require('tmp');
var uuid = require('node-uuid');

tmp.setGracefulCleanup();

var fsClose = Promise.promisify(fs.close);
var fsEnsureFile = Promise.promisify(fs.ensureFile);
var fsFTruncate = Promise.promisify(fs.ftruncate);
var fsOpen = Promise.promisify(fs.open);
var fsRead = Promise.promisify(fs.read);
var fsWrite = Promise.promisify(fs.write);
var tmpName = Promise.promisify(tmp.tmpName);

module.exports = Database;

Database.MAX_CACHE_SIZE_DEFAULT = 134217728; // 128 MB

function Database(fileName) {
  this.fileName = fileName;
  this.items = {};

  this.cache = [];
  this.cacheHoles = {};
  this.maxCacheSize = Database.MAX_CACHE_SIZE_DEFAULT;

  this.blocks = 0;
  this.blockSize = -1;
  this.blockHoles = {};

  this.operators = {
    $lt: function (itemValue, testValue) {
      return itemValue < testValue;
    },
    $lte: function (itemValue, testValue) {
      return itemValue <= testValue;
    },
    $gt: function (itemValue, testValue) {
      return itemValue > testValue;
    },
    $gte: function (itemValue, testValue) {
      return itemValue >= testValue;
    },
    $in: function(itemValue, testValues) {
      return testValues.indexOf(itemValue) >= 0;
    },
    $ne: function(itemValue, testValue) {
      return !deepEqual(itemValue, testValue);
    },
    $nin: function(itemValue, testValues) {
      return testValues.indexOf(itemValue) < 0;
    },
    $exists: function(itemValue, testValue) {
      return (itemValue === undefined) === testValue;
    },
    $regex: function(itemValue, testValue) {
      return testValue.test(itemValue);
    }
  };

  this.arrayOperators = {
    $size: function(itemArray, testValue) {
      if (typeof testValue === 'object') {
        for (var key in testValue) {
          if (testValue.hasOwnProperty(key)) {
            var operator = this.operators[key];
            if (operator && !operator(itemArray.length, testValue[key])) {
              return false;
            }
          }
        }
      }
      return itemArray.length === testValue;
    }
  };
}

Database.prototype.load = function() {
  var that = this;
  var initPromise;
  if (!this.fileName) {
    initPromise = tmpName()
      .then(function(path) {
        that.fileName = path;
      });
  } else {
    initPromise = Promise.resolve();
  }
  return initPromise
    .then(function() {
      return fsEnsureFile(that.fileName);
    })
    .then(function() {
      var lineReader = readline.createInterface({
        input: fs.createReadStream(that.fileName)
      });

      var block = 0;
      var promise = new Promise(function(resolve, reject) {
        lineReader.on('line', function(line) {
          var item = JSON.parse(line);
          if (that.blockSize < 0) {
            that.blockSize = line.length + 1;
          } else if (line.length + 1 !== that.blockSize) {
            reject(new Error('Invalid database: all blocks must be of uniform size: ' + that.blockSize + ' bytes.'));
          }
          var itemData = {
            block: block,
            cached: false,
            cacheIndex: -1,
          };
          if (that.blockSize * (that.cache.length + 1) <= that.maxCacheSize) {
            itemData.cached = true;
            itemData.cacheIndex = that.cache.length;
            that.cache.push(item);
          }
          that.items[item._id] = itemData;
          block++;
        });
        lineReader.on('close', function() {
          that.blocks = block;
          resolve(that);
        });
        lineReader.on('error', function(err) {
          reject(err);
        });
      });
      return promise;
    });
};

Database.prototype.close = function() {
  this.items = {};
  this.cache = [];
};

Database.prototype.iterateOutOfCore = function(startBlock, handler) {
  var lineReader = readline.createInterface({
    input: fs.createReadStream(this.fileName, {
      start: startBlock * this.blockSize
    })
  });
  var stopped = false;
  var promise = new Promise(function(resolve, reject) {
    lineReader.on('line', function(line) {
      var item = JSON.parse(line);
      if (!stopped && !handler(item)) {
        stopped = true;
        lineReader.close();
      }
    });
    lineReader.on('close', function() {
      resolve(!stopped);
    });
    lineReader.on('error', function(err) {
      reject(err);
    });
  });
  return promise;
};

Database.prototype.iterate = function(handler) {
  var items = this.items;
  var cache = this.cache;
  var stopped = false;
  for (var id in items) {
    if (items.hasOwnProperty(id)) {
      var item = items[id];
      if (item.cached) {
        if (!handler(cache[item.cacheIndex])) {
          stopped = true;
          break;
        }
      }
    }
  }
  if (this.blocks > cache.length) {
    return this.iterateOutOfCore(cache.length, handler);
  }
  return Promise.resolve(!stopped);
};

function matches(db, item, query) {
  for (var key in query) {
    if (query.hasOwnProperty(key)) {
      var queryValue = query[key];
      var itemValue = item[key];
      var operation = false;
      if (itemValue && queryValue instanceof RegExp) {
        if (!queryValue.test('' + itemValue)) {
          return false;
        }
      } else if (typeof queryValue === 'object') {
        for (var operatorKey in queryValue) {
          if (queryValue.hasOwnProperty(operatorKey)) {
            var operator = db.operators[operatorKey];
            if (operator) {
              var value = queryValue[operatorKey];
              if (!operator(itemValue, value)) {
                return false;
              }
              operation = true;
            }
          }
        }
      }
      if (!operation && !deepEqual(queryValue, itemValue)) {
        return false;
      }
    }
  }
  return true;
}

function addToResults(item, results, options) {
  options = defaults(options, {});
  results.push(item);
  if (options.limit !== undefined) {
    if (results.length > options.limit) {
      results.sort();
      results.pop();
    }
  }
}

Database.prototype.find = function(query, options) {
  var results = [];
  var that = this;
  return this.iterate(function(item) {
    if (matches(that, item, query)) {
      addToResults(item, results, options);
    }
    return true;
  }).then(function() {
    return results;
  });
};

Database.prototype.findOne = function(query) {
  var result;
  var that = this;
  return this.iterate(function(item) {
    if (matches(that, item, query)) {
      result = item;
      return false;
    }
    return true;
  }).then(function() {
    return result;
  });
};

function blankEntry(buffer) {
  for (var i = 0; i < buffer.length - 1; i++) {
    buffer.write(' ', i, 'utf8');
  }
  buffer.write('\n', buffer.length - 1, 'utf8');
}

function nextPowerOfTwo(num) {
  return Math.pow(2,Math.ceil(Math.log(num)/Math.log(2)));
}

Database.prototype.resize = function(blockSize, options) {
  options = defaults(options, {
    concurrency: 1
  });
  var that = this;
  var cache = this.cache;
  var items = this.items;
  var i;
  var buffer = new Buffer(blockSize);
  if (blockSize !== this.blockSize) {
    return fsOpen(this.fileName, 'r+')
      .then(function(fd) {
        if (that.blocks > cache.length) {
          // The whole database is not in memory, so some on-disk rewriting is required
          blankEntry(buffer);
          var moveIndices = [];
          if (blockSize > that.blockSize) {
            // The block size has increased, move the entries starting from the bottom
            for (i = that.blocks - 1; i >= cache.length; i--) {
              moveIndices.push(i);
            }
          } else {
            // The block size has decreased, move the entries starting from the top
            for (i = cache.length; i < that.blocks; i++) {
              moveIndices.push(i);
            }
          }
          return Promise.mapSeries(moveIndices, function(index) {
            return fsRead(fd, buffer, 0, Math.min(that.blockSize - 1, buffer.length - 1), index * that.blockSize)
              .then(function() {
                return fsWrite(fd, buffer, 0, buffer.length, index * blockSize);
              })
              .then(function() {
                blankEntry(buffer);
              });
          }, options).then(function() {
            if (blockSize <= that.blockSize) {
              // The file needs to be truncated to delete hanging characters
              return fsFTruncate(fd, that.blocks * blockSize)
                .then(function() {
                  return fd;
                });
            }
            return fd;
          });
        }
        return Promise.resolve(fd);
      })
      .then(function(fd) {
        // All of the remaining data is in memory, just write it back
        return Promise.map(cache, function(data) {
          var item = items[data._id];
          var dataString = JSON.stringify(data);
          blankEntry(buffer);
          buffer.write(dataString);
          return fsWrite(fd, buffer, 0, buffer.length, item.block * blockSize);
        }, options).then(function() {
          return fd;
        });
      })
      .then(function(fd) {
        return fsClose(fd);
      })
      .then(function() {
        that.blockSize = blockSize;
        // Resize the cache if we now exceed maxCacheSize
        while (that.blockSize * that.cache.length > that.maxCacheSize) {
          var item = that.cache.pop();
          var itemData = that.items[item._id];
          itemData.cached = false;
          itemData.cacheIndex = -1;
        }
        return that;
      });
  }
};

Database.prototype.insert = function(item) {
  var items = this.items;
  var cache = this.cache;
  var blocks = this.blocks;
  var that = this;

  if (Array.isArray(item)) {
    return Promise.map(item, function(subItem) {
      return that.insert(subItem);
    }, {
      concurrency: 1
    });
  }

  var itemData;
  if (item._id) {
    itemData = items[item._id];
  }
  if (!itemData) {
    item._id = uuid.v1();
    itemData = {
      block: blocks,
      cached: false,
      cacheIndex: -1,
    };
    this.blocks++;
  }
  var block = itemData.block;
  items[item._id] = itemData;

  var itemString = JSON.stringify(item);
  var promise;
  if (itemString.length + 1 > this.blockSize) {
    // The new item is larger than the block size, resize the database
    promise = this.resize(nextPowerOfTwo(itemString.length + 1));
  } else {
    promise = Promise.resolve();
  }
  return promise.then(function() {
    if (itemData.cached) {
      cache[itemData.cacheIndex] = item;
    } else if (blocks <= cache.length && that.blockSize * (cache.length + 1) <= that.maxCacheSize) {
      // There is room, add it to the cache
      itemData.cached = true;
      itemData.cacheIndex = cache.length;
      cache.push(item);
    }
    // Write the new entry to disk
    var buffer = new Buffer(that.blockSize);
    blankEntry(buffer);
    buffer.write(itemString);
    return fsOpen(that.fileName, 'r+')
      .then(function(fd) {
        return fsWrite(fd, buffer, 0, buffer.length, block * that.blockSize)
          .then(function() {
            return fd;
          });
      })
      .then(function(fd) {
        return fsClose(fd);
      });
  });
};

function moveBlock(db, fd, fromBlock, toBlock) {
  var buffer = new Buffer(db.blockSize);
  return fsRead(fd, buffer, 0, buffer.length, fromBlock * buffer.length)
    .then(function() {
      var item = JSON.parse(buffer.toString());
      var id = item._id;
      var itemData = db.items[id];
      itemData.block = toBlock;
      return fsWrite(fd, buffer, 0, buffer.length, toBlock * buffer.length)
        .then(function() {
          return item;
        });
    });
}

function getLastNBlocks(db, n) {
  var blocks = [];
  var i = db.blocks - 1;
  while (blocks.length < n && i >= 0) {
    while (db.blockHoles[i]) {
      i--;
    }
    blocks.push(i);
    i--;
  }
  return blocks;
}

function getLastNCacheIndices(db, n) {
  var indices = [];
  var cache = db.cache;
  var i = cache.length - 1;
  while (indices.length < n && i >= 0) {
    while (db.cacheHoles[i]) {
      i--;
    }
    indices.push(i);
    i--;
  }
  return indices;
}

function fillHoles(db, options) {
  var blocks = db.blocks;
  var blockHoles = Object.keys(db.blockHoles);
  var cacheHoles = Object.keys(db.cacheHoles);
  var fromBlocks = getLastNBlocks(db, blockHoles.length);
  return fsOpen(db.fileName, 'r+')
    .then(function(fd) {
      // Move elements from the bottom of the database up to fill the holes
      return Promise.map(blockHoles, function(block, index) {
        var fromBlock = fromBlocks[index];
        if (fromBlock && fromBlock > block) {
          return moveBlock(db, fd, fromBlock, block)
            .then(function(item) {
              if (block < db.cache.length) {
                // If we are filling a block hole above the cache line, this item can be placed into the cache
                var cacheHole = cacheHoles.shift();
                var itemData = db.items[item._id];
                itemData.cached = true;
                itemData.cacheIndex = cacheHole;
                db.cache[cacheHole] = item;
              }
            });
        }
      }, options)
      .then(function() {
        db.blockHoles = {};
        db.blocks = blocks - blockHoles.length;
        return fd;
      });
    })
    .then(function(fd) {
      // Truncate any extra data from the bottom of the file
      return fsFTruncate(fd, db.blocks * db.blockSize)
        .then(function() {
          return fd;
        });
    })
    .then(function(fd) {
      // If there are unfilled cache holes, then the cache needs to be resized
      var newCacheSize = Math.min(db.blocks, db.cache.length - cacheHoles.length);
      if (cacheHoles.length > 0) {
        var indices = getLastNCacheIndices(db, cacheHoles.length);
        // Make sure there are no holes in the middle of the cache
        for (var i = 0; i < cacheHoles.length && indices.length > 0; i++) {
          var cacheHole = cacheHoles[i];
          if (cacheHole < newCacheSize) {
            // Fill this cache hole with the last item in the cache
            var index = indices.shift();
            var item = db.cache[index];
            var itemData = db.items[item._id];
            itemData.cacheIndex = index;
            db.cache[index] = item;
          }
        }
      }
      // Resize the cache
      while (db.cache.length > newCacheSize) {
        db.cache.pop();
      }
      db.cacheHoles = {};
      return fsClose(fd);
    });
}

function deleteItem(db, id) {
  var itemData = db.items[id];
  if (itemData.cached) {
    var cacheIndex = itemData.cacheIndex;
    db.cacheHoles[cacheIndex] = true;
  }
  var block = itemData.block;
  db.blockHoles[block] = true;
  delete db.items[id];
}

Database.prototype.delete = function(query, options) {
  var db = this;
  var numRemoved = 0;
  options = defaults(options, {
    concurrency: 4,
    limit: Number.MAX_VALUE
  });

  var queries;
  if (!Array.isArray(query)) {
    queries = [query];
  } else {
    queries = query;
  }

  return this.iterate(function(item) {
    var match = false;
    for (var i = 0; i < queries.length; i++) {
      if (matches(db, item, queries[i])) {
        match = true;
        break;
      }
    }
    if (match) {
      deleteItem(db, item._id);
      numRemoved++;
      if (numRemoved >= options.limit) {
        return false;
      }
    }
    return true;
  }).then(function() {
    if (numRemoved > 0) {
      return fillHoles(db, options);
    }
    return Promise.resolve();
  }).then(function() {
    return numRemoved;
  });
};
