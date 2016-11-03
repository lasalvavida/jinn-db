'use strict';
var Promise = require('bluebird');
var clone = require('clone');
var deepEqual = require('deep-equal');
var defaults = require('defaults');
var fs = require('fs-extra');
var readline = require('readline');
var tmp = require('tmp');
var uuid = require('node-uuid');

tmp.setGracefulCleanup();

var fsClose = Promise.promisify(fs.close);
var fsCopy = Promise.promisify(fs.copy);
var fsEnsureFile = Promise.promisify(fs.ensureFile);
var fsFTruncate = Promise.promisify(fs.ftruncate);
var fsOpen = Promise.promisify(fs.open);
var fsRead = Promise.promisify(fs.read);
var fsWrite = Promise.promisify(fs.write);
var tmpName = Promise.promisify(tmp.tmpName);

module.exports = Database;

Database.MAX_CACHE_SIZE_DEFAULT = 134217728; // 128 MB

function Database(options) {
  if (typeof options === 'object') {
    options = defaults(options, {});
    this.fileName = options.fileName;
    this.copyOf = options.copyOf;
  }
  else {
    this.fileName = options;
  }
  this.items = {};

  this.cache = [];
  this.cacheHoles = {};
  this.maxCacheSize = Database.MAX_CACHE_SIZE_DEFAULT;

  this.blocks = 0;
  this.blockSize = -1;
  this.blockHoles = {};
}

function arrayContains(array, element) {
  for (var i = 0; i < array.length; i++) {
    if (deepEqual(array[i], element)) {
      return true;
    }
  }
  return false;
}

Database.prototype.operators = {
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

Database.prototype.queryOperators = {
  $or: function(itemValue, queries) {
    for (var i = 0; i < queries.length; i++) {
      if (this.matches(itemValue, queries[i])) {
        return true;
      }
    }
    return false;
  },
  $and: function(itemValue, queries) {
    for (var i = 0; i < queries.length; i++) {
      if (!this.matches(itemValue, queries[i])) {
        return false;
      }
    }
    return true;
  },
  $not: function(itemValue, query) {
    return !this.matches(itemValue, query);
  }
};

Database.prototype.updateOperators = {
  $set: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        item[key] = update[key];
      }
    }
  },
  $unset: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        delete item[key];
      }
    }
  },
  $inc: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        item[key] += update[key];
      }
    }
  },
  $min: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        item[key] = Math.min(item[key], update[key]);
      }
    }
  },
  $max: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        item[key] = Math.max(item[key], update[key]);
      }
    }
  },
  $push: function(item, update, forceUnique) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        var items = item[key];
        var value = update[key];
        var arrayValue = value.$each;
        if (arrayValue) {
          for (var i = 0; i < arrayValue.length; i++) {
            if (forceUnique && !arrayContains(items, arrayValue[i])) {
              items.push(arrayValue[i]);
            }
          }
          if (value.$sort) {
            items.sort();
          }
          if (value.$slice !== undefined) {
            items.splice(0, value.$slice);
          }
        } else {
          items.push(value);
        }
      }
    }
  },
  $pop: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        var value = update[key];
        if (value > 0) {
          item[key].pop();
        } else if (value < 0) {
          item[key].shift();
        }
      }
    }
  },
  $addToSet: function(item, update) {
    return this.$push(item, update, true);
  },
  $pull: function(item, update) {
    for (var key in update) {
      if (update.hasOwnProperty(key)) {
        var items = item[key];
        for (var i = 0; i < items.length; i++) {
          var item = items[i];
          if (this.matches(itemValue, update[key])) {
            items.splice(i, 1);
            i--;
          }
        }
      }
    }
  }
};

Database.prototype.load = function() {
  var db = this;
  var initPromise;
  if (!this.fileName) {
    initPromise = tmpName()
      .then(function(path) {
        db.fileName = path;
      });
  } else {
    initPromise = Promise.resolve();
  }
  if (this.copyOf) {
    initPromise = initPromise.then(function() {
      return fsCopy(db.copyOf, db.fileName);
    });
  }
  return initPromise
    .then(function() {
      return fsEnsureFile(db.fileName);
    })
    .then(function() {
      var lineReader = readline.createInterface({
        input: fs.createReadStream(db.fileName)
      });

      var block = 0;
      var promise = new Promise(function(resolve, reject) {
        lineReader.on('line', function(line) {
          var item = JSON.parse(line);
          if (db.blockSize < 0) {
            db.blockSize = line.length + 1;
          } else if (line.length + 1 !== db.blockSize) {
            reject(new Error('Invalid database: all blocks must be of uniform size: ' + db.blockSize + ' bytes.'));
          }
          var itemData = {
            block: block,
            cached: false,
            cacheIndex: -1,
          };
          if (db.blockSize * (db.cache.length + 1) <= db.maxCacheSize) {
            itemData.cached = true;
            itemData.cacheIndex = db.cache.length;
            db.cache.push(item);
          }
          db.items[item._id] = itemData;
          block++;
        });
        lineReader.on('close', function() {
          db.blocks = block;
          resolve(db);
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

Database.prototype.matches = function(item, query) {
  for (var key in query) {
    if (query.hasOwnProperty(key)) {
      var queryValue = query[key];
      var itemValue = item[key];
      var operation = false;
      var operator = this.queryOperators[key];
      if (operator) {
        if (!operator.call(this, item, queryValue)) {
          return false;
        }
        operation = true;
      } else if (itemValue && queryValue instanceof RegExp) {
        if (!queryValue.test('' + itemValue)) {
          return false;
        }
      } else if (typeof queryValue === 'object') {
        for (var operatorKey in queryValue) {
          if (queryValue.hasOwnProperty(operatorKey)) {
            operator = this.operators[operatorKey];
            var value = queryValue[operatorKey];
            if (operator) {
              if (!operator.call(this, itemValue, value)) {
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
};

function addToResults(item, query, results, options) {
  options = defaults(options, {});
  item = clone(item);
  if (options.projections) {
    var projections = options.projections;
    var keys = Object.keys(item);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var projection = projections[key];
      if (!projection && (key !== '_id' || projection === false)) {
        delete item[key];
      }
    }
  }
  results.push(item);
  if (query._id !== undefined && typeof query._id === 'string' && query._id.indexOf('$') < 0) {
    // If the query is id based, the first match is the only match
    return false;
  }
  else if (options.limit !== undefined) {
    if (results.length === options.limit && !options.sort) {
      return false;
    }
    else if (results.length > options.limit) {
      results.sort(options.sort);
      results.pop();
    }
  }
  return true;
}

Database.prototype.find = function(query, options) {
  var results = [];
  var db = this;
  return this.iterate(function(item) {
    if (db.matches(item, query)) {
      return addToResults(item, query, results, options);
    }
    return true;
  }).then(function() {
    return results;
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
  var db = this;
  var cache = this.cache;
  var items = this.items;
  var i;
  var buffer = new Buffer(blockSize);
  if (blockSize !== this.blockSize) {
    return fsOpen(this.fileName, 'r+')
      .then(function(fd) {
        if (db.blocks > cache.length) {
          // The whole database is not in memory, so some on-disk rewriting is required
          blankEntry(buffer);
          var moveIndices = [];
          if (blockSize > db.blockSize) {
            // The block size has increased, move the entries starting from the bottom
            for (i = db.blocks - 1; i >= cache.length; i--) {
              moveIndices.push(i);
            }
          } else {
            // The block size has decreased, move the entries starting from the top
            for (i = cache.length; i < db.blocks; i++) {
              moveIndices.push(i);
            }
          }
          return Promise.mapSeries(moveIndices, function(index) {
            return fsRead(fd, buffer, 0, Math.min(db.blockSize - 1, buffer.length - 1), index * db.blockSize)
              .then(function() {
                return fsWrite(fd, buffer, 0, buffer.length, index * blockSize);
              })
              .then(function() {
                blankEntry(buffer);
              });
          }, options).then(function() {
            if (blockSize <= db.blockSize) {
              // The file needs to be truncated to delete hanging characters
              return fsFTruncate(fd, db.blocks * blockSize)
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
        db.blockSize = blockSize;
        // Resize the cache if we now exceed maxCacheSize
        while (db.blockSize * db.cache.length > db.maxCacheSize) {
          var item = db.cache.pop();
          var itemData = db.items[item._id];
          itemData.cached = false;
          itemData.cacheIndex = -1;
        }
        return db;
      });
  }
};

Database.prototype.insert = function(item) {
  var items = this.items;
  var cache = this.cache;
  var blocks = this.blocks;
  var db = this;

  if (Array.isArray(item)) {
    return Promise.map(item, function(subItem) {
      return db.insert(subItem);
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
    } else if (blocks <= cache.length && db.blockSize * (cache.length + 1) <= db.maxCacheSize) {
      // There is room, add it to the cache
      itemData.cached = true;
      itemData.cacheIndex = cache.length;
      cache.push(item);
    }
    // Write the new entry to disk
    var buffer = new Buffer(db.blockSize);
    blankEntry(buffer);
    buffer.write(itemString);
    return fsOpen(db.fileName, 'r+')
      .then(function(fd) {
        return fsWrite(fd, buffer, 0, buffer.length, block * db.blockSize)
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

function removeItem(db, id) {
  var itemData = db.items[id];
  if (itemData.cached) {
    var cacheIndex = itemData.cacheIndex;
    db.cacheHoles[cacheIndex] = true;
  }
  var block = itemData.block;
  db.blockHoles[block] = true;
  delete db.items[id];
}

Database.prototype.remove = function(query, options) {
  var db = this;
  var numRemoved = 0;
  options = defaults(options, {
    concurrency: 4,
    limit: Number.MAX_VALUE
  });

  var removePromise;
  if (options.sort === undefined) {
    removePromise = this.iterate(function(item) {
      if (db.matches(item, query)) {
        removeItem(db, item._id);
        numRemoved++;
        if (numRemoved >= options.limit) {
          return false;
        }
      }
      return true;
    });
  } else {
    // If a sort is defined, we can't just remove in order
    removePromise = this.find(query, options)
      .then(function(results) {
        for (var i = 0; i < results.length; i++) {
          removeItem(db, results[i]._id);
          numRemoved++;
        }
      });
  }
  return removePromise.then(function() {
    if (numRemoved > 0) {
      return fillHoles(db, options);
    }
    return Promise.resolve();
  }).then(function() {
    return numRemoved;
  });
};

Database.prototype.update = function(query, update, options) {
  var db = this;
  var numUpdated = 0;
  options = defaults(options, {
    concurrency: 4
  });

  return this.find(query, options)
    .then(function(results) {
      numUpdated = results.length;
      return Promise.map(results, function(item) {
        for (var key in update) {
          if (update.hasOwnProperty(key)) {
            var operator = this.updateOperators[key];
            if (operator) {
              operator.call(db, item, update[key]);
            } else {
              item[key] = update[key];
            }
          }
        }
        return db.insert(item);
      });
    })
    .then(function() {
      return numUpdated;
    });
};
