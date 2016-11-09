'use strict';
var Promise = require('bluebird');
var clone = require('clone');
var deepEqual = require('deep-equal');
var defaults = require('defaults');
var fs = require('fs-extra');
var smaz = require('smaz');
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

var compress = smaz.compress;
var decompress = smaz.decompress;

module.exports = Database;

Database.MAX_CACHE_SIZE_DEFAULT = 134217728; // 128 MB
Database.HEADER_LENGTH = 22; // bytes

function Database(options) {
  if (typeof options === 'object') {
    options = defaults(options, {
      compressed: true
    });
    this.fileName = options.fileName;
    this.copyOf = options.copyOf;
    this.compressed = options.compressed;
  }
  else {
    this.fileName = options;
  }
  if (this.compressed === undefined) {
    this.compressed = true;
  }
  this.version = 1;
  this.fd = undefined;
  this.headerLength = Database.HEADER_LENGTH;
  this.items = {};

  this.cache = [];
  this.cacheHoles = {};
  this.maxCacheSize = Database.MAX_CACHE_SIZE_DEFAULT;

  this.blocks = 0;
  this.blockSize = 0;
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
          var itemValue = items[i];
          if (this.matches(itemValue, update[key])) {
            items.splice(i, 1);
            i--;
          }
        }
      }
    }
  }
};

function readBufferFromBlock(db, block, buffer, length) {
  if (!buffer) {
    buffer = new Buffer(db.blockSize);
  }
  if (length === undefined) {
    length = buffer.length;
  }
  return fsRead(db.fd, buffer, 0, length, db.headerLength + block * db.blockSize);
}

function readFromBlock(db, block, buffer, length) {
  return readBufferFromBlock(db, block, buffer, length)
    .then(function() {
      var itemString;
      if (db.compressed) {
        itemString = decompress(buffer);
        itemString = itemString.substring(itemString.indexOf('{'), itemString.lastIndexOf('}') + 1);
      } else {
        itemString = buffer.toString();
      }
      return JSON.parse(itemString);
    });
}

function blankEntry(buffer) {
  for (var i = 0; i < buffer.length; i++) {
    buffer.write(' ', i, 'utf8');
  }
}

function writeBufferToBlock(db, block, buffer, blockSize) {
  if (blockSize === undefined) {
    blockSize = db.blockSize;
  }
  return fsWrite(db.fd, buffer, 0, buffer.length, db.headerLength + block * blockSize);
}

function writeToBlock(db, block, item, buffer, blockSize) {
  if (blockSize === undefined) {
    blockSize = db.blockSize;
  }
  if (!buffer) {
    buffer = new Buffer(blockSize);
  }
  blankEntry(buffer);
  var itemString = JSON.stringify(item);
  if (db.compressed) {
    var compressedBuffer = new Buffer(compress(itemString).buffer);
    compressedBuffer.copy(buffer);
  } else {
    buffer.write(itemString);
  }
  return writeBufferToBlock(db, block, buffer, blockSize);
}

function readHeader(db) {
  var header = new Buffer(db.headerLength);
  return fsRead(db.fd, header, 0, header.length, 0)
    .then(function() {
      var magic = header.toString('utf8', 0, 4);
      if (magic !== 'jinn') {
        throw new Error('Invalid magic: ' + magic + ' expecteded \'jinn\'');
      }
      var version = header.readUInt8(4);
      if (version !== 1) {
        throw new Error('Invalid version: ' + version + ' only version 1 is valid');
      }
      var flags = header.readUInt8(5);
      var compressed = false;
      if (flags === 1) {
        compressed = true;
      }
      var blockSize = header.readUInt32LE(6);
      blockSize = blockSize << 16 << 16;
      blockSize += header.readUInt32LE(10);

      var numBlocks = header.readUInt32LE(14);
      numBlocks = numBlocks << 16 << 16;
      numBlocks += header.readUInt32LE(18);

      db.compressed = compressed;
      db.blockSize = blockSize;
      db.blocks = numBlocks;
    });
}

function writeHeader(db) {
  var header = new Buffer(db.headerLength);
  header.write('jinn');
  header.writeUInt8(1, 4);
  header.writeUInt8(db.compressed ? 1 : 0, 5);
  var blockSizeUpper = db.blockSize >> 16 >> 16;
  header.writeUInt32LE(blockSizeUpper, 6);
  var blockSizeLower = db.blockSize & 0xFFFFFFFF;
  header.writeUInt32LE(blockSizeLower, 10);
  var numBlocksUpper = db.blocks >> 16 >> 16;
  header.writeUInt32LE(numBlocksUpper, 14);
  var numBlocksLower = db.blocks & 0xFFFFFFFF;
  header.writeUInt32LE(numBlocksLower, 18);
  return fsWrite(db.fd, header, 0, header.length, 0);
}

Database.prototype.load = function(options) {
  var db = this;
  var initPromise;
  if (!db.fileName) {
    initPromise = tmpName()
      .then(function(path) {
        db.fileName = path;
      });
  } else {
    initPromise = Promise.resolve();
  }
  if (db.copyOf) {
    initPromise = initPromise.then(function() {
      return fsCopy(db.copyOf, db.fileName);
    });
  }
  var buffer;
  return initPromise
    .then(function() {
      return fsEnsureFile(db.fileName);
    })
    .then(function() {
      return fsOpen(db.fileName, 'r+');
    })
    .then(function(fd) {
      db.fd = fd;
      return readHeader(db);
    })
    .then(function() {
      buffer = new Buffer(db.blockSize);
      return db.iterateOutOfCore(0, function(item, block) {
        var itemData = {
          block: block,
          cached: false,
          cacheIndex: -1
        };
        if (db.blockSize * (db.cache.length + 1) <= db.maxCacheSize) {
          itemData.cached = true;
          itemData.cacheIndex = db.cache.length;
          db.cache.push(item);
        }
        db.items[item._id] = itemData;
        return true;
      }, options);
    })
    .catch(function() {
      return writeHeader(db);
    });
};

Database.prototype.close = function() {
  var db = this;
  db.items = {};
  db.cache = [];
  return writeHeader(db)
    .then(function() {
      var fd = db.fd;
      db.fd = undefined;
      return fsClose(fd);
  });
};

Database.prototype.iterateOutOfCore = function(startBlock, handler, options) {
  var db = this;
  var buffers = [];
  var freeBuffers = [];
  var cancelled = false;
  var index = startBlock;
  var iterable = {};
  iterable[Symbol.iterator] = function() {
    return {
      next: function() {
        return {
          done: index >= db.blocks || cancelled,
          value: index++
        };
      }
    };
  };
  return Promise.map(iterable, function(block) {
    var buffer;
    var bufferIndex = freeBuffers.pop();
    if (bufferIndex === undefined) {
      buffer = new Buffer(db.blockSize);
    } else {
      buffer = buffers.splice(bufferIndex, 1);
    }
    return readFromBlock(db, block, buffer)
      .then(function(item) {
        freeBuffers.push(buffers.length);
        buffers.push(buffer);
        if (!cancelled && !handler(item, block)) {
          cancelled = true;
        }
      });
  }, options)
    .then(function() {
      return !cancelled;
    });
};

Database.prototype.iterate = function(handler, options) {
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
    return this.iterateOutOfCore(cache.length, handler, options);
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
  var id = query._id;
  if (id !== undefined && db.items[id]) {
    var itemData = db.items[id];
    var singleItemPromise;
    if (itemData.cached) {
      singleItemPromise = Promise.resolve(db.cache[itemData.cacheIndex]);
    } else {
      singleItemPromise = readFromBlock(db, db.items[id].block);
    }
    return singleItemPromise
      .then(function(result) {
        results.push(result);
        return results;
      });
  }
  return this.iterate(function(item) {
    if (db.matches(item, query)) {
      return addToResults(item, query, results, options);
    }
    return true;
  }).then(function() {
    return results;
  });
};

function nextPowerOfTwo(num) {
  return Math.pow(2,Math.ceil(Math.log(num)/Math.log(2)));
}

Database.prototype.resize = function(blockSize, options) {
  options = defaults(options, {
    concurrency: 1,
  });
  var db = this;
  var cache = this.cache;
  var items = this.items;
  var i;
  var buffer = new Buffer(blockSize);
  if (blockSize !== this.blockSize) {
    var handleFileIO;
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
      handleFileIO = Promise.mapSeries(moveIndices, function(index) {
        return readBufferFromBlock(db, index, buffer, Math.min(db.blockSize, buffer.length))
          .then(function() {
            return writeBufferToBlock(db, index, buffer, blockSize);
          })
          .then(function() {
            blankEntry(buffer);
          });
      }, options)
        .then(function() {
          if (blockSize <= db.blockSize) {
            // The file needs to be truncated to delete hanging characters
            return fsFTruncate(db.fd, db.headerLength + db.blocks * blockSize);
          }
        });
    } else {
      handleFileIO = Promise.resolve();
    }
    return handleFileIO.then(function() {
      // All of the remaining data is in memory, just write it back
      return Promise.map(cache, function(item) {
        var itemData = items[item._id];
        return writeToBlock(db, itemData.block, item, buffer, blockSize);
      }, options);
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
  } else {
    return Promise.resolve(db);
  }
};

Database.prototype.insert = function(item, options) {
  var items = this.items;
  var cache = this.cache;
  var blocks = this.blocks;
  var db = this;

  if (Array.isArray(item)) {
    return Promise.map(item, function(subItem) {
      return db.insert(subItem, options);
    }, {
      concurrency: 1
    });
  }

  var itemData;
  if (item._id) {
    itemData = items[item._id];
  } else {
    item._id = uuid.v1();
  }
  if (!itemData) {
    itemData = {
      block: blocks,
      cached: false,
      cacheIndex: -1,
    };
    db.blocks++;
  }
  var block = itemData.block;
  items[item._id] = itemData;

  var itemLength = 0;
  var itemString = JSON.stringify(item);
  itemLength = itemString.length;
  if (db.compressed) {
    var compressed = compress(itemString);
    itemLength = compressed.length;
  }
  var promise = Promise.resolve();
  if (itemLength > db.blockSize) {
    if (db.blocks > 1) {
      // The new item is larger than the block size, resize the database
      promise = this.resize(nextPowerOfTwo(itemLength), options);
    } else {
      db.blockSize = nextPowerOfTwo(itemLength);
    }
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
    return writeToBlock(db, block, item);
  });
};

function moveBlock(db, fromBlock, toBlock) {
  var buffer = new Buffer(db.blockSize);
  return readFromBlock(db, fromBlock, buffer)
    .then(function(item) {
      var id = item._id;
      var itemData = db.items[id];
      itemData.block = toBlock;
      return writeBufferToBlock(db, toBlock, buffer)
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
  // Move elements from the bottom of the database up to fill the holes
  return Promise.map(blockHoles, function(block, index) {
    var fromBlock = fromBlocks[index];
    if (fromBlock && fromBlock > block) {
      return moveBlock(db, fromBlock, block)
        .then(function(item) {
          var itemData = db.items[item._id];
          if (!itemData.cached && cacheHoles.length > 0) {
            var cacheHole = cacheHoles.shift();
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
      // Truncate any extra data from the bottom of the file
      return fsFTruncate(db.fd, db.headerLength + db.blocks * db.blockSize);
    })
    .then(function() {
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
            itemData.cacheIndex = cacheHole;
            db.cache[cacheHole] = item;
          }
        }
      }
      // Resize the cache
      while (db.cache.length > newCacheSize) {
        db.cache.pop();
      }
      db.cacheHoles = {};
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

  return db.find(query, options)
    .then(function(results) {
      numUpdated = results.length;
      return Promise.map(results, function(item) {
        for (var key in update) {
          if (update.hasOwnProperty(key)) {
            var operator = db.updateOperators[key];
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
