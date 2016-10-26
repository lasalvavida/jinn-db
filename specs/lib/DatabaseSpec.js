'use strict';
var Database = require('../../lib/Database');
var Promise = require('bluebird');
var fs = require('fs-extra');

var fsCopy = Promise.promisify(fs.copy);
var fsRemove = Promise.promisify(fs.remove);

var bigDataTemp = 'specs/data/.tmp/big.db';
var fruitDb = 'specs/data/fruit.db';
var fruitDbTemp = 'specs/data/.tmp/fruit.db';
var helloWorldDb = 'specs/data/helloWorld.db';
var mismatchedBlockSizesDb = 'specs/data/mismatchedBlockSizes.db';
var tmpDir = 'specs/data/.tmp';

var expectFruits = [
  'apple',
  'coconut',
  'banana',
  'grape',
  'orange',
  'pear',
  'pineapple',
  'strawberries'
];

describe('Database', function() {
  describe('load', function() {
    it('loads a database from a file', function(done) {
      var database = new Database(helloWorldDb);
      database.load()
        .then(function() {
          expect(database.cache.length).toBe(2);
          var hello = database.items["0"];
          expect(hello).toBeDefined();
          expect(hello.cached).toBe(true);
          var cacheIndex = hello.cacheIndex;
          expect(database.cache[cacheIndex].data).toBe("Hello");
          var world = database.items["1"];
          expect(world).toBeDefined();
          expect(world.cached).toBe(true);
          cacheIndex = world.cacheIndex;
          expect(database.cache[cacheIndex].data).toBe("World");
          done();
        });
    });

    it('throws when loading a database with mismatched block sizes', function(done) {
      var database = new Database(mismatchedBlockSizesDb);
      database.load()
        .catch(function(err) {
          expect(err).toBeDefined();
          done();
        });
    });

    it('blocks are loaded into memory until the cache is full', function(done) {
      var database = new Database(fruitDb);
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          expect(database.cache.length).toBe(4);
          expect(database.blocks).toBe(8);
          done();
        });
    });
  });

  describe('iterate', function() {
    it('iterates over a loaded database', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      database.load()
        .then(function() {
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function() {
          expect(fruits).toEqual(expectFruits);
          done();
        });
    });

    it('quits early if handler returns false', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      var count = 0;
      database.load()
        .then(function() {
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            count++;
            if (count > 3) {
              return false;
            }
            return true;
          });
        })
        .then(function(completed) {
          expect(completed).toBe(false);
          expect(fruits).toEqual(expectFruits.slice(0, 4));
          done();
        });
    });

    it('falls back on iterateOutOfCore when the db exceeds the maxCacheSize', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          expect(database.cache.length).toBe(4);
          expect(database.blocks).toBe(8);
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function() {
          expect(fruits).toEqual(expectFruits);
          done();
        });
    });
  });

  describe('iterateOutOfCore', function() {
    it('iterates over a database on disk', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      var count = 0;
      database.load()
        .then(function() {
          return database.iterateOutOfCore(0, function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function(completed) {
          expect(completed).toBe(true);
          expect(fruits).toEqual(expectFruits);
          done();
        });
    });

    it('iterates over a database on disk with a starting block', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      var count = 0;
      database.load()
        .then(function() {
          return database.iterateOutOfCore(4, function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function(completed) {
          expect(completed).toBe(true);
          expect(fruits[0]).not.toBeDefined();
          expect(fruits[1]).not.toBeDefined();
          expect(fruits[2]).not.toBeDefined();
          expect(fruits[3]).not.toBeDefined();
          expect(fruits.slice(4, 8)).toEqual(expectFruits.slice(4, 8));
          done();
        });
    });

    it('quits early if handler returns false', function(done) {
      var database = new Database(fruitDb);
      var fruits = [];
      var count = 0;
      database.load()
        .then(function() {
          return database.iterateOutOfCore(0, function(item) {
            fruits[parseInt(item._id)] = item.name;
            count++;
            if (count > 3) {
              return false;
            }
            return true;
          });
        })
        .then(function(completed) {
          expect(completed).toBe(false);
          expect(fruits).toEqual(expectFruits.slice(0, 4));
          done();
        });
    });
  });

  describe('find', function() {
    var db;
    beforeAll(function(done) {
      db = new Database(fruitDb);
      db.load().then(done);
    });

    it('finds matching items in a database', function(done) {
      db.find({color: 'red'})
        .then(function(results) {
          expect(results.length).toBe(2);
          var names = [];
          for (var i = 0; i < results.length; i++) {
            names.push(results[i].name);
          }
          names.sort();
          expect(names).toEqual(['apple', 'strawberries']);
          done();
        });
    });

    describe('supports built-in operator functions', function() {
      it('$lt', function(done) {
        db.find({_id: {$lt: 3}})
          .then(function(results) {
            expect(results.length).toBe(3);
            expect(results[0].name).toBe('apple');
            expect(results[1].name).toBe('coconut');
            expect(results[2].name).toBe('banana');
            done();
          });
      });

      it('$lte', function(done) {
        db.find({_id: {$lte: 3}})
          .then(function(results) {
            expect(results.length).toBe(4);
            expect(results[0].name).toBe('apple');
            expect(results[1].name).toBe('coconut');
            expect(results[2].name).toBe('banana');
            expect(results[3].name).toBe('grape');
            done();
          });
      });

      it('$gt', function(done) {
        db.find({_id: {$gt: 6}})
          .then(function(results) {
            expect(results.length).toBe(1);
            expect(results[0].name).toBe('strawberries');
            done();
          });
      });

      it('$gte', function(done) {
        db.find({_id: {$gte: 6}})
          .then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].name).toBe('pineapple');
            expect(results[1].name).toBe('strawberries');
            done();
          });
      });

      it('$in', function(done) {
        db.find({color: {$in: ['red', 'yellow']}})
          .then(function(results) {
            expect(results.length).toBe(4);
            expect(results[0].name).toBe('apple');
            expect(results[1].name).toBe('banana');
            expect(results[2].name).toBe('pineapple');
            expect(results[3].name).toBe('strawberries');
            done();
          });
      });

      it('$ne', function(done) {
        db.find({name: {$ne: 'grape'}})
          .then(function(results) {
            expect(results.length).toBe(7);
            for (var i = 0; i < results.length; i++) {
              expect(results[0].name).not.toBe('grape');
            }
            done();
          });
      });

      it('$nin', function(done) {
        db.find({color: {$nin: ['red', 'yellow', 'purple', 'orange', 'green']}})
          .then(function(results) {
            expect(results.length).toBe(1);
            expect(results[0].name).toBe('coconut');
            done();
          });
      });

      it('$exists', function(done) {
        db.find({color: {$exists: true}})
          .then(function(results) {
            expect(results.length).toBe(0);
            return db.find({color: {$exists: false}});
          })
          .then(function(results) {
            expect(results.length).toBe(8);
            done();
          });
      });

      it('$regex', function(done) {
        db.find({name: {$regex: /p/}, _id: {$lt: 4}})
          .then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].name).toBe('apple');
            expect(results[1].name).toBe('grape');
            done();
          });
      });
    });
  });

  describe('findOne', function() {
    it('finds the first match in the database', function(done) {
      var database = new Database(fruitDb);
      database.load()
        .then(function() {
          return database.findOne({color: 'red'})
            .then(function(result) {
              expect(result.name).toBe('apple');
              done();
            });
        });
    });
  });

  describe('resize', function() {
    it('can change disk block size of in-memory database', function(done) {
      var fruits = [];
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          return new Database(fruitDbTemp).load();
        })
        .then(function(database) {
          return database.resize(64);
        })
        .then(function() {
          return new Database(fruitDbTemp).load();
        })
        .then(function(database) {
          expect(database.blockSize).toBe(64);
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function() {
          expect(fruits).toEqual(expectFruits);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('can increase block size of a partially on-disk database', function(done) {
      var fruits = [];
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          var database = new Database(fruitDbTemp);
          database.maxCacheSize = 200;
          return database.load();
        })
        .then(function(database) {
          return database.resize(64);
        })
        .then(function() {
          var database = new Database(fruitDbTemp);
          return database.load();
        })
        .then(function(database) {
          expect(database.blockSize).toBe(64);
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function() {
          expect(fruits).toEqual(expectFruits);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('can decrease block size of a partially on-disk database', function(done) {
      var fruits = [];
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          var database = new Database(fruitDbTemp);
          database.maxCacheSize = 200;
          return database.load();
        })
        .then(function(database) {
          return database.resize(48);
        })
        .then(function() {
          var database = new Database(fruitDbTemp);
          return database.load();
        })
        .then(function(database) {
          expect(database.blockSize).toBe(48);
          return database.iterate(function(item) {
            fruits[parseInt(item._id)] = item.name;
            return true;
          });
        })
        .then(function() {
          expect(fruits).toEqual(expectFruits);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });
  });

  describe('delete', function() {
    it('removes an item from a database', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          return new Database(fruitDbTemp).load();
        })
        .then(function(database) {
          db = database;
          return db.delete({name: 'grape'});
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(1);
          var cache = db.cache;
          expect(cache.length).toBe(7);
          return db.find({name: 'grape'});
        })
        .then(function(results) {
          expect(results.length).toBe(0);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('removes multiple items from a database', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          return new Database(fruitDbTemp).load();
        })
        .then(function(database) {
          db = database;
          return db.delete({color: 'red'});
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(2);
          return db.find({color: 'red'});
        })
        .then(function(results) {
          expect(results.length).toBe(0);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('removes an array of queries, cached and on-disk', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          db = new Database(fruitDbTemp);
          db.maxCacheSize = 200;
          return db.load();
        })
        .then(function() {
          return db.delete([
            {name: 'strawberries'},
            {name: 'coconut'}
          ]);
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(2);
          expect(db.cache.length).toBe(4);
          return db.find({name: 'grape'});
        })
        .then(function(results) {
          expect(results.length).toBe(1);
          expect(results[0].color).toBe('purple');
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });
  });

  describe('insert', function() {
    it('adds a new item to the database', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          db = new Database(fruitDbTemp);
          return db.load();
        })
        .then(function() {
          return db.insert({_id:"8", color:'green', name:'honeydew'});
        })
        .then(function() {
          return db.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('adds a new item to the database, resizing it', function(done) {
      var db;
      var blockSize;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          db = new Database(fruitDbTemp);
          db.maxCacheSize = 200;
          return db.load();
        })
        .then(function() {
          blockSize = db.blockSize;
          return db.insert({color:'green', name:'honeydew'});
        })
        .then(function() {
          return db.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          expect(db.blockSize).not.toBe(blockSize);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('overwrites an existing item in the database', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          db = new Database(fruitDbTemp);
          return db.load();
        })
        .then(function() {
          return db.insert({_id:'5', color:'green', name:'honeydew'});
        })
        .then(function() {
          return db.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(1);
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });

    it('adds multiple items to the database', function(done) {
      var db;
      fsCopy(fruitDb, fruitDbTemp)
        .then(function() {
          db = new Database(fruitDbTemp);
          return db.load();
        })
        .then(function() {
          return db.insert([
            {_id:"8", color:'green', name:'honeydew'},
            {color:'orange', name:'cantalope'},
            {_id:"0", color:'black', name:'blackberry'}
          ]);
        })
        .then(function() {
          return db.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          return db.findOne({name: 'blackberry'})
        })
        .then(function(item) {
          expect(item).toBeDefined();
          expect(item.color).toBe('black');
          return fsRemove(tmpDir);
        })
        .then(function() {
          done();
        });
    });
  });
});
