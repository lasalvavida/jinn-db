'use strict';
var Database = require('../../lib/Database');

var fruitDb = 'specs/data/fruit.db';
var helloWorldDb = 'specs/data/helloWorld.db';

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

    it('creates a database that is a copy of another', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.find({color: 'red'})
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          expect(results[0].name).toBe('apple');
          expect(results[1].name).toBe('strawberries');
          done();
        });
    });

    it('blocks are loaded into memory until the cache is full', function(done) {
      var database = new Database(fruitDb);
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          expect(database.cache.length).toBe(3);
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
          expect(database.cache.length).toBe(3);
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

    it('finds the first match in the database', function(done) {
      var database = new Database(fruitDb);
      database.load()
        .then(function() {
          return database.find({color: 'red'}, {limit: 1})
            .then(function(results) {
              expect(results.length).toBe(1);
              expect(results[0].name).toBe('apple');
              done();
            });
        });
    });

    it('finds the first match with a sort', function(done) {
      var database = new Database(fruitDb);
      database.load()
        .then(function() {
          return database.find({color: 'red'}, {
            limit: 1,
            sort: function(a, b) {
              return a.name < b.name;
            }
          }).then(function(results) {
              expect(results.length).toBe(1);
              expect(results[0].name).toBe('strawberries');
              done();
            });
        });
    });

    it('finds with a projection', function(done) {
      var database = new Database(fruitDb);
      database.load()
        .then(function() {
          return database.find({color: 'red'}, {
            projections: {
              name: true
            }
          }).then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].name).toBe('apple');
            expect(results[0]._id).toBeDefined();
            expect(results[0].color).not.toBeDefined();
            expect(results[1].name).toBe('strawberries');
            expect(results[1]._id).toBeDefined();
            expect(results[1].color).not.toBeDefined();
            done();
          });
        });
    });

    it('finds with a projection, ommitting _id', function(done) {
      var database = new Database(fruitDb);
      database.load()
        .then(function() {
          return database.find({color: 'red'}, {
            projections: {
              _id: false,
              name: true
            }
          }).then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].name).toBe('apple');
            expect(results[0]._id).not.toBeDefined();
            expect(results[0].color).not.toBeDefined();
            expect(results[1].name).toBe('strawberries');
            expect(results[1]._id).not.toBeDefined();
            expect(results[1].color).not.toBeDefined();
            done();
          });
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

      it('$or', function(done) {
        db.find({$or: [{color: 'red'}, {color: 'yellow'}]})
          .then(function(results) {
            expect(results.length).toBe(4);
            expect(results[0].name).toBe('apple');
            expect(results[1].name).toBe('banana');
            expect(results[2].name).toBe('pineapple');
            expect(results[3].name).toBe('strawberries');
            done();
          });
      });

      it('$and', function(done) {
        db.find({$and: [{color: 'red'}, {color: 'yellow'}]})
          .then(function(results) {
            expect(results.length).toBe(0);
            done();
          });
      });

      it('$not', function(done) {
        db.find({$not: {$or: [
          {color: 'red'},
          {color: 'yellow'},
          {color: 'orange'},
          {color: 'green'}
        ]}}).then(function(results) {
          expect(results.length).toBe(2);
          expect(results[0].name).toBe('coconut');
          expect(results[1].name).toBe('grape');
          done();
        });
      });
    });
  });

  describe('resize', function() {
    it('can change disk block size of in-memory database', function(done) {
      var fruits = [];
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.resize(64);
        })
        .then(function() {
          database = new Database(database.fileName);
          return database.load();
        })
        .then(function() {
          expect(database.blockSize).toBe(64);
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

    it('can increase block size of a partially on-disk database', function(done) {
      var fruits = [];
      var database = new Database({
        copyOf: fruitDb
      });
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          return database.resize(64);
        })
        .then(function() {
          return database.close();
        })
        .then(function() {
          database = new Database(database.fileName);
          return database.load();
        })
        .then(function() {
          expect(database.blockSize).toBe(64);
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

    it('can decrease block size of a partially on-disk database', function(done) {
      var fruits = [];
      var database = new Database({
        copyOf: fruitDb
      });
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          return database.resize(48);
        })
        .then(function() {
          return database.close();
        })
        .then(function() {
          database = new Database(database.fileName);
          return database.load();
        })
        .then(function() {
          expect(database.blockSize).toBe(48);
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

  describe('remove', function() {
    it('removes an item from a database', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.remove({name: 'grape'});
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(1);
          var cache = database.cache;
          expect(cache.length).toBe(7);
          return database.find({name: 'grape'});
        })
        .then(function(results) {
          expect(results.length).toBe(0);
          done();
        });
    });

    it('removes multiple items from a database', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.remove({color: 'red'});
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(2);
          return database.find({color: 'red'});
        })
        .then(function(results) {
          expect(results.length).toBe(0);
          done();
        });
    });

    it('removes an array of queries, cached and on-disk', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          return database.remove({$or: [
            {name: 'strawberries'},
            {name: 'coconut'}
          ]});
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(2);
          expect(database.cache.length).toBe(3);
          return database.find({name: 'grape'});
        })
        .then(function(results) {
          expect(results.length).toBe(1);
          expect(results[0].color).toBe('purple');
          done();
        });
    });

    it('removes a sorted and limited set of queries', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.remove({color: 'red'}, {
            limit: 1,
            sort: function(a, b) {
              return a.name < b.name;
            }
          });
        })
        .then(function(numRemoved) {
          expect(numRemoved).toBe(1);
          expect(database.cache.length).toBe(7);
          return database.find({name: 'strawberries'});
        })
        .then(function(results) {
          expect(results.length).toBe(0);
          done();
        });
    });
  });

  describe('insert', function() {
    it('adds a new item to the database', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.insert({_id:"8", color:'green', name:'honeydew'});
        })
        .then(function() {
          return database.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          done();
        });
    });

    it('adds a new item to the database, resizing it', function(done) {
      var blockSize;
      var database = new Database({
        copyOf: fruitDb
      });
      database.maxCacheSize = 200;
      database.load()
        .then(function() {
          blockSize = database.blockSize;
          return database.insert({color:'green', name:'honeydew'});
        })
        .then(function() {
          return database.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          expect(database.blockSize).not.toBe(blockSize);
          done();
        });
    });

    it('overwrites an existing item in the database', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.insert({_id:'5', color:'green', name:'honeydew'});
        })
        .then(function() {
          return database.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(1);
          done();
        });
    });

    it('adds multiple items to the database', function(done) {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.insert([
            {_id:"8", color:'green', name:'honeydew'},
            {color:'orange', name:'cantalope'},
            {_id:"0", color:'black', name:'blackberry'}
          ]);
        })
        .then(function() {
          return database.find({color:'green'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          return database.find({name: 'blackberry'})
        })
        .then(function(results) {
          expect(results.length).toBe(1);
          var item = results[0];
          expect(item).toBeDefined();
          expect(item.color).toBe('black');
          done();
        });
    });
  });

  describe('update', function() {
    it('updates a existing entries', function() {
      var database = new Database({
        copyOf: fruitDb
      });
      database.load()
        .then(function() {
          return database.update({color: 'red'}, {color: 'maroon'});
        })
        .then(function(numUpdated) {
          expect(numUpdated).toBe(2);
          return database.find({color:'maroon'});
        })
        .then(function(results) {
          expect(results.length).toBe(2);
          expect(results[0].name).toBe('apple');
          expect(results[0].color).toBe('maroon');
          expect(results[1].name).toBe('strawberries');
          expect(results[1].color).toBe('maroon');
          done();
        });
    });

    describe('supports built-in operator functions', function() {
      it('$set', function() {
        var database = new Database({
          copyOf: fruitDb
        });
        database.load()
          .then(function() {
            return database.update({color: 'red'}, {$set: {isRed: true, isNotRed: false}});
          })
          .then(function() {
            return database.find({isRed: true})
          })
          .then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].isNotRed).toBe(false);
            expect(results[1].isNotRed).toBe(false);
          });
      });

      it('$unset', function() {
        var database = new Database({
          copyOf: fruitDb
        });
        database.load()
          .then(function() {
            return database.update({color: 'orange'}, {$unset: {name: true}})
          })
          .then(function() {
            return database.find({color: 'orange'})
          })
          .then(function(results) {
            expect(results.length).toBe(1);
            expect(results[0].name).not.toBeDefined();
          })
      });

      it('$inc', function() {
        var database = new Database({
          copyOf: fruitDb
        });
        database.load()
          .then(function() {
            return database.insert({name: 'counter', value: '1'});
          })
          .then(function() {
            return database.update({name: 'counter'}, {$inc: {value: -1}});
          })
          .then(function() {
            return database.find({name: 'counter'});
          })
          .then(function(results) {
            expect(results.length).toBe(1);
            expect(results[0].value).toBe(0);
          })
      });

      it('$min', function() {
        var database = new Database({
          copyOf: fruitDb
        });
        database.load()
          .then(function() {
            return database.insert({name: 'minTest', value: 2}, {name: 'minTest', value: 5});
          })
          .then(function() {
            return database.update({name: 'minTest'}, {$min: {value: 3}});
          })
          .then(function() {
            return database.find({name: 'minTest'})
          })
          .then(function(results) {
            expect(results.length).toBe(2);
            expect(results[0].value).toBe(2);
            expect(results[1].value).toBe(3);
          });
      });
    });
  });
});
