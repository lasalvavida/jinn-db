#!/usr/bin/env node
'use strict';
var Datastore = require('nedb');
var Promise = require('bluebird');
var tmp = require('tmp');

var tmpName = Promise.promisify(tmp.tmpName);
Datastore.prototype.loadDatabaseAsync = Promise.promisify(Datastore.prototype.loadDatabase);
Datastore.prototype.insertAsync = Promise.promisify(Datastore.prototype.insert);

var db;
var fileName;
var blocks = 5600000;
var printEvery = 10000;
tmpName()
  .then(function(path) {
    fileName = path;
    db = new Datastore({filename: path});
    return db.loadDatabaseAsync();
  })
  .then(function() {
    var items = [];
    for (var i = 0; i < blocks; i++) {
      items.push({});
    }
    var total = 0;
    console.time('Total Insert Time');
    return Promise.map(items, function(item) {
      total++;
      if (total % printEvery === 0) {
        console.log('Inserted ' + total + '/' + blocks);
      }
      return db.insertAsync(item);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    console.timeEnd('Total Insert Time');
    var db = new Datastore({filename: fileName});
    return db.loadDatabaseAsync();
  })
  .then(function() {
    console.log('Test completed successfully');
  })
  .catch(function(err) {
    console.log('Test failed with error: ' + err);
  });
