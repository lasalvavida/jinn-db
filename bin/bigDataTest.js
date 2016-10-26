#!/usr/bin/env node
'use strict';
var Promise = require('bluebird');

var Database = require('../lib/Database');

var db = new Database();
var blocks = 5600000;
var printEvery = 10000;
db.blockSize = 48;
db.load()
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
      return db.insert(item);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    console.timeEnd('Total Insert Time');
    db.close();
    return db.load();
  })
  .then(function() {
    if (db.blocks === blocks) {
      console.log('Test completed successfully');
    } else {
      throw new Error('Db on read had wrong number of blocks');
    }
  })
  .catch(function(err) {
    console.log('Test failed with error: ' + err);
  });
