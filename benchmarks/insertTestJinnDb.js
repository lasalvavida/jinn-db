#!/usr/bin/env node
'use strict';
var Promise = require('bluebird');
var fs = require('fs-extra');
var json2csv = require('json2csv');
var Database = require('../lib/Database');

var fsOutputFile = Promise.promisify(fs.outputFile);

var data = [];
var startTime = Date.now();

var db = new Database();
var blocks = 100000;

db.load()
  .then(function() {
    var items = [];
    for (var i = 0; i < blocks; i++) {
      items.push({});
    }
    var total = 0;
    return Promise.map(items, function(item) {
      var memoryUsage = process.memoryUsage();
      data.push({
        item: total,
        time: Date.now() - startTime,
        memTotal: memoryUsage.heapTotal,
        memUsed: memoryUsage.heapUsed
      });
      total++;
      return db.insert(item);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    db.close();
    var csv = json2csv({
      data: data,
      fields: ['item', 'time', 'memUsed']
    });
    return fsOutputFile('results/insertTestJinnDb.csv', csv);
  })
  .catch(function(err) {
    console.error('Test failed with error: ' + err)
  });
