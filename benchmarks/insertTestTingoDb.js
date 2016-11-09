#!/usr/bin/env node
'use strict';
var Engine = require('tingodb')();
var Promise = require('bluebird');
var fs = require('fs-extra');
var json2csv = require('json2csv');
var tmp = require('tmp');

var fsOutputFile = Promise.promisify(fs.outputFile);
var tmpDir = Promise.promisify(tmp.dir);

var data = [];
var startTime = Date.now();

var blocks = 100000;
var db;
var collection;
tmpDir()
  .then(function(path) {
    db = new Engine.Db(path, {});
    collection = db.collection('test');
    collection.insertAsync = Promise.promisify(collection.insert);
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
      return collection.insertAsync(item);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    var csv = json2csv({
      data: data,
      fields: ['item', 'time', 'memTotal', 'memUsed']
    });
    return fs.writeFile('results/insertTestTingoDb.csv', csv);
  })
  .catch(function(err) {
    console.error('Test failed with error: ' + err)
  });
