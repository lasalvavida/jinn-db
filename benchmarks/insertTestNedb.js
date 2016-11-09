#!/usr/bin/env node
'use strict';
var Datastore = require('nedb');
var Promise = require('bluebird');
var fs = require('fs-extra');
var json2csv = require('json2csv');
var tmp = require('tmp');

Datastore.prototype.loadDatabaseAsync = Promise.promisify(Datastore.prototype.loadDatabase);
Datastore.prototype.insertAsync = Promise.promisify(Datastore.prototype.insert);
var fsOutputFile = Promise.promisify(fs.outputFile);
var tmpName = Promise.promisify(tmp.tmpName);

var data = [];
var startTime = Date.now();

var blocks = 100000;
var db;
tmpName()
  .then(function(path) {
    db = new Datastore({filename: path});
    return db.loadDatabaseAsync();
  })
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
      return db.insertAsync(item);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    var csv = json2csv({
      data: data,
      fields: ['item', 'time', 'memTotal', 'memUsed']
    });
    return fs.writeFile('results/insertTestNedb.csv', csv);
  })
  .catch(function(err) {
    console.error('Test failed with error: ' + err)
  });
