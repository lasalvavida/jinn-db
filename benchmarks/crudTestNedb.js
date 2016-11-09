#!/usr/bin/env node
'use strict';
var Datastore = require('nedb');
var Promise = require('bluebird');
var fs = require('fs-extra');
var json2csv = require('json2csv');
var tmp = require('tmp');
var random = require('./random');

Datastore.prototype.loadDatabaseAsync = Promise.promisify(Datastore.prototype.loadDatabase);
Datastore.prototype.insertAsync = Promise.promisify(Datastore.prototype.insert);
Datastore.prototype.findAsync = Promise.promisify(Datastore.prototype.find);
Datastore.prototype.removeAsync = Promise.promisify(Datastore.prototype.remove);
Datastore.prototype.updateAsync = Promise.promisify(Datastore.prototype.update);
var fsOutputFile = Promise.promisify(fs.outputFile);
var tmpName = Promise.promisify(tmp.tmpName);

function randomBetween(a, b) {
  return Math.floor(random() * (b-a)) + a;
}

var operations = [
  'insert',
  'find',
  'update',
  'remove'
];
var groups = 'abcdefghijklmnopqrstuvwxyz';

var data = [];
var startTime = Date.now();

var blocks = 80000;
var db;
tmpName()
  .then(function(path) {
    db = new Datastore({filename: path});
    return db.loadDatabaseAsync();
  })
  .then(function() {
    var items = [];
    for (var i = 0; i < blocks; i++) {
      var operation = operations[randomBetween(0, operations.length)];
      var element = {
        operation: operation,
        item: {
          group: groups.charAt(randomBetween(0, groups.length))
        }
      };
      if (operation === 'update') {
        element.update = {
          group: groups.charAt(randomBetween(0, groups.length))
        }
      }
      items.push(element);
    }
    var total = 0;
    return Promise.map(items, function(element) {
      var memoryUsage = process.memoryUsage();
      data.push({
        item: total,
        time: Date.now() - startTime,
        memTotal: memoryUsage.heapTotal,
        memUsed: memoryUsage.heapUsed
      });
      total++;
      if (element.update) {
        return db[element.operation + 'Async'](element.item, element.update);
      } else {
        return db[element.operation + 'Async'](element.item);
      }
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    var csv = json2csv({
      data: data,
      fields: ['item', 'time', 'memTotal', 'memUsed']
    });
    return fsOutputFile('results/crudTestNedb.csv', csv);
  })
  .catch(function(err) {
    console.error('Test failed with error: ' + err);
    console.error(err.stack);
  });
