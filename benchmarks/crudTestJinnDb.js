#!/usr/bin/env node
'use strict';
var Promise = require('bluebird');
var fs = require('fs-extra');
var json2csv = require('json2csv');
var Database = require('../lib/Database');
var random = require('./random');

var fsOutputFile = Promise.promisify(fs.outputFile);

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

var db = new Database();
var blocks = 80000;

db.load()
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
      return db[element.operation](element.item, element.update);
    }, {
      concurrency: 1
    });
  })
  .then(function() {
    db.close();
    var csv = json2csv({
      data: data,
      fields: ['item', 'time', 'memTotal', 'memUsed']
    });
    return fsOutputFile('results/crudTestJinnDb.csv', csv);
  })
  .catch(function(err) {
    console.error('Test failed with error: ' + err);
    console.error(err.stack);
  });
