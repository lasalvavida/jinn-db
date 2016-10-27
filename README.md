![jinn-db](images/jinn-db-title.png)

A pure javascript, persistent key-value store for Node.js that supports out-of-core data access.

## About
### What can Jinn do for me?
Jinn allows you to store and retrieve JSON data using NoSQL style queries from
an embedded database.

Jinn is designed to be a lightweight database for local node applications
that need a data storage solution. If you are a server or cloud-based
application with heavy usage, a dedicated server like
[PostgresSQL](https://www.postgresql.org/)
or
[MongoDB](https://www.mongodb.com/)
might be a better choice.

### Is this another in-memory database?
No. I got frustrated with the abundance of in-memory only databases for node,
so I wrote this for another project I was working on. In-memory databases can
only handle as much data as you can fit in memory which is very limiting
for larger datasets.

### How fast is Jinn?
[Pretty fast.](#benchmarks)
Jinn caches as many entries as it can in memory, so for smaller
datasets, it will be as fast as any in-memory database. The difference is that
it won't choke when the dataset is larger than that, and it won't starve the
rest of your application from memory as the database grows.

## Usage
### Get Jinn
```
npm install jinn-db --save
```

### Open/Load a Database
```javascript
var Database = require('jinn-db');
var db = new Database('wishes.db'); // If this file doesn't exist, it will be created
db = new Database(); // If no fileName is provided, the database is created as a temporary file that will be deleted when the process exits.
db.load()
  .then(function(db) {
    // Done loading!
  });
```

For the examples below, our database is composed of the following entries:
```javascript
[{"_id":"0", "person": "ali", "wishNum": 0, "wishedFor": "gold"},
 {"_id":"1", "person": "ali", "wishNum": 1, "wishedFor": "gold"},
 {"_id":"2", "person": "ali", "wishNum": 2, "wishedFor": "more gold"}]
```

### Find entries
```javascript
// Find all of Ali's wishes
db.find({person: 'ali'})
  .then(function(results) {
    /*
     * results -> [{_id: '0', person: 'ali', wishNum: 0, wishedFor: 'gold'},
     *             {_id: '1', person: 'ali', wishNum: 1, wishedFor: 'gold'},
     *             {_id: '2', person: 'ali', wishNUm: 2, wishedFor: 'more gold'}]
     */
  });
```

Credit to @louischatriot and
[NeDB](https://github.com/louischatriot/nedb)
off of which this API is largely based.

## Benchmarks

TODO:
