var MongoClient = require("mongodb").MongoClient,
    Db = require("mongodb").Db,
    Server = require("mongodb").Server,
    assert = require("assert");

/*
 * This module exposes 2 factory methods:
 * - mongoWrapper.server(params)
 * - mongoWrapper.client(params)
 *
 * - where params is an optional object expecting any number of
 *   the following key-value pairs;
 *   {
 *     hostname: (Type: String) host url of the db server,
 *     port: (Type: Number) integer port number,
 *     dbName: (Type: String) the desired database to use,
 *     user: (Type: String) username if using the client method,
 *     password: (Type: String) password if using the client method
 *   }
 */

/*
 * Both server and client methods follows a factory model by returning
 * either a mongo server (root) connection instance or client connection
 *
 * Both instances contains the following properties:
 * - common pre-wrapped query methods such as:
 *   - find
 *   - updateOne
 *   - deleteOne
 * - wrapFunc a wrapper function to handle db/connection boilerplate
 *   open/close procedures
 * - db (reference to the raw db instance only for server connection)
 * - connect (reference to the connect method only for client connection)
 *   - the above respective db/connect properties can be used directly
 *     to interface with the MongoDB server just as using the
 *     nodejs native mongo driver
 */

/*
 * wrapFunc is a function which takes 2 parameters as such:
 * - function wrapFunc(method, callback)
 *
 * --> method is the first function to be executed
 *     when db conneciton is open with parameters:
 *     - function method(db, cb)
 *     --> where db is a reference to the opened db instance
 *     --> cb is the callback to call after method is done
 *         with the opened db instance taking the following parameters:
 *         - function cb(err, results)
 *         --> pass in the error if encountered during the db query
 *         --> else call cb(null, results) passing in any result
 *             to be passed down to the final callback
 *
 * --> callback is the final function to be called and should expect
 *     the following parameters:
 *     - function callback(err, results)
 *     --> where err is any error encountered during this entire process,
 *         this callback is immediately invoked with a non-null error if
 *         any error is encountered
 *     --> if all successful, callback will be called with err === null and
 *         any results passed in from the method
 */

var db, url;
module.exports.server = function(params) {
  params = params || {};
  var hostname = params.hostname || "localhost",
      port = params.port || 27017,
      dbName = params.dbName || "my_default_db";

  db = new Db(dbName, new Server(hostname, port));
  return {
    wrapFunc: wrapperServer,
    find: findWrap(wrapperServer),
    updateOne: updateOneWrap(wrapperServer),
    deleteOne: deleteOneWrap(wrapperServer),
    db: db
  }
};

module.exports.client = function(params) {
  params = params || {};
  var hostname = params.hostname || "ds023088.mlab.com",
      port = params.port || 23088,
      user = params.user || "my_mongo_admin",
      password = params.password || "mymongopass",
      uri = user + ":" + password + "@" + hostname,
      dbName = params.dbName || "my_default_db";

  url = "mongodb://" + uri + ":" + port + "/" + dbName;
  return {
    wrapFunc: wrapperClient,
    find: findWrap(wrapperClient),
    updateOne: updateOneWrap(wrapperClient),
    deleteOne: deleteOneWrap(wrapperClient),
    connect: MongoClient.connect
  }
}

function findWrap(wrapFunc) {
  return function find(coll, doc, options, callback) {
    wrapFunc(function(db, cb) {
      db.collection(coll).find(doc, options).toArray(function(err, docs) {
        if (err)
          return cb(err);

        cb(null, docs);
      });
    }, callback);
  };
}

function updateOneWrap(wrapFunc) {
  return function updateOne(coll, key, doc, callback) {
    var filter = {};
    filter[key] = doc[key] || doc["$set"][key];
    wrapFunc(function(db, cb) {
      db.collection(coll).updateOne(filter, doc, {upsert: true},
        function(err, results) {
          if (err)
            return cb(err);

          cb(null, results);
      });
    }, callback);
  };
}

function deleteOneWrap(wrapFunc) {
  return function deleteOne(coll, filter, callback) {
    wrapFunc(function(db, cb) {
      db.collection(coll).deleteOne(filter, function(err, results) {
        if (err)
          return cb(err);

        cb(null, results);
      });
    }, callback);
  };
}


function wrapperClient(method, callback) {
  callback = (typeof callback === "function")? callback : null;
  MongoClient.connect(url, function(err, db) {
    if (err) {
      if (callback)
        return callback(err);

      return;
    }

    method(db, function(err, results) {
      db.close();
      if (err) {
        if (callback)
          return callback(err);

        return;
      }

      if (callback)
        callback(null, results);
     });
  })
}

function wrapperServer(method, callback) {
  callback = (typeof callback === "function")? callback : null;
  db.open(function(err, db) {
    if (err) {
      if (callback)
        return callback(err);

      return;
    }

    method(db, function(err, results) {
      db.close();
      if (err) {
        if (callback)
          return callback(err);

        return;
      }

      if (callback)
        callback(null, results);
    });
  });
}
