var MongoClient = require("mongodb").MongoClient,
    Db = require("mongodb").Db,
    Server = require("mongodb").Server,
    assert = require("assert");

var db, url;
module.exports.server = function(params) {
  params = params || {};
  var hostname = params.hostname || "localhost",
      port = params.port || 27017,
      dbName = params.dbName || "my_default_db";

  db = new Db(dbName, new Server(params.hostname, params.port));
  return {
    updateOne: updateOnewrap(wrapperServer),
    deleteOne: deleteOneWrap(wrapperServer),
    db: db
  }
};

module.exports.client = function(params) {
  params = params || {};
  var user = params.user || "my_mongo_admin",
      password = params.password || "mymongopass"
      port = params.port || 23088,
      uri = user + ":" + password + "@ds023088.mlab.com",
      dbName = params.dbName || "my_default_db";

  url = "mongodb://" + uri + ":" + port + "/" + dbName;
  return {
    updateOne: updateOneWrap(wrapperClient),
    deleteOne: deleteOneWrap(wrapperClient),
    connect: MongoClient.connect
  }
}

function updateOneWrap(wrapFunc) {
  return function updateOne(coll, doc, callback) {
    var id = doc.id || doc["$set"].id;
    wrapFunc(function(db, cb) {
      db.collection(coll).updateOne({id: id}, doc, {update: true},
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
      db.collection(coll).deleteOne(filter, {update: true},
        function(err, results) {
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
