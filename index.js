/**
 * Module dependencies
 */

var stream = require('stream'),
    _ = require('lodash'),
    mongodb = require('mongodb')
;

var db;

function getDb(url, options, cb) {
  if (db === undefined) {
    return mongodb.MongoClient.connect(url, options, function (err, _db) {
      if (err) return cb(err);

      db = _db;
      return cb(null, db);
    })
  }
  else {
    return cb(null, db);
  }
}

/**
 * skipper-adapter-gridfs
 *
 * @param  {Object} options
 * @return {Object}
 */

module.exports = function SkipperGridFS(globalOpts) {

  globalOpts = globalOpts || {};

  var adapter = {
    ls: ls,
    read: read,
    rm: rm,
    receive: receive,
    write: write
  };

  return adapter;

  function ls(dirname, cb) {
    var options = globalOpts;

    getDb(options.url, options, function (err, db) {
      if (err) {
        //if (db) db.close();
        return cb(err);
      }

      var bucket = new mongodb.GridFSBucket(db, { bucketName: options.bucket });

      bucket.find({'metadata.dirname': dirname}).toArray(function(err, docs) {
        if (err) {
          //db.close();
          return cb(err);
        }

        return cb(null, docs);
      })
    });
  }

  function read(fd, cb) {
    var options = globalOpts;

    getDb(options.url, options, function(err, db) {
      if (err) {
        //if (db) db.close();
        return cb(err);
      }

      var bucket = new mongodb.GridFSBucket(db, { bucketName: options.bucket });

      var readStream = bucket.openDownloadStreamByName(fd);

      readStream.on('error', function(err) {
        //db.close();
      });

      readStream.on('close', function() {
        //db.close();
      })

      return cb(null, readStream);
    })
  }

  function rm(fd, cb) {
    var options = globalOpts;

    if (!fd) return cb('need id of file');

    getDb(options.url, options, function(err, db) {
      if (err) {
        //if (db) db.close();
        return cb(err);
      }

      var bucket = new mongodb.GridFSBucket(db, { bucketName: options.bucket });

      var ObjectID = mongodb.ObjectID;

      bucket.delete(new ObjectID(fd), function(err) {
        if (err) return cb(err);

        //db.close();

        return cb(null);
      })
    })
  }

  /**
   * A simple receiver for Skipper that writes Upstreams to
   * GridFS.
   *
   * @param  {Object} options
   * @return {Stream.Writable}
   */
  function receive(options) {
    options = options || {};
    options = _.defaults(options, globalOpts);

    // Build an instance of a writable stream in object mode.
    var receiver__ = stream.Writable({
      objectMode: true
    });

    receiver__.once('error', function (err, db) {
      // console.log('ERROR ON RECEIVER__ ::',err);
      //if (db) db.close();
    });

    // This `_write` method is invoked each time a new file is pumped in
    // from the upstream.  `__newFile` is a readable binary stream.
    receiver__._write = function onFile(__newFile, _unused, done) {

      var fd = __newFile.fd;

      __newFile.once('error', function (err) {
        // console.log('ERROR ON file read stream in receiver (%s) ::', __newFile.filename, err);
        // TODO: the upload has been cancelled, so we need to stop writing
        // all buffered bytes, then call gc() to remove the parts of the file that WERE written.
        // (caveat: may not need to actually call gc()-- need to see how this is implemented
        // in the underlying knox-mpu module)
        //
        // Skipper core should gc() for us.
      });

      write(fd, function(err, writeStream) {
        if (err) {
          // console.log(('Receiver: Error writing `' + __newFile.filename + '`:: ' + require('util').inspect(err) + ' :: Cancelling upload and cleaning up already-written bytes...').red);
          receiver__.emit('error', err);
          return;
        }

        // Then ensure necessary parent directories exist
        // ...

        // Create/acquire a writable stream to the remote filesystem
        // (this may involve using options like "bucket", "secret", etc. to build an appropriate request)
        // ...
        var outs__ = writeStream;

        // Pump bytes from the incoming file (`__newFile`)
        // to the outgoing stream towards the remote filesystem (`outs__`)
        __newFile.pipe(outs__);

        // Bind `finish` event for when the file has been completely written.
        outs__.once('finish', function () {
          __newFile.extra = {id: outs__.id.toString()};
          done();
        });

        // Handle potential error from the outgoing stream
        outs__.on('error', function (err) {

          // If this is not a fatal error (e.g. certain types of "ECONNRESET"s w/ S3)
          // don't do anything.
          //if ( ! findOutIfIsFatalSomehow(err) ) return;

          // The handling of this might vary adapter-to-adapter, but at some point, the
          // `done` callback should be triggered w/ a properly formatted error object.
          // Since calling `done(err)` will cause an `error` event to be emitted on the receiver,
          // it is important to format the error in this way (esp. with code===E_WRITE) so it can
          // be detected and discriminated from other types of errors that might be emitted from a
          // receiver.
          receiver__.emit('error', {
            incoming: __newFile,
            outgoing: outs__,
            code: 'E_WRITE',
            stack: typeof err === 'object' ? err.stack : new Error(err),
            name: typeof err === 'object' ? err.name : err,
            message: typeof err === 'object' ? err.message : err
          });
          // If this receiver is being used through Skipper, this event will be intercepted and, if possible
          // (if the adapter implements the `rm` method) any bytes that were already written for this file
          // will be garbage-collected.
        });


      })
    }

    return receiver__;
  }

  function write(fd, cb) {
    var options = globalOpts;

    getDb(options.url, options, function(err, db) {
      if (err) return cb(err);

      var bucket = new mongodb.GridFSBucket(db, { bucketName: options.bucket });

      return cb(null, bucket.openUploadStream(fd));
    })
  }
};
