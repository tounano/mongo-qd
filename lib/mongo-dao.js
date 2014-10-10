'use strict';
var PulledJob = require('qd').PulledJob,
  format = require('util').format,
  extend = require('xtend');

module.exports = MongoDao;

function MongoDao(db, opts) {
  this.opts = opts = opts || {};
  this.db = db;
  this._pulledJobFactory = opts.pulledJobFactory || defaultPulledJobFactory(opts);
  this._generateCollectionName = opts.collectionNameGenerator || defaultCollectionNameGenerator(opts);
}

MongoDao.prototype._getCollectionForQueue = function (queue, cb) {
  var self = this;
  this.db.collection(this._generateCollectionName(queue), function (err, col) {
    if (err) return cb(err);

    self._ensureQueueCollectionIndexes(col, cb);
  });
}

MongoDao.prototype._ensureQueueCollectionIndexes = function (col, cb) {
  col.ensureIndex({'_todo.n': 1, '_todo.ts':1, '_todo.p':1}, {sparse: true}, function(err) {
    if (err) return cb(err);
    cb(null, col);
  })
}

MongoDao.prototype.pullJobFromQueue = function (queue, jobName, cb) {
  var self = this;
  this._getCollectionForQueue(queue, function (err, col) {
    if (err) return cb(err);

    col.findAndModify({'_todo.n': jobName, '_todo.ts': {$lte: new Date().getTime()}},
      {'_todo.p': 1, '_todo.ts': 1},
      {$set: {status: 'running'}, $unset: {_todo: ""}},
      function (err, job) {
        if (err) return cb(err);

        if (!job) return cb(null, null);

        var id = job._id;
        delete job._id;
        delete job._todo;

        var pulledJob = self._pulledJobFactory(id, job);
        pulledJob.queue = queue;

        cb(null, pulledJob);
      })
  })
}

MongoDao.prototype.completePulledJob = function (pulledJob, result, cb) {
  this._getCollectionForQueue(pulledJob.queue, function (err, col) {
    if (err) return cb(err);

    col.update({_id: pulledJob.id}, {$set: {status: 'complete', result: result}}, cb);
  })
}

MongoDao.prototype.failPulledJob = function (pulledJob, reason, cb) {
  this._getCollectionForQueue(pulledJob.queue, function (err, col) {
    if (err) return cb(err);

    col.update({_id: pulledJob.id}, {$set: {status: 'failed', error: reason}}, cb);
  })
}

MongoDao.prototype.progressPulledJob = function (pulledJob, progress, total, cb) {
  this._getCollectionForQueue(pulledJob.queue, function (err, col) {
    if (err) return cb(err);

    col.update({_id: pulledJob.id}, {$set: {progress: [progress, total]}}, cb);
  })
}

MongoDao.prototype.addFailedAttemptToPulledJob = function (pulledJob, restartAt, cb) {
  this._getCollectionForQueue(pulledJob.queue, function (err, col) {
    if (err) return cb(err);

    var _todo = {
      n: pulledJob.state.name,
      p: pulledJob.state.priority,
      ts: restartAt
    }

    col.update({_id: pulledJob.id}, {$set: {status: 'waiting', startAt: restartAt, _todo: _todo}, $inc: {attempts: 1}}, cb);
  })
}

MongoDao.prototype.saveNewJob = function (newJob, cb) {
  this._getCollectionForQueue(newJob.queue, function (err, col) {
    if (err) return cb(err);

    var job = newJob.state;
    job = extend(job, {_todo: {
      n: job.name,
      p: job.priority,
      ts: job.startAt
    }});

    col.insert(job, function (err, job) {
      if (err) return cb(err);

      cb(null, job.shift()._id);
    });
  })
}

function defaultPulledJobFactory(opts) {
  return function (id, state) {
    return new PulledJob(id, state);
  }
}

function defaultCollectionNameGenerator(opts) {
  var ns = opts.ns || 'qd',
    sep = opts.separator || ':';
  return function (queue) {
    return format('%s%s%s', ns, sep, queue.name)
  }
}