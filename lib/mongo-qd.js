'use strict';
var MongoDao = require('./mongo-dao'),
  QdClient = require('qd').QdClient,
  MongoBrocast = require('mongo-brocast').Brocast,
  extend = require('xtend'),
  pick = require('shallow-pick'),
  format = require('util').format;

module.exports = MongoQd;

function MongoQd (db, opts) {
  opts = opts || {};

  var dao = opts.dao || new MongoDao(db, opts);
  var client = new QdClient(dao, opts);

  new MongoBrocast(db, extend({brocast: client.brocast}, getBrocastOpts(opts)));

  return client;
}

function getBrocastOpts(opts) {
  return extend(
      {collectionNameGenerator: BrocastCollectionNameGenerator(opts)},
      opts.bc || {},
      pick(opts, ['ns', 'separator'])
    );
}

function BrocastCollectionNameGenerator(opts) {
  var ns = opts.ns || 'qd',
    sep = opts.separator || ':';

  return function (channel) {
    return format('%s%s%s%snotifications', ns, sep, channel.name, sep);
  }
}