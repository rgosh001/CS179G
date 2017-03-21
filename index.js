// index.js

console.log('hello from Node.js')

var cassandra = require('cassandra-driver');
var async = require('async');
var assert = require('assert');

// connect to the cluster
var client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    keyspace: 'hospitals'
});



client.execute(function(err){
    assert.ifError(err);
});