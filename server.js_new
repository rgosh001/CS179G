// index.js

console.log('hello from Node.js')

const http = require('http');
const httpProxy = require('http-proxy');
const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');

// connect to the cluster
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    protocolOptions: {port:'9043'},
    keyspace: 'hospitals'
});

// http://stackoverflow.com/a/2190927
var nodeConnector = nodeConnector || (function(){
    var _args = {}; // private

    return {
        init : function(Args) {
            _args = Args;
            // some other initialising
        },
        printArgs : function() {
            for(var i = 0; i < _args.length; i++){
                console.log('_args[' + i + ']: ' + _args[i]);
            }
        }
    };
}());

var server = http.createServer(function (req, res) {
    res.writeHead(200, {
        'Content-Type': 'text/plain' ,
        'Access-Control-Allow-Origin': 'http://localhost:8888',
    });
    var zipcode = req.url.substr(1, req.url.length - 1);
    console.log('execQueryOnZipcode: ' + execQueryOnZipcode(zipcode));
    res.write(zipcode);
    res.end();
}).listen(8000);

console.log("listening on port 8000")

server.on('error', function (e) {
    // Handle your error here
    console.log(e);
});


function execQueryOnZipcode(zipcode){
    const query = 'SELECT zipcode, pop, numberofdoctors, ratio FROM pop_doctor_ratio WHERE zipcode = ?';
    var row;
    client.connect()
        .then(function () {
            console.log('zipcode: ' + zipcode);
            return client.execute(query, [zipcode], {prepare: true});
        })
        .then(function (result) {
            for(var i = 0; i < result.rows.length; i++){
                row = result.rows[i];
                console.log('row: ', row);
            }
            //console.log('#rows: ', result.rows.length);
            //const row = result.rows[0];
            //console.log('Obtained row: ', row);
            console.log('Shutting down');
            client.shutdown();

            //console.log('JSON.stringify(row): ' + JSON.stringify(row));
            return JSON.stringify(row);
        })
        .catch(function (err) {
            console.error('There was an error when connecting', err);
            return client.shutdown();
        });
}

/*client.execute(query) {
  .then(result => {
  console.log('hospitalname: %s', result.first().hospitalname);
  });

client.execute(query, function(err, result) {
  assert.ifError(err);
  for(var i = 0; i < result.length; i++){
  console.log('name: ' + result[i]);
  }*/
