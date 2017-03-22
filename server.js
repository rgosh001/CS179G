// index.js

console.log('hello from Node.js')

const http = require('http');
const httpProxy = require('http-proxy');
const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');
var Promise = require('promise');


// connect to the cluster
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    protocolOptions: {port:'9043'},
    keyspace: 'hospitals'
});

client.connect();

var server = http.createServer(function (req, res) {
    res.writeHead(200, {
        'Content-Type': 'text/plain' ,
        'Access-Control-Allow-Origin': 'http://localhost:8888',
    });

    var zipcode = req.url.substr(1, req.url.length - 1);

    console.log('before client.execute');

    var row;
    const query = 'SELECT zipcode, pop, numberofdoctors, ratio FROM pop_doctor_ratio WHERE zipcode = ?';
    client.execute(query, [zipcode], {prepare: true}).then(function(result) {
        console.log('after execute and before for');
        for(var i = 0; i < result.rows.length; i++){
            row = result.rows[i];
            console.log('row: ', row);
        }
        console.log('finished printing');
        res.write(JSON.stringify(row));
        res.end();
        console.log('Shutting down');
        client.shutdown();
    })
}).listen(8000);

console.log("listening on port 8000")

server.on('error', function (e) {
    // Handle your error here
    console.log(e);
});


function execQueryOnZipcode(zipcode){
    const query = 'SELECT zipcode, pop, numberofdoctors, ratio FROM pop_doctor_ratio WHERE zipcode = ?';
    var row;
    return new Promise(function(resolve, reject) {

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

        if(err){
            return reject(err);
        }

        return resolve(result);
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
