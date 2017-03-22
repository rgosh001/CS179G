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

const client2 = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    protocolOptions: {port:'9043'},
    keyspace: 'providers'
});

client.connect();
client2.connect();

var server = http.createServer(function (req, res) {
    res.writeHead(200, {
        'Content-Type': 'text/plain' ,
        'Access-Control-Allow-Origin': 'http://localhost:8888',
    });


    // req.url = /1a&zipcode
    // zipcode = 1a&zipcode
    var params = req.url.substr(1, req.url.length - 1);
    var cases = params.substr(0, 2);
    var zipcode = params.substr(3, params.length - 1);
    var amp = zipcode.indexOf('&');
    var zipcode2 = params.substr(3, amp);
    var specialty = zipcode.substr(amp + 1, req.url.length - 1);
    switch(cases) {
        case '1a':
            console.log('case: ' + cases);
            console.log(params);
            console.log(zipcode);
            console.log('before client.execute');
            var row;
            const query1 = 'SELECT zipcode, pop, numberofdoctors, ratio FROM pop_doctor_ratio WHERE zipcode = ?';
            client.execute(query1, [zipcode], {prepare: true}).then(function(result) {
                console.log('after execute and before for');
                console.log('finished printing');
                res.write(JSON.stringify(result.rows[0]));
                res.end();
            });
            break;
        case '1b':
            console.log('case: ' + cases);
            console.log(params);
            console.log(zipcode);
            console.log('before client.execute');
            var row;
            const query2 = 'SELECT zipcode, x, y, hospitalname, providernumber, pop, numberofdoctors, numberofhospitals, ratio FROM pop_doctor_ratio_hospital WHERE zipcode = ?';
            client.execute(query2, [zipcode], {prepare: true}).then(function(result) {
                console.log('after execute and before for');
                console.log('finished printing');
                res.write(JSON.stringify(result.rows));
                res.end();
            });
            break;
        case '2a':
            console.log('case: ' + cases);
            console.log(zipcode2);
            console.log(specialty);
            console.log('before client.execute');
            var row;
            const query3 = 'SELECT zipcode, taxonomycode1, count FROM taxonomy_count WHERE zipcode = ? and taxonomycode1 = ?';
            client2.execute(query3, [zipcode2, specialty], {prepare: true}).then(function(result) {
                console.log('after execute and before for');
                console.log('finished printing');
                res.write(JSON.stringify(result.rows[0]));
                res.end();
            });
            break;
        default:
            break;
    }
}).listen(8000);

console.log("listening on port 8000")

server.on('error', function (e) {
    // Handle your error here
    console.log(e);
});
