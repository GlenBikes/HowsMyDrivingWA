const awsMock = require("aws-sdk-mock"),
      AWS = require("aws-sdk");

var assert = require('assert'),
    log4js = require('log4js'),
    server = require('../server'),
    sinon = require('sinon'),
    strUtils = require('../util/stringutils.js'),
    twitmocks   = require('../test/mocks/twitter.js');

// Log files
log4js.configure('./config/log4js.json');
var log = log4js.getLogger("test");

awsMock.setSDKInstance(AWS);

var config = {
  twitter: {
    consumer_key: process.env.CONSUMER_KEY,
    consumer_secret: process.env.CONSUMER_SECRET,
    access_token: process.env.ACCESS_TOKEN,
    access_token_secret: process.env.ACCESS_TOKEN_SECRET
  }
};

describe('Tweet handling', function() {
  describe('Handle tweet with reference', function() {
    it('should write a single request', (done) => {
      try {
        var start, end;
        var datetimehigh = new Date();

        start = Date.now();
        awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(params, callback){
          callback(null, { UnprocessedItems: [] });
        });

        var docClient = new AWS.DynamoDB.DocumentClient();

        var T = {
          get: ( path, params, cb) => {
            var data = {
              statuses: [
                twitmocks._createTweet({
                  id: 123,
                  id_str: '123',
                  full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX:78DFSD for me?`
                })
              ]
            };

            cb(null, data, null);
          }
        }

        return new Promise( ( resolve, reject ) => {
          server._processNewTweets(T, docClient).then( () => {
            assert(true);
            resolve();
          });
        });
      } finally {
        done();
      }
    });
  });
});

describe('Tweet handling TEST', function() {
  describe('Handle tweet with reference', function() {
    it('should write a single request', (done) => {
      try {
        awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(params, callback){
          callback(null, { UnprocessedItems: [] });
        });

        var docClient = new AWS.DynamoDB.DocumentClient();

        var batchWriteJSON = 
        {
          RequestItems: { 
            'TestTable': [
              {
                PutRequest: {
                  Item: {
                    id: '1',
                    a: 'b',
                    b: 'c'
                  }
                }
              }
            ]
          }
        };

        return new Promise( ( resolve, reject ) => {
          docClient.batchWrite(batchWriteJSON, function(err, data) {
            assert.equal(data.UnprocessedItems.length, 0);
            resolve (data);
          });
        });
      } finally {
        awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
        done();
      }
    });
  });
});

describe('Mock testing', function() {
  describe('Mock DynamoDB', function() {
    it('should be mocked', (done) => {
      try {
        awsMock.mock('DynamoDB.DocumentClient', 'get', function (params, callback) {
          callback(null, {pk: "foo", sk: "bar"});
        })

        let input = { TableName: '', Key: {} };
        const client = new AWS.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});
        
        return new Promise( ( resolve, reject ) => {
          client.get(input, function (err, result) {
            assert.deepEqual(result, { pk: 'foo', sk: 'bar' });
            resolve(result);
          });
        });
      }
      finally {
        awsMock.restore('DynamoDB.DocumentClient');
        done();
      }
    })
  })
})
