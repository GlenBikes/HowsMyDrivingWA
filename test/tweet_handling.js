var assert = require('assert'),
    log4js = require('log4js'),
    server = require('../server'),
    sinon = require('sinon'),
    strUtils = require('../util/stringutils.js'),
    twitmocks   = require('../test/mocks/twitter.js');

// Log files
log4js.configure('./config/log4js.json');
var log = log4js.getLogger("test");

describe('Tweet handling', function() {
  describe('Handle tweet with reference', function() {
    it('should write a single request to Request table', () => {
      const awsMock = require("aws-sdk-mock"),
            AWS = require("aws-sdk");
      
      awsMock.setSDKInstance(AWS);

      debugger; 

      
      const batchWriteSpy = sinon.spy((params, callback) => {
        debugger;
        if (Object.keys(params.RequestItems)[0] != server._tableNames['Request']) {
          debugger;
        }
        assert.equal(Object.keys(params.RequestItems)[0], server._tableNames['Request']);
        assert.equal(params.RequestItems[server._tableNames['Request']].length, 1);
        callback(null, { UnprocessedItems: [] });
      })

      var batchWriteMock = awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', batchWriteSpy);

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

      return new Promise( ( resolve, reject) => {
        server._processNewTweets(T, docClient).then( () => {
          log.trace(`Inside then statement for Handle tweet with reference _processNewTweets.`);
          assert(batchWriteSpy.calledOnce);
          resolve();
          awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
        }).catch ( (err) => {
          
          reject(err);
          awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
        });
      });
    });
  });

  describe('Handle tweet without license', function() {
    it('should write a single request to Request table, saying no license', () => {
      const awsMock = require("aws-sdk-mock"),
            AWS = require("aws-sdk");
        
      awsMock.setSDKInstance(AWS);

      awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(params, callback) {          
        if (Object.keys(params.RequestItems)[0] != server._tableNames['Request']) {
          debugger;
        }
        assert.equal(Object.keys(params.RequestItems)[0], server._tableNames['Request']);
        assert.equal(params.RequestItems[server._tableNames['Request']].length, 1);
        // Make sure this is the "no license found" tweet.
        assert.equal(params.RequestItems[server._tableNames['Request']][0].PutRequest.Item.license, ':');
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
                full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX_78DFSD for me?`
              })
            ]
          };

          cb(null, data, null);
        }
      }

      return new Promise( ( resolve, reject ) => {
        server._processNewTweets(T, docClient).then( () => {
          log.trace(`Inside then statement for Handle tweet without license _processNewTweets.`);
          assert(true);
          resolve();
          awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
        }).catch ( ( err ) => {
          awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
          reject(err);
        });
      });
    });
  });

  describe('Handle multiple tweets', function() {
    it('should write one request to Request table for each tweet', () => {
      const awsMock = require("aws-sdk-mock"),
            AWS = require("aws-sdk");
        
      awsMock.setSDKInstance(AWS);

      awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(params, callback) {          
        if (Object.keys(params.RequestItems)[0] != server._tableNames['Request']) {
          debugger;
        }
        assert.equal(Object.keys(params.RequestItems)[0], server._tableNames['Request']);
        assert.equal(params.RequestItems[server._tableNames['Request']].length, 1);
        // Make sure this is the "no license found" tweet.
        assert.equal(params.RequestItems[server._tableNames['Request']][0].PutRequest.Item.license, ':');
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
                full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX_78DFSD for me?`
              })
            ]
          };

          cb(null, data, null);
        }
      }

      return new Promise( ( resolve, reject ) => {
        server._processNewTweets(T, docClient).then( () => {
        log.trace(`Inside then statement for Handle multiple tweets _processNewTweets.`);
          assert(true);
          resolve();
          awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
        });
      });
    });
  });
});

