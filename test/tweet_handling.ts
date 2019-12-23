import { GetNewTweets } from 'howsmydriving-twitter';

import { tableNames } from '../src/server';
import { createTweet } from './mocks/twitter';
import { uuidv1 } from '../src/util/stringutils';
import * as strUtils from '../src/util/stringutils';

var assert = require('assert'),
  sinon = require('sinon'),
  path = require('path');

/*
describe('Tweet handling', function() {
  describe('Handle tweet with reference', function() {
    it('should write a single request to Request table', () => {
      const awsMock = require('aws-sdk-mock'),
        AWS = require('aws-sdk');

      awsMock.setSDKInstance(AWS);

      // Use the fake timer (now is 0).
      var now = new Date().valueOf();
      var stubNow = sinon.stub(Date, 'now').returns(now);

      const stubUuid = sinon
        .stub(strUtils, 'uuidv1')
        .returns('4887b7a0-09a1-11ea-a100-f9a53a6b0433');

      const batchWriteSpy = sinon.spy((params: any, callback: any) => {
        assert.equal(
          Object.keys(params.RequestItems)[0],
          tableNames['Request']
        );
        assert.equal(params.RequestItems[tableNames['Request']].length, 1);
        callback(null, { UnprocessedItems: [] });
      });

      var batchWriteMock = awsMock.mock(
        'DynamoDB.DocumentClient',
        'batchWrite',
        batchWriteSpy
      );

      var docClient = new AWS.DynamoDB.DocumentClient();

      var T: any = {
        get: (path: string, params: any, cb: any) => {
          var data = {
            statuses: [
              createTweet({
                id: 123,
                id_str: '123',
                full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX:78DFSD for me?`,
                user: {
                  id: 1,
                  id_str: '2',
                  screen_name: 'fakeuser'
                }
              })
            ]
          };

          cb(null, data, null);
        }
      };

      return new Promise((resolve, reject) => {
        processNewTweets(T, docClient, 12)
          .then(() => {
            var expected_params = {
              RequestItems: {
                HMDWA_Request: [
                  {
                    PutRequest: {
                      Item: {
                        id: strUtils.uuidv1(),
                        license: 'TX:78DFSD',
                        created: now,
                        modified: now,
                        processing_status: 'UNPROCESSED',
                        tweet_id: '123',
                        tweet_id_str: '123',
                        tweet_user_id: '-1',
                        tweet_user_id_str: '-1',
                        tweet_user_screen_name: 'None'
                      }
                    }
                  }
                ]
              }
            };
            assert(batchWriteSpy.calledOnce);
            batchWriteSpy.calledWithMatch(expected_params);
            resolve();
          })
          .catch((err: Error) => {
            reject(err);
          })
          .finally(() => {
            awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
            stubNow.restore();
          });
      });
    });
  });

  describe('Handle tweet without license', function() {
    it('should write a single request to Request table, saying no license', () => {
      const awsMock = require('aws-sdk-mock'),
        AWS = require('aws-sdk');

      awsMock.setSDKInstance(AWS);

      awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(
        params: any,
        callback: any
      ) {
        assert.equal(
          Object.keys(params.RequestItems)[0],
          tableNames['Request']
        );
        assert.equal(params.RequestItems[tableNames['Request']].length, 1);
        // Make sure this is the "no license found" tweet.
        assert.equal(
          params.RequestItems[tableNames['Request']][0].PutRequest.Item.license,
          ':'
        );
        callback(null, { UnprocessedItems: [] });
      });

      var docClient = new AWS.DynamoDB.DocumentClient();

      var T = {
        get: (path: string, params: any, cb: any) => {
          var data = {
            statuses: [
              createTweet({
                id: 123,
                id_str: '123',
                full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX_78DFSD for me?`,
                user: {
                  id: 2,
                  id_str: '3',
                  screen_name: 'fakeyuser'
                }
              })
            ]
          };

          cb(null, data, null);
        }
      };

      return new Promise((resolve, reject) => {
        processNewTweets(T as any, docClient, 1)
          .then(() => {
            resolve();
            awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
          })
          .catch((err: Error) => {
            reject(err);
          })
          .finally(() => {
            awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
          });
      });
    });
  });

  describe('Handle multiple tweets', function() {
    it('should write one request to Request table for each tweet', () => {
      const awsMock = require('aws-sdk-mock'),
        AWS = require('aws-sdk');

      awsMock.setSDKInstance(AWS);

      awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', function(
        params: any,
        callback: any
      ) {
        assert.equal(
          Object.keys(params.RequestItems)[0],
          tableNames['Request']
        );
        assert.equal(params.RequestItems[tableNames['Request']].length, 1);
        // Make sure this is the "no license found" tweet.
        assert.equal(
          params.RequestItems[tableNames['Request']][0].PutRequest.Item.license,
          ':'
        );
        callback(null, { UnprocessedItems: [] });
      });

      var docClient = new AWS.DynamoDB.DocumentClient();

      var T = {
        get: (path: string, params: any, cb: any) => {
          var data = {
            statuses: [
              createTweet({
                id: 123,
                id_str: '123',
                full_text: `Hey ${process.env.TWITTER_HANDLE} can you look up TX_78DFSD for me?`,
                user: {
                  id: 4321,
                  id_str: '4321',
                  screen_name: 'dummyuser'
                }
              })
            ]
          };

          cb(null, data, null);
        }
      };

      return new Promise((resolve, reject) => {
        processNewTweets(T as any, docClient, 1)
          .then(() => {
            resolve();
          })
          .catch((err: Error) => {
            throw err;
          })
          .finally(() => {
            awsMock.restore('DynamoDB.DocumentClient', 'batchWrite');
          });
      });
    });
  });
});
*/
