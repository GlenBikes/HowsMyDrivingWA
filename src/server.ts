
/* Setting things up. */
import * as AWS from 'aws-sdk';
import { DocumentClient, QueryOutput } from 'aws-sdk/clients/dynamodb';
import { Request, Response } from 'express';
import * as http from 'http';
import * as uuid from 'uuid';
import * as mutex from 'mutex';
import * as redis_server from 'redis-server';

// howsmydriving-utils
import { Citation } from 'howsmydriving-utils';
import { CitationIds } from 'howsmydriving-utils';
import { DumpObject } from 'howsmydriving-utils';
import { IRegion } from 'howsmydriving-utils';
import { ICitation } from 'howsmydriving-utils';
import { CompareNumericStrings } from 'howsmydriving-utils';
import { SplitLongLines } from 'howsmydriving-utils';
import { PrintTweet } from 'howsmydriving-utils';

import {
  GetNewTweets,
  GetNewDMs,
  GetTweetById,
  SendTweets,
  ITweet,
  ITwitterUser
} from 'howsmydriving-twitter';

// interfaces internal to project
import { IRequestRecord } from './interfaces';
import { IReportItemRecord } from './interfaces';
import { ICitationRecord } from './interfaces';
import { CitationRecord } from './interfaces';
import { StatesAndProvinces, formatPlate } from './interfaces';
import { GetHowsMyDrivingId } from './interfaces';
import { ReportItemRecord } from './interfaces';

import { getUnusedPort } from './util/process';

// legacy commonjs modules
const express = require('express'),
  fs = require('fs'),
  LocalStorage = require('node-localstorage').LocalStorage,
  path = require('path'),
  Q = require('dynamo-batchwrite-queue'),
  soap = require('soap'),
  packpath = require('packpath');

// Local storage to keep track of our last processed tweet/dm
var localStorage = new LocalStorage('./.localstore');

// packpath.self() does not seem to work for a top-level app.
// Just use packpath.parent{} which is /app.
const package_json_path = path.join(packpath.parent(), '/package.json');

let pjson = require(package_json_path);

export var __MODULE_NAME__: string = pjson.name;

import { log, lastdmLog, lastmentionLog } from './logging';

const redis_port: number = getUnusedPort();
const redis_srv = new redis_server({
  port: redis_port,
  bin: path.join(packpath.parent(), '/.data/redis-server')
});

const PROCESS_EXIT_CODES = {
  'no-regions': -20
};

var processing_mutex;

// We lock a mutex when doing any of the processing steps to make sure we never 
// have multiple "threads" concurrently reading tweets/DB and writing DB / tweets.
// Most of these are relatively fast operations
const mutex_lock_opts = {
  duration: 5 * 60 * 1000,
  maxWait: 10 * 60 * 1000
};

// When sending tweets, we take much longer because we wait in between
// each tweet post so that Twitter doesn't flag our account as a bot.
// This means that for 1 region to send a bunch of tweets could take quite
// a long time and the other regions could wait a very long time before
// they acquire the mutex and can start sending their tweets.
// TODO: Make sure the report item records in DB are updated for each
// region that sends tweets. This way if other regions time out waiting for
// the mutex, they can independently send their tweets on the next attempt
// without resending the first region's tweets again. Otherwise it would likely
// result in the last region never being able to acquire the mutex without
// timing out and the earlier regions would keep attempting to spam the
// same tweets over and over forever.
const mutex_lock_opts_send_tweets = {
  duration: 10 * 60 * 1000, // It could potentially take a long time send a bunch of tweets
  maxWait: 30 * 60 * 1000   // the more regions we have, the longer the last reghion will have to wait to start sending tweets
};

redis_srv
  .on('open', () => {
    log.debug(`redis_srv started on port ${redis_port}. Creating mutex...`);
    processing_mutex = mutex( {id: uuid.v1(), strategy: { name: 'redis', connectionString: `redis://127.0.0.1:${redis_port}`} } );
    log.debug(`Mutex created.`);
  })
  .on('opening', () => {
    log.debug('redis_srv opening...');
  })
  .on('closing', () => {
    log.debug('redis_srv closing...');
  })
  .on('close', () => {
    log.debug('redis_srv closed.');
  });

redis_srv
  .open()
  .then(() => {
    log.debug(`redis_srv open completed.`);
  })
  .catch((err: Error) => {
    handleError(err);
  });

const noCitationsFoundMessage = 'No __REGION_NAME__ citations found for plate #__LICENSE__ . [__DATETIME__]',
  noValidPlate =
    'No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #. [__DATETIME__]',
  citationQueryText = 'License #__LICENSE__ has been queried __COUNT__ times.';

const app = express();

// Don't think we need to override this. The only EventEmitters we
// use are for mocha test execution, fs file watching.
process.setMaxListeners(15);

const MAX_RECORDS_BATCH = 2000,
  INTER_TWEET_DELAY_MS =
    process.env.hasOwnProperty('INTER_TWEET_DELAY_MS') &&
    CompareNumericStrings(process.env.INTER_TWEET_DELAY_MS, '0') < 0
      ? parseInt(process.env.INTER_TWEET_DELAY_MS, 10)
      : 5000;

export const tableNames: { [tabletype: string]: string } = {
  Request: `${process.env.DB_PREFIX}_Request`,
  Citations: `${process.env.DB_PREFIX}_Citations`,
  ReportItems: `${process.env.DB_PREFIX}_ReportItems`
};

AWS.config.update({ region: 'us-east-2' });

const maxTweetLength: number = 280 - 17; // Max username is 15 chars + '@' plus the space after the full username
const noCitations: string = 'No citations found for plate # ';
const parkingAndCameraViolationsText: string =
  'Total parking and camera violations for #';
const violationsByYearText: string = 'Violations by year for #';
const violationsByStatusText: string = 'Violations by status for #';
const licenseQueriedCountText: string =
  'License __LICENSE__ has been queried __COUNT__ times.';
const licenseRegExp: RegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;

const MUTEX_KEY: { [index: string]: string } = {
  tweet_processing: '__HOWSMYDRIVING_TWEET_PROCESSING__',
  dm_processing: '__HOWSMYDRIVING_DM_PROCESSING__',
  request_processing: '__HOWSMYDRIVING_REQUEST_PROCESSING__',
  citation_processing: '__HOWSMYDRIVING_CITATION_PROCESSING__',
  report_item_processing: '__HOWSMYDRIVING_REPORT_ITEM_PROCESSING__',
  tweet_sending: '__HOWSMYDRIVING_TWEET_SENDING__',
  dm_sending: '__HOWSMYDRIVING_DM_SENDING__'
};

app.use(express.static('public'));

var listener = app.listen(process.env.PORT, function() {
  log.info(
    `${pjson.name} version: '${pjson.version}', port: ${
      listener.address().port
    }, started.`
  );
});

process
  .on('exit', code => {
    log.info(`Process exiting with exit code: ${code}.`);
    listener.close();
  })
  .on('uncaughtException', err => {
    log.error(`Caught exception: ${err}\n`);
  })
  .on('unhandledRejection', (reason, promise) => {
    log.error(
      `Unhandled Rejection at: promise: ${DumpObject(
        promise
      )}, reason: ${reason}.`
    );
  });

// Initialize regions
log.info('Loading regions...');
const regions: { [key: string]: IRegion } = {};
let import_promises: Array<Promise<void>> = [];

if (!process.env.REGIONS || process.env.REGIONS.split(',').length == 0) {
  log.error(`No regions configured in process.env.REGIONS. Aborting.`);
  process.exit(PROCESS_EXIT_CODES['no-regions']);
}

// TODO: This needs to be async
process.env.REGIONS.split(',').forEach(region_package => {
  region_package = region_package.trim();
  
  if (!region_package || region_package.length === 0) {
    log.error(`No regions configured in process.env.REGIONS. Aborting.`);
    process.exit(PROCESS_EXIT_CODES['no-regions']);
  }
  
  log.info(`Loading package ${region_package}`);
  import_promises.push(
    new Promise<any>((resolve, reject) => {
      import(region_package)
        .then(module => {
          log.debug(
            `Imported region module ${module.Region.name} from module ${region_package}.`
          );

          regions[module.Region.name] = module.Region;
          resolve(module);
        })
        .catch(err => {
          handleError(err);
        });
    })
  );
});

// Wait for the imports to finish
Promise.all(import_promises)
  .then(modules => {
    log.info(`Loaded ${modules.length} regions.`);
  })
  .catch(err => {
    handleError(err);
  });

/* tracks the largest tweet ID retweeted - they are not processed in order, due to parallelization  */
/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all('/tweet', function(request: Request, response: Response) {
  var docClient: any = new AWS.DynamoDB.DocumentClient();

  /* First, let's load the ID of the last tweet we responded to. */
  var maxTweetIdRead: string;
  var maxDmIdRead: string;
  var last_mention_id = (maxTweetIdRead = getLastMentionId());
  var last_dm_id = (maxDmIdRead = getLastDmId());

  if (!last_mention_id) {
    handleError(new Error('ERROR: No last tweet found!'));
  }

  if (!last_dm_id) {
    handleError(new Error('ERROR: No last dm found!'));
  }

  var tweet_promises = [];

  log.debug(`Locking processing_mutex for ${MUTEX_KEY['tweet_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['tweet_processing'],
    mutex_lock_opts_send_tweets,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.debug(`Locked processing_mutex for ${MUTEX_KEY['tweet_processing']}.`);

        var twitter_promises: Array<Promise<Array<ITweet>>> = [];

        let get_tweets_promise: Promise<Array<ITweet>> = GetNewTweets(
          last_mention_id
        );

        var dm_process_promise = GetNewDMs(last_dm_id);

        twitter_promises.push(get_tweets_promise);
        // TODO: Move DM's out, wrapped w their own mutex
        twitter_promises.push(dm_process_promise);

        // Now wait until processing of both tweets and dms is done.
        get_tweets_promise
          .then(tweets => {
              processTweets(tweets, docClient)
                .then(requests_written => {
                  if (tweets.length == 0) {
                    log.info(`No new mentions found.`);
                  }
                  else {
                    log.debug(`Finished processing tweets.`);
                  }

                  log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} on success...`);
                  processing_mutex.unlock(lock);
                  log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['tweet_processing']} on success.`);

                  response.sendStatus(200);
                })
                .catch(err => {
                  handleError(err);
                });
            // TODO do the same for DM promise
          })
          .catch((err: Error) => {
            handleError(err);
          });
      } catch (err) {
        log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} on error...`);
        processing_mutex.unlock(lock);
        log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['tweet_processing']} on error.`);
        response.status(500).send(err);
      }
    }
  );
});

// uptimerobot.com hits this every 5 minutes
app.all('/processrequests', (request: Request, response: Response) => {
  log.debug(`Locking processing_mutex for ${MUTEX_KEY['request_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['request_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      log.debug(`Locked processing_mutex for ${MUTEX_KEY['request_processing']}.`);

      processRequestRecords()
        .then(() => {
          log.info(`Finished processing requests.`);

          log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['request_processing']} on success...`);
          processing_mutex.unlock(lock);
          log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['request_processing']} on success.`);

          response.sendStatus(200);
        })
        .catch((err: Error) => {
          log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['request_processing']} on error...`);
          processing_mutex.unlock(lock);
          log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['request_processing']} on error.`);

          response.status(500).send(err);
        });
    }
  );
});

app.all('/processcitations', (request: Request, response: Response) => {
  log.debug(`Locking processing_mutex for ${MUTEX_KEY['citation_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['citation_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.debug(`Locked processing_mutex for ${MUTEX_KEY['citation_processing']}.`);

        processCitationRecords().then(() => {
          log.info(`Finished processing citations.`);

          log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['citation_processing']} on success...`);
          processing_mutex.unlock(lock);
          log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['citation_processing']} on success.`);

          response.sendStatus(200);
        });
      } catch (err) {
        log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['citation_processing']} on error...`);
        processing_mutex.unlock(lock);
        log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['citation_processing']} on error.`);

        response.status(500).send(err);
      }
    }
  );
});

app.all('/processreportitems', (request: Request, response: Response) => {
  log.debug(`Locking processing_mutex for ${MUTEX_KEY['report_item_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['report_item_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.debug(`Locked processing_mutex for ${MUTEX_KEY['report_item_processing']}.`);

        processReportItemRecords()
          .then(() => {
            log.info(`Finished processing report items.`);

            log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['report_item_processing']} on success...`);
            processing_mutex.unlock(lock);
            log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['report_item_processing']} on success.`);

            response.sendStatus(200);
          })
          .catch(err => {
            handleError(err);
          });
      } catch (err) {
        log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['report_item_processing']} on error...`);
        processing_mutex.unlock(lock);
        log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['report_item_processing']} on error.`);

        response.status(500).send(err);
      }
    }
  );
});

app.all(
  ['/errors', '/error', '/err'],
  (request: Request, response: Response) => {
    try {
      log.info('errors');
      var fileName = path.resolve(`${__dirname}/../log/err.log`);

      log.info(`Sending file: ${fileName}.`);
      response.sendFile(fileName);
    } catch (err) {
      response.status(500).send(err);
    }
  }
);

app.all(['/test', '/tests', '/runtest', '/runtests'], function(
  request: Request,
  response: Response
) {
  // Doing the require here will cause the tests to rerun every
  // time the /test url is loaded even if no test or product
  // code has changed.
  var Mocha = require('mocha');
  // Instantiate a Mocha instance.
  var mocha = new Mocha();
  var testDir = path.resolve(path.join(__dirname, '../test'));

  log.info(`testDir: ${testDir}`);

  // Add each .js file to the mocha instance
  fs.readdirSync(testDir)
    .filter((file: string) => {
      // Only keep the .js files
      return file.substr(-3) === '.js';
    })
    .forEach((file: string) => {
      mocha.addFile(path.join(testDir, file));
    });

  var test_results = '';
  var failures = false;

  // Run the tests.
  mocha
    .run()
    .on('test', (test: any) => {
      test_results += `Test started: ${test.title}\n`;
    })
    .on('test end', (test: any) => {
      //test_results += `Test done: ${test.title}\n`;
    })
    .on('pass', (test: any) => {
      test_results += `Test passed: ${test.title}\n\n`;
    })
    .on('fail', (test: any, err: Error) => {
      test_results += `Test failed: ${test.title}\nError:\n${err}\n\n`;
      failures = true;
    })
    .on('end', () => {
      test_results +=
        '*********************************************\n\nTests all finished!\n\n********************************************';
      // send your email here
      if (!failures) {
        response.sendStatus(200);
      } else {
        response.status(500).send(test_results);
      }
    });
});

app.all('/dumpfile', (request: Request, response: Response) => {
  try {
    log.info('dumpfile');
    var fileName = path.resolve(`${__dirname}/../log/err.log`);

    if (request.query.hasOwnProperty('filename')) {
      fileName = path.resolve(`${__dirname}/../${request.query.filename}`);
    }
    log.info(`Sending file: ${fileName}.`);
    response.sendFile(fileName);
  } catch (err) {
    response.status(500).send(err);
  }
});

app.all('/dumptweet', (request: Request, response: Response) => {
  try {
    if (request.query.hasOwnProperty('id')) {
      var tweet = GetTweetById(request.query.id);
      response.set('Cache-Control', 'no-store');
      response.json(tweet);
    } else {
      handleError(new Error('Error: id is required for /dumptweet'));
    }
  } catch (err) {
    response.status(500).send(err);
  }
});

app.all('/dumpcitations', (request: Request, response: Response) => {
  try {
    let state: string;
    let plate: string;
    if (
      request.query.hasOwnProperty('state') &&
      request.query.hasOwnProperty('plate')
    ) {
      state = request.query.state;
      plate = request.query.plate;
    } else {
      throw new Error('state and plate query string parameters are required.');
    }

    Object.keys(regions).forEach(region_name => {
      regions[region_name]
        .GetCitationsByPlate(plate, state)
        .then((citations: Array<ICitation>) => {
          var body = 'Citations found:\n';

          if (!citations || citations.length == 0) {
            response.send(noCitations);
          } else {
            response.json(citations);
          }
        })
        .catch(err => {
          handleError(err);
        });
    });
  } catch (err) {
    response.status(500).send(err);
  }
});

export function processTweets(
  tweets: Array<ITweet>,
  docClient: AWS.DynamoDB.DocumentClient
): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    /* First, let's load the ID of the last tweet we responded to. */
    var maxTweetIdRead;
    var last_mention_id = (maxTweetIdRead = getLastMentionId());

    var batch_write_promise: Promise<void>;
    var request_records: Array<any> = [];

    if (tweets.length > 0) {
      tweets.forEach((tweet: ITweet) => {
        if (CompareNumericStrings(maxTweetIdRead, tweet.id_str) < 0) {
          maxTweetIdRead = tweet.id_str;
        }

        const { state, plate } = parseTweet(tweet.full_text);
        var now = Date.now();

        log.info(`Found ${PrintTweet(tweet)}`);

        var item = {
          PutRequest: {
            Item: {
              id: GetHowsMyDrivingId(),
              license: `${state}:${plate}`, // TODO: Create a function for this plate formatting.
              created: now,
              modified: now,
              processing_status: 'UNPROCESSED',
              tweet_id: tweet.id,
              tweet_id_str: tweet.id_str,
              tweet_user_id: tweet.user.id,
              tweet_user_id_str: tweet.user.id_str,
              tweet_user_screen_name: tweet.user.screen_name
            }
          }
        };

        request_records.push(item);
      });
    }

    if (request_records.length > 0) {
      batch_write_promise = batchWriteWithExponentialBackoff(
        docClient,
        tableNames['Request'],
        request_records
      );
    }

    // Update the ids of the last tweet/dm if we processed
    // anything with larger ids.
    if (CompareNumericStrings(maxTweetIdRead, last_mention_id) > 0) {
      setLastMentionId(maxTweetIdRead);
    }

    if (batch_write_promise) {
      batch_write_promise
        .then(() => {
          log.info(`Wrote ${request_records.length} request records.`);
          resolve(request_records.length);
        })
        .catch(err => {
          handleError(err);
        });
    } else {
      resolve(0);
    }
  });
}

function processRequestRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    var docClient = new AWS.DynamoDB.DocumentClient();

    log.debug(`Checking for request records for all regions...`);

    GetRequestRecords()
      .then((request_records: Array<IRequestRecord>) => {
        var request_promises: Array<Promise<void>> = [];
        let citation_records: Array<object> = [];
        let citations_written: {
          [id: string]: { [region_name: string]: number };
        } = {};
        let licenses: { [request_id: string]: string } = {};

        // create a map of request_id to license so we can use it when logging summary
        request_records.forEach(request_record => {
          licenses[request_record.id] = request_record.license;
        });

        // DynamoDB does not allow any property to be null or empty string.
        // Set these values to 'None' or a default number.
        // TODO: This needs to move down into the region modules.
        const column_overrides: { [key: string]: any } = {
          CaseNumber: -1,
          ChargeDocNumber: 'None',
          Citation: 'None',
          CollectionsStatus: 'None',
          FilingDate: 'None',
          InCollections: 'false',
          Status: 'None',
          Type: 'None',
          ViolationDate: 'None',
          ViolationLocation: 'None'
        };

        if (request_records && request_records.length > 0) {
          log.info(
            `Processing ${request_records.length} request records for all regions...`
          );

          request_records.forEach(item => {
            let tokens: Array<string> = item.license.split(':');
            let state: string;
            let plate: string;

            //initialize the hash for this request id.
            citations_written[item.id] = {};

            if (tokens.length == 2) {
              state = tokens[0];
              plate = tokens[1];
            }

            if (!state || state === '' || !plate || plate === '') {
              log.warn(`Not a valid state/plate in request (${item.id}).`);

              // There was no valid plate found in the tweet. Add a dummy citation.
              var now = Date.now();
              // TTL is 10 years from now until the records are PROCESSED
              var ttl_expire: number = new Date(now).setFullYear(
                new Date(now).getFullYear() + 10
              );
              var citation: ICitationRecord = {
                id: GetHowsMyDrivingId(),
                citation_id: CitationIds.CitationIDNoPlateFound,
                Citation: CitationIds.CitationIDNoPlateFound,
                region: 'Global', // Not a valid region, but we don't pass this one down to region plugins.
                processing_status: 'UNPROCESSED',
                license: item.license,
                request_id: item.id,
                created: now,
                modified: now,
                ttl_expire: ttl_expire,
                tweet_id: item.tweet_id,
                tweet_id_str: item.tweet_id_str,
                tweet_user_id: item.tweet_user_id,
                tweet_user_id_str: item.tweet_user_id_str,
                tweet_user_screen_name: item.tweet_user_screen_name
              };

              citation_records.push({
                PutRequest: {
                  Item: citation
                }
              });

              citations_written[item.id]['Invalid Plate'] = 1;

              // Add a fake promise since we didn't make any async calls so we're done.
              log.info(
                `Resolving promise for request ${item.id} since it had no valid plate.`
              );
              request_promises.push(Promise.resolve());
            } else {
              Object.keys(regions).forEach(region_name => {
                log.info(
                  `Getting citations for request ${item.id} ${state}:${plate} in region ${region_name}...`
                );

                request_promises.push(
                  new Promise<void>((resolve, reject) => {
                    regions[region_name]
                      .GetCitationsByPlate(plate, state)
                      .then(citations => {
                        log.debug(
                          `Retrieved ${citations.length} citations for ${plate}:${state} in ${region_name} region.`
                        );
                        if (!citations || citations.length == 0) {
                          var now = Date.now();
                          // TTL is 10 years from now until the records are PROCESSED
                          var ttl_expire: number = new Date(now).setFullYear(
                            new Date(now).getFullYear() + 10
                          );

                          var citation_record: ICitationRecord = {
                            id: GetHowsMyDrivingId(),
                            citation_id: CitationIds.CitationIDNoCitationsFound,
                            region: region_name,
                            request_id: item.id,
                            processing_status: 'UNPROCESSED',
                            license: item.license,
                            created: now,
                            modified: now,
                            ttl_expire: ttl_expire,
                            tweet_id: item.tweet_id,
                            tweet_id_str: item.tweet_id_str,
                            tweet_user_id: item.tweet_user_id,
                            tweet_user_id_str: item.tweet_user_id_str,
                            tweet_user_screen_name: item.tweet_user_screen_name
                          };

                          // TODO: Move this down to the region plugins
                          mungeObject(citation_record, column_overrides);

                          citation_records.push({
                            PutRequest: {
                              Item: citation_record
                            }
                          });

                          if (!(region_name in citations_written[item.id])) {
                            citations_written[item.id][region_name] = 0;
                          }
                          citations_written[item.id][region_name] += 1;
                        } else {
                          citations.forEach(citation => {
                            var now = Date.now();
                            // TTL is 10 years from now until the records are PROCESSED
                            var ttl_expire: number = new Date(now).setFullYear(
                              new Date(now).getFullYear() + 10
                            );

                            let citation_record: CitationRecord = new CitationRecord(
                              citation
                            );

                            citation_record.id = GetHowsMyDrivingId();
                            citation_record.citation_id = citation.citation_id;
                            citation_record.request_id = item.id;
                            citation_record.region = region_name;
                            citation_record.processing_status = 'UNPROCESSED';
                            citation_record.license = item.license;
                            citation_record.created = now;
                            citation_record.modified = now;
                            citation_record.ttl_expire = ttl_expire;
                            citation_record.tweet_id = item.tweet_id;
                            citation_record.tweet_id_str = item.tweet_id_str;
                            citation_record.tweet_user_id = item.tweet_user_id;
                            citation_record.tweet_user_id_str =
                              item.tweet_user_id_str;
                            citation_record.tweet_user_screen_name =
                              item.tweet_user_screen_name;

                            // DynamoDB does not allow any property to be null or empty string.
                            // Set these values to 'None'.
                            mungeObject(citation_record, column_overrides);

                            citation_records.push({
                              PutRequest: {
                                Item: citation_record
                              }
                            });
                          });

                          if (!(region_name in citations_written[item.id])) {
                            citations_written[item.id][region_name] = 0;
                          }
                          citations_written[item.id][region_name] +=
                            citations.length;
                        }

                        // OK we have the citations for this region/plate combination,
                        // resolve this promise
                        log.info(
                          `Resolving inner request_promise for request ${item.id}.`
                        );
                        resolve();
                      })
                      .catch((err: Error) => {
                        handleError(err);
                      });
                  })
                );
              });
            }
          });

          log.debug(
            `Waiting for ${request_promises.length} queries by region plugins to complete...`
          );

          Promise.all(request_promises)
            .then(() => {
              log.info(`All request_promises resolved.`);

              // We have all the citations for every request from every region.
              // Now write the citation records for all request/region combinations
              batchWriteWithExponentialBackoff(
                new AWS.DynamoDB.DocumentClient(),
                tableNames['Citations'],
                citation_records
              ).then(() => {
                let msg: string = 'Finished writing:';

                Object.keys(citations_written).forEach(request_id => {
                  Object.keys(citations_written[request_id]).forEach(
                    region_name => {
                      if (region_name === 'Invalid Plate') {
                        msg += `\n  1 citations for invalid plate in request ${request_id}.`;
                      } else {
                        msg += `\n  ${citations_written[request_id][region_name]} citations for request ${request_id} ${licenses[request_id]} in ${region_name} region.`;
                      }
                    }
                  );
                });

                log.info(msg);

                var request_update_records: Array<any> = [];
                var now = Date.now();
                // Now that the record is PROCESSED, TTL is 30 days
                var ttl_expire: number =
                  new Date().getTime() + 30 * 24 * 60 * 60 * 1000;

                request_records.forEach(request_record => {
                  request_record.processing_status = 'PROCESSED';
                  request_record.modified = now;
                  request_record.ttl_expire = ttl_expire;

                  request_update_records.push({
                    PutRequest: {
                      Item: request_record
                    }
                  });
                });

                batchWriteWithExponentialBackoff(
                  new AWS.DynamoDB.DocumentClient(),
                  tableNames['Request'],
                  request_update_records
                )
                  .then(() => {
                    // This is the one success point for this request.
                    // All other codepaths indicate a failure.
                    log.info(
                      `Set ${request_update_records.length} request records to PROCESSED.`
                    );

                    // resolve the main promise returned from this function
                    log.info(
                      `Resolving primary promise for processRequestRecords() after updating request records.`
                    );
                    resolve();
                  })
                  .catch((err: Error) => {
                    handleError(err);
                  });
              });
            })
            .catch((err: Error) => {
              handleError(err);
            });
        } else {
          log.info(
            `Resolving primary promise for processRequestRecords() since there were no request records.`
          );
          resolve();
          log.info('No request records found for any region.');
        }
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function processCitationRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    var docClient = new AWS.DynamoDB.DocumentClient();

    log.debug('Querying citation records for all regions...');
    GetCitationRecords()
      .then((citations: Array<ICitationRecord>) => {
        if (citations && citations.length > 0) {
          let citationsByRequest: {
            [request_id: string]: {
              [region_name: string]: Array<ICitationRecord>;
            };
          } = {};
          let citationsByPlate: { [plate: string]: number } = {};
          let licenseByRequest: { [request_id: string]: string } = {};

          log.debug(
            `Processing ${citations.length} citation records for all regions...`
          );

          // Sort them based on request
          citations.forEach((citation: ICitationRecord) => {
            citationsByPlate[citation.license] = 1;

            if (!(citation.request_id in citationsByRequest)) {
              citationsByRequest[citation.request_id] = {};
            }

            if (!(citation.region in citationsByRequest[citation.request_id])) {
              citationsByRequest[citation.request_id][
                citation.region
              ] = new Array<ICitation>();
            }

            citationsByRequest[citation.request_id][citation.region].push(
              citation
            );

            if (!(citation.request_id in licenseByRequest)) {
              licenseByRequest[citation.request_id] = citation.license;
            }
          });

          let requestsforplate_promises: {
            [plate: string]: Promise<{
              license: string;
              query_count: number;
            }>;
          } = {};

          // Kick of the DB calls to get query counts for each of these requests
          log.debug(
            `Starting queries for query counts of ${citationsByPlate.length} licenses.`
          );

          // TODO: Is there a way to do this all in one query?
          Object.keys(citationsByPlate).forEach(license => {
            requestsforplate_promises[license] = GetQueryCount(license);
          });

          let report_items: Array<IReportItemRecord> = [];
          var report_items_by_request: {
            [request_id: string]: {
              [region_name: string]: number;
            };
          } = {};

          // Block until all those GetQueryCount calls are done.
          log.debug(
            `Waiting for ${citationsByPlate.length} license query count queries to complete...`
          );

          Promise.all(Object.values(requestsforplate_promises))
            .then(license_query_pairs => {
              let license_query_hash: { [license: string]: number } = {};

              license_query_pairs.forEach(pair => {
                license_query_hash[pair.license] = pair.query_count;
              });

              // Now process the citations, on a per-request basis
              Object.keys(citationsByRequest).forEach(request_id => {
                report_items_by_request[request_id] = {};

                Object.keys(citationsByRequest[request_id]).forEach(
                  region_name => {
                    report_items_by_request[request_id][region_name] = 0;
                    log.debug(
                      `Processing citations for request ${request_id} in region ${region_name}.`
                    );
                    // Get the first citation to access citation columns
                    let citation: ICitationRecord =
                      citationsByRequest[request_id][region_name][0];

                    // Check to see if there was only a dummy citation for this plate
                    if (
                      citationsByRequest[request_id][region_name].length == 1 &&
                      citation.citation_id < CitationIds.MINIMUM_CITATION_ID
                    ) {
                      log.debug(
                        `No citations found for request ${request_id} license ${licenseByRequest[request_id]} in ${region_name} region.`
                      );

                      let message: string = GetReportItemForPseudoCitation(
                        citation,
                        region_name,
                        license_query_hash[citation.license]
                      );

                      report_items.push(
                        new ReportItemRecord(message, 0, citation)
                      );
                      report_items_by_request[request_id][region_name] = 1;

                      log.info(
                        `Created 1 report item record for request ${request_id} ${licenseByRequest[request_id]} in ${region_name} region.`
                      );
                    } else {
                      log.debug(
                        `Processing citations for request ${request_id} ${licenseByRequest[request_id]} in ${region_name} region.`
                      );
                      log.debug(
                        `regions[region_name] for region_name ${region_name}: ${DumpObject(
                          regions[region_name]
                        )}.`
                      );
                      let messages = regions[
                        region_name
                      ].ProcessCitationsForRequest(
                        citationsByRequest[request_id][region_name],
                        license_query_hash[citation.license]
                      );

                      // 1. Go through all messages and split up any that will be > 280 characters when tweeted.
                      // TODO: We should probably do this when processing the records, not before writing them.
                      let truncated_messages: Array<string> = SplitLongLines(
                        messages,
                        maxTweetLength
                      );

                      log.info(
                        `Retrieved ${truncated_messages.length} messages for request ${request_id} region ${region_name}.`
                      );

                      report_items_by_request[request_id][region_name] =
                        truncated_messages.length;

                      truncated_messages.forEach((message, index) => {
                        report_items.push(
                          new ReportItemRecord(
                            message,
                            index - 1,
                            citationsByRequest[request_id][region_name][0]
                          )
                        );
                      });
                    }
                  }
                );
              });

              // Write report items.
              log.info(
                `Writing ${report_items.length} total report item records:`
              );

              Object.keys(citationsByRequest).forEach(request_id => {
                Object.keys(report_items_by_request[request_id]).forEach(region_name => {
                  if (report_items_by_request[request_id][region_name])
                  log.info(
                    ` - ${report_items_by_request[request_id][region_name]} for request ${request_id} license ${licenseByRequest[request_id] === ':' ? 'invalid license' : licenseByRequest[request_id]} for ${region_name} region`
                  );
                });
              });

              WriteReportItemRecords(
                new AWS.DynamoDB.DocumentClient(),
                report_items
              )
                .then(() => {
                  log.info(
                    `Wrote ${report_items.length} report item records for all regions.`
                  );

                  // Set the processing status of all the citations
                  var citation_records: any = [];
                  var now = Date.now();
                  // Now that the record is PROCESSED, TTL is 30 days
                  var ttl_expire: number =
                    new Date().getTime() + 30 * 24 * 60 * 60 * 1000;

                  let msg: string = 'Updated to PROCESSED:';

                  Object.keys(citationsByRequest).forEach(request_id => {
                    Object.keys(citationsByRequest[request_id]).forEach(
                      region_name => {
                        msg += `\n  ${
                          Object.keys(
                            citationsByRequest[request_id][region_name]
                          ).length
                        } citation records for request ${request_id} ${
                          licenseByRequest[request_id] === ':' ? 'invalid license' : licenseByRequest[request_id]
                        } in ${region_name} region.`;

                        citationsByRequest[request_id][region_name].forEach(
                          (citation, index) => {
                            citation.processing_status = 'PROCESSED';
                            citation.modified = now;
                            citation.ttl_expire = ttl_expire;

                            citation_records.push({
                              PutRequest: {
                                Item: citation
                              }
                            });
                          }
                        );
                      }
                    );
                  });

                  log.debug(
                    `Updating ${citation_records.length} citation records...`
                  );

                  batchWriteWithExponentialBackoff(
                    new AWS.DynamoDB.DocumentClient(),
                    tableNames['Citations'],
                    citation_records
                  )
                    .then(() => {
                      // This is the one success point for this request.
                      // All other codepaths indicate a failure.
                      log.info(msg);

                      // This is the one success point for all citations being processed.
                      // Every other codepath is a failure of some kind.
                      log.debug('Processing citations completed successfully.');
                      resolve();
                    })
                    .catch((err: Error) => {
                      handleError(err);
                    });
                })
                .catch((e: Error) => {
                  handleError(e);
                });
            })
            .catch(err => {
              handleError(err);
            });
        } else {
          log.info('No citations found.');

          // This is the one success point for all citations being processed.
          // Every other codepath is a failure of some kind.
          log.debug('Resolving top-level promise.');
          resolve();
        }
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function processReportItemRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    var docClient = new AWS.DynamoDB.DocumentClient();
    var request_promises: Array<Promise<void>> = [];

    GetReportItemRecords()
      .then((report_items: Array<IReportItemRecord>) => {
        var reportitem_count = report_items.length;
        var tweetCountByRequest: {
          [request_id: string]: { [region_name: string]: number };
        } = {};

        if (report_items && report_items.length > 0) {
          log.info(
            `Processing ${report_items.length} report items for all regions...`
          );
          var reportItemsByRequest: {
            [request_id: string]: {
              [region_name: string]: Array<IReportItemRecord>;
            };
          } = {};

          // Sort them based on request and region
          report_items.forEach(report_item => {
            if (!(report_item.request_id in reportItemsByRequest)) {
              reportItemsByRequest[report_item.request_id] = {};
            }

            if (
              !(
                report_item.region in
                reportItemsByRequest[report_item.request_id]
              )
            ) {
              reportItemsByRequest[report_item.request_id][
                report_item.region
              ] = [];
            }

            reportItemsByRequest[report_item.request_id][
              report_item.region
            ].push(report_item);
          });

          // For each request/region, we need to sort the report items since the order
          // they are tweeted in matters.
          Object.keys(reportItemsByRequest).forEach(request_id => {
            Object.keys(reportItemsByRequest[request_id]).forEach(
              region_name => {
                reportItemsByRequest[request_id][
                  region_name
                ] = reportItemsByRequest[request_id][region_name].sort(
                  (a, b) => {
                    return a.record_num - b.record_num;
                  }
                );
              }
            );
          });

          // Now process the report_items, on a per-request/region basis
          Object.keys(reportItemsByRequest).forEach((request_id: string) => {
            Object.keys(reportItemsByRequest[request_id]).forEach(
              region_name => {
                log.debug(`Pushing promise for request ${request_id} region ${region_name}.`);
                
                request_promises.push( new Promise<void>(
                  (resolve, reject) => {
                    log.debug(`Locking processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name}...`);

                    processing_mutex.lock(
                      MUTEX_KEY['tweet_sending'],
                      mutex_lock_opts,
                      (err, lock) => {
                        if (err) {
                          handleError(err);
                        }

                        try {
                          log.debug(`Locked processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name}.`);

                        // Get the first report_item to access report_item columns
                        var report_item =
                          reportItemsByRequest[request_id][region_name][0];

                        log.info(
                          `Processing ${reportItemsByRequest[request_id][region_name].length} report items for ${region_name} region...`
                        );

                        // Build a fake tweet for the request report_item
                        let user: ITwitterUser = {} as ITwitterUser;
                        user.screen_name = report_item.tweet_user_screen_name;

                        let origTweet: ITweet = {
                          id: report_item.tweet_id,
                          id_str: report_item.tweet_id_str,
                          user: user
                        } as ITweet;

                        log.info(
                          `Posting ${reportItemsByRequest[request_id][region_name].length} tweets for request ${request_id} ${report_item.license} in ${region_name} region.`
                        );

                        SendTweets(
                          region_name,
                          origTweet,
                          reportItemsByRequest[request_id][region_name].map(
                            ({ tweet_text }) => tweet_text
                          )
                        )
                          .then(tweets_sent_count => {
                            log.info(
                              `Finished sending ${reportItemsByRequest[request_id][region_name].length} tweets for request ${request_id} ${report_item.license == ':' ? "'invalid license'" : report_item.license} for ${region_name} region.`
                            );

                            if (
                              !(report_item.request_id in tweetCountByRequest)
                            ) {
                              tweetCountByRequest[report_item.request_id] = {};
                            }

                            if (
                              !(
                                report_item.region in
                                tweetCountByRequest[report_item.request_id]
                              )
                            ) {
                              tweetCountByRequest[report_item.request_id][
                                report_item.region
                              ] = 0;
                            }

                            tweetCountByRequest[report_item.request_id][
                              report_item.region
                            ] += tweets_sent_count;

                            // Tweets for this request/region have been sent.
                            // Release the mutex so the rest of the request/region
                            // combinations can be released to send tweets
                            log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['tweet_sending']} on success for request ${request_id} region ${region_name}...`);
                            processing_mutex.unlock(lock);
                            log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['tweet_sending']} on success for request ${request_id} region ${region_name}.`);

                            log.debug(`Resolving request_promise for request ${request_id} region ${region_name}`);
                            resolve();
                          })
                          .catch((err: Error) => {
                            handleError(err);
                          });
                        } catch (err) {
                          log.debug(`Unlocking processing_mutex for ${MUTEX_KEY['tweet_sending']} on error for request ${request_id} region ${region_name}...`);
                          processing_mutex.unlock(lock);
                          log.debug(`Unlocked processing_mutex for ${MUTEX_KEY['tweet_sending']} on error for request ${request_id} region ${region_name}.`);
                          handleError(err);
                        }
                      });
                  }));
              });
          });

          log.info(
            `Waiting for ${request_promises.length} region plugin processes for all regions.`
          );

          Promise.all(request_promises)
            .then(() => {
              if (request_promises.length > 0) {
                // Report how many tweets we sent for each region.
                let msg: string = 'Tweets sent:';

                Object.keys(tweetCountByRequest).forEach(request_id => {
                  Object.keys(tweetCountByRequest[request_id]).forEach(
                    region_name => {
                      msg += `\n - ${tweetCountByRequest[request_id][region_name]} tweets for request ${request_id} ${reportItemsByRequest[request_id][region_name][0].license} in ${region_name} region for ${reportItemsByRequest[request_id][region_name].length} report items`;
                    }
                  );
                });

                log.info(msg);
              }

              // Set the processing status of all the report_items
              var report_item_records: Array<object> = [];

              Object.keys(reportItemsByRequest).forEach(request_id => {
                Object.keys(reportItemsByRequest[request_id]).forEach(
                  region_name => {
                    var now = Date.now();
                    // Now that the record is PROCESSED, TTL is 30 days
                    var ttl_expire: number =
                      new Date().getTime() + 30 * 24 * 60 * 60 * 1000;

                    reportItemsByRequest[request_id][region_name].forEach(
                      report_item => {
                        report_item.processing_status = 'PROCESSED';
                        report_item.modified = now;
                        report_item.ttl_expire = ttl_expire;

                        report_item_records.push({
                          PutRequest: {
                            Item: report_item
                          }
                        });
                      }
                    );
                  }
                );
              });

              batchWriteWithExponentialBackoff(
                new AWS.DynamoDB.DocumentClient(),
                tableNames['ReportItems'],
                report_item_records
              )
                .then(() => {
                  // This is the one and only success point for these report item records.
                  // Every other codepath is an error of some kind.
                  log.debug(
                    `Updated ${report_item_records.length} report items to PROCESSED for all regions.`
                  );

                  // Tweets for all the requests completed successfully and report item records updated.
                  log.debug(`Resolving top-level promise after upating report item records.`)
                  resolve();
                })
                .catch((err: Error) => {
                  handleError(err);
                });
            })
            .catch((err: Error) => {
              handleError(err);
            });
        } else {
          log.info('No report items found for any region.');
          resolve();
        }
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function batchWriteWithExponentialBackoff(
  docClient: AWS.DynamoDB.DocumentClient,
  table: string,
  records: Array<object>
): Promise<void> {
  return new Promise((resolve, reject) => {
    var qdb = docClient ? Q(docClient) : Q();
    qdb.drain = function() {
      resolve();
    };

    qdb.error = function(err: Error, task: any) {
      reject(err);
    };

    var startPos: number = 0;
    var endPos: number;
    while (startPos < records.length) {
      endPos = startPos + 25;
      if (endPos > records.length) {
        endPos = records.length;
      }

      let params = {
        RequestItems: {
          [`${table}`]: records.slice(startPos, endPos)
        }
      };

      qdb.push(params);

      startPos = endPos;
    }
  });
}

function GetReportItemForPseudoCitation(
  citation: ICitation,
  region_name: string,
  query_count: number
): string {
  if (!citation || citation.citation_id >= CitationIds.MINIMUM_CITATION_ID) {
    throw new Error(`ERROR: Unexpected citation ID: ${citation.citation_id}.`);
  }

  switch (citation.citation_id) {
    case CitationIds.CitationIDNoPlateFound:
      return noValidPlate.replace('__DATETIME__', new Date().toLocaleTimeString());
      break;

    case CitationIds.CitationIDNoCitationsFound:
      return (
        noCitationsFoundMessage
          .replace('__LICENSE__', formatPlate(citation.license))
          .replace('__REGION_NAME__', region_name)
          .replace('__DATETIME__', new Date().toLocaleTimeString()) +
        '\n\n' +
        citationQueryText
          .replace('__LICENSE__', formatPlate(citation.license))
          .replace('__COUNT__', query_count.toString())
      );
      break;

    default:
      throw new Error(
        `ERROR: Unexpected citation ID: ${citation.citation_id}.`
      );
      break;
  }
}

export function chompTweet(tweet: ITweet) {
  // Extended tweet objects include the screen name of the tweeting user within the full_text,
  // as well as all replied-to screen names in the case of a reply.
  // Strip off those because if UserA tweets a license plate and references the bot and then
  // UserB replies to UserA's tweet without explicitly referencing the bot, we do not want to
  // process that tweet.
  var chomped = false;
  var text = tweet.full_text;

  if (
    tweet.display_text_range != null &&
    tweet.display_text_range.length >= 2 &&
    tweet.display_text_range[0] > 0
  ) {
    text = tweet.full_text.substring(tweet.display_text_range[0]);
    chomped = true;
  }

  return {
    chomped: chomped,
    chomped_text: text
  };
}

// Extract the state:license and optional flags from tweet text.
function parseTweet(text: string) {
  var state;
  var plate;
  const matches = licenseRegExp.exec(text);

  if (matches == null || matches.length < 2 || matches[1] == 'XX') {
    log.warn(`No license found in tweet: ${text}`);
  } else {
    state = matches[1];
    plate = matches[2];

    if (StatesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      log.warn(`Invalid state: ${state}`);
      state = '';
      plate = '';
    }
  }

  return {
    state: state ? state.toUpperCase() : '',
    plate: plate ? plate.toUpperCase() : ''
  };
}

function mungeObject(o: any, propertyOverrides: { [key: string]: any }): void {
  for (var p in o) {
    if (o.hasOwnProperty(p)) {
      if (p in propertyOverrides) {
        // OK, we need to override this property if it is undefined, null or empty string
        var val = o[p];

        if (val == undefined || val == null || val == '') {
          o[p] = propertyOverrides[p];
        }
      }
    }
  }
}

function GetQueryCount(
  license: string
): Promise<{ license: string; query_count: number }> {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var request_records = [];

  // Query unprocessed requests
  var params = {
    TableName: tableNames['Request'],
    IndexName: 'license-index',
    Select: 'COUNT',
    KeyConditionExpression: '#license = :lic',
    ExpressionAttributeNames: {
      '#license': 'license'
    },
    ExpressionAttributeValues: {
      ':lic': license
    }
  };

  return new Promise<{ license: string; query_count: number }>(
    (resolve, reject) => {
      // 1. Do a query to get just the request rcords for this license.
      //    If the result is not complete, then we have to take the request_id's
      //    we got back and do individual queries for UNPROCESSED citations for
      //    each request_id. This ensures we process all the citations for a given
      //    request together. This is required cause we tweet out summaries/totals.
      docClient.query(
        params,
        (err: Error, result: AWS.DynamoDB.Types.QueryOutput) => {
          if (err) {
            handleError(err);
          }

          let query_count: number = result.Count;

          resolve({ license, query_count });
        }
      );
    }
  );
}

function getLastDmId(): string {
  var lastdm = localStorage.getItem('lastdm');

  // TODO: Should we rather just add some code to go query what the most
  // recent tweet/dm id is (can we even do that with dm?) and set that as
  // the last id?
  if (!lastdm) {
    // When moving from storing this in local file to using localstorage,
    // There is the first-time read that will return 0.
    // This would result in the bot reprocessing every tweet that is still
    // in the twitter delayed feed.
    // To avoid this, we put the current lastdmid in the .env file
    // and read it from there in this first-read scenario.
    if (process.env.hasOwnProperty('FALLBACK_LAST_DM_ID')) {
      lastdm = process.env.FALLBACK_LAST_DM_ID;

      if (lastdm <= 0) {
        throw new Error('No last dm id found.');
      } else {
        setLastDmId(lastdm);
      }
    }
  }

  return lastdm ? lastdm : '0';
}

function getLastMentionId(): string {
  var lastmention = localStorage.getItem('lastmention');

  if (!lastmention) {
    // When moving from storing this in local file to using localstorage,
    // There is the first-time read that will return 0.
    // This would result in the bot reprocessing every tweet that is still
    // in the twitter delayed feed.
    // To avoid this, we put the current lastmentionid in the .env file
    // and read it from there in this first-read scenario.
    if (process.env.hasOwnProperty('FALLBACK_LAST_MENTION_ID')) {
      lastmention = process.env.FALLBACK_LAST_MENTION_ID;

      if (lastmention <= 0) {
        throw new Error('No last mention id found.');
      } else {
        setLastMentionId(lastmention);
      }
    }
  }

  return lastmention ? lastmention : '0';
}

function setLastDmId(lastDmId: string) {
  lastdmLog.info(`Writing last dm id ${lastDmId}.`);
  localStorage.setItem('lastdm', lastDmId);
}

function setLastMentionId(lastMentionId: string) {
  lastmentionLog.info(`Writing last mention id ${lastMentionId}.`);
  localStorage.setItem('lastmention', lastMentionId);
}

function handleError(error: Error): void {
  // Truncate the callstack because only the first few lines are relevant to this code.
  var stacktrace = '';

  if (error.stack) {
    error.stack
      .split('\n')
      .slice(0, 10)
      .join('\n');
  }
  var formattedError = `===============================================================================\n${error.message}\n${stacktrace}`;

  log.error(formattedError);
  throw error;
}

// asynchronous query function to fetch all unprocessed request records.
// returns: promise
function GetRequestRecords(): Promise<Array<IRequestRecord>> {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var request_records: Array<IRequestRecord> = [];

  // Query unprocessed requests
  var params = {
    TableName: tableNames['Request'],
    IndexName: 'processing_status-index',
    Select: 'ALL_ATTRIBUTES',
    KeyConditionExpression: '#processing_status = :pkey',
    ExpressionAttributeNames: {
      '#processing_status': 'processing_status'
    },
    ExpressionAttributeValues: {
      ':pkey': 'UNPROCESSED'
    }
  };

  return new Promise(function(resolve, reject) {
    // 1. Do a query to get just the citations that are UNPROCESSED.
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED citations for
    //    each request_id. This ensures we process all the citations for a given
    //    request together. This is required cause we tweet out summaries/totals.
    docClient.query(params, async function(err, result) {
      if (err) {
        handleError(err);
      } else {
        request_records = result.Items as Array<IRequestRecord>;
      }
      resolve(request_records);
    });
  });
}

// asynchronous query function to fetch all citation records.
// returns: promise
function GetCitationRecords(): Promise<Array<ICitationRecord>> {
  var docClient = new AWS.DynamoDB.DocumentClient();
  var citation_records: Array<ICitationRecord> = [];

  // Query unprocessed citations
  var params = {
    TableName: tableNames['Citations'],
    IndexName: 'processing_status-index',
    Select: 'ALL_ATTRIBUTES',

    KeyConditionExpression: '#processing_status = :pkey',
    ExpressionAttributeNames: {
      '#processing_status': 'processing_status'
    },
    ExpressionAttributeValues: {
      ':pkey': 'UNPROCESSED'
    },
    Limit: MAX_RECORDS_BATCH // If more than this, we'll have to handle it below
  };

  return new Promise(function(resolve, reject) {
    // 1. Do a query to get just the citations that are UNPROCESSED.
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED citations for
    //    each request_id. This ensures we process all the citations for a given
    //    request together. This is required cause we tweet out summaries/totals.
    docClient.query(params, async (err: Error, result: QueryOutput) => {
      if (err) {
        handleError(err);
      } else {
        // 2. De-dupe the returned request_id's.
        var requestIDs: { [key: string]: number } = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id as string] = 1;
        });

        // 3. Check if we retrieved all the unprocessed citations
        if (
          result.hasOwnProperty('LastEvaluatedKey') &&
          result.LastEvaluatedKey
        ) {
          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex: number = 0;

          while (
            citation_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            var requestID = requestIDs[requestIndex];
            requestIndex++;

            // 3a. Query for the citations for this request_id
            // Use the index which includes request_id.
            params.IndexName = 'request_id-processing_status-index';
            params.KeyConditionExpression = `request_id = ${requestID} AND processing_status = 'UNPROCESSED'`;
            params.Limit = MAX_RECORDS_BATCH; // If there is a license with more citations than this... good enough

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH citations.
            //       I'm sure some gasshole will eventually prove us overly optimistic.
            await docClient.query(params, (err: Error, result) => {
              if (err) {
                handleError(err);
              } else {
                // TODO: There must be a type-safe way to do this...
                let citation_records_batch: any = result.Items;
                citation_records.concat(
                  citation_records_batch as Array<ICitationRecord>
                );
              }
            });
          }
        } else {
          // TODO: There must be a type-safe way to do this...
          let citation_records_batch: any = result.Items;
          citation_records = citation_records_batch as Array<ICitationRecord>;
        }

        resolve(citation_records);
      }
    });
  });
}

function GetReportItemRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var report_item_records: Array<IReportItemRecord> = [];

  // Query unprocessed report items
  var params = {
    TableName: tableNames['ReportItems'],
    IndexName: 'processing_status-index',
    Select: 'ALL_ATTRIBUTES',

    KeyConditionExpression: '#processing_status = :pkey',
    ExpressionAttributeNames: {
      '#processing_status': 'processing_status'
    },
    ExpressionAttributeValues: {
      ':pkey': 'UNPROCESSED'
    },
    Limit: MAX_RECORDS_BATCH // If more than this, we'll have to handle it below
  };

  return new Promise(function(resolve, reject) {
    // 1. Do a query to get just the report items that are UNPROCESSED.
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED report items for
    //    each request_id. This ensures we process all the report_items for a given
    //    request together. This is required since we can't handle finding some
    // .  additional report items the next time we wake up to process report items.
    docClient.query(params, async function(err, result) {
      if (err) {
        handleError(err);
      } else {
        // 2. De-dupe the returned request_id's.
        var requestIDs: { [key: string]: number } = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id] = 1;
        });

        // 3. Check if we retrieved all the unprocessed report items
        if (
          result.hasOwnProperty('LastEvaluatedKey') &&
          result.LastEvaluatedKey
        ) {
          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex = 0;

          while (
            report_item_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            var requestID = requestIDs[requestIndex];
            requestIndex++;

            // 3a. Query for the report items for this request_id
            // Use the index which includes request_id.
            params.IndexName = 'request_id-processing_status-index';
            params.KeyConditionExpression = `request_id = ${requestID} AND processing_status = 'UNPROCESSED'`;
            params.Limit = MAX_RECORDS_BATCH;

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH report items.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                report_item_records.concat(
                  result.Items as Array<IReportItemRecord>
                );
              }
            });
          }
        } else {
          report_item_records = result.Items as Array<IReportItemRecord>;
        }

        resolve(report_item_records);
      }
    });
  });
}

function WriteReportItemRecords(
  docClient: AWS.DynamoDB.DocumentClient,
  report_items: Array<IReportItemRecord>
) {
  var report_item_records: Array<object> = [];

  report_items.forEach((report_item, index) => {
    var item = {
      PutRequest: {
        Item: report_item
      }
    };

    report_item_records.push(item);
  });

  log.debug(
    `Calling batchWriteWithExponentialBackoff with ${report_item_records.length} report item records.`
  );

  // Write the report item records, returning that Promise.
  return batchWriteWithExponentialBackoff(
    docClient,
    tableNames['ReportItems'],
    report_item_records
  );
}
