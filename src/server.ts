/* Setting things up. */
import * as AWS from 'aws-sdk';
import { DocumentClient, QueryOutput } from 'aws-sdk/clients/dynamodb';
import { Request, Response } from 'express';
import * as http from 'http';
import { LMXClient, LMXBroker, Client, Broker } from 'live-mutex';
import * as Twit from 'twit';

// howsmydriving-utils
import { Citation } from 'howsmydriving-utils';
import { CitationIds } from 'howsmydriving-utils';
import { DumpObject } from 'howsmydriving-utils';
import { IRegion } from 'howsmydriving-utils';
import { ICitation } from 'howsmydriving-utils';
import { CompareNumericStrings } from 'howsmydriving-utils';
import { SplitLongLines } from 'howsmydriving-utils';
import { PrintTweet } from 'howsmydriving-utils';

// interfaces internal to project
import { IRequestRecord } from './interfaces';
import { IReportItemRecord } from './interfaces';
import { ICitationRecord } from './interfaces';
import { CitationRecord } from './interfaces';
import { StatesAndProvinces, formatPlate } from './interfaces';
import { GetHowsMyDrivingId } from './interfaces';
import { ReportItemRecord } from './interfaces';

import * as HMDSEA from 'howsmydriving-seattle';

// legacy commonjs modules
const express = require('express'),
  fs = require('fs'),
  LocalStorage = require('node-localstorage').LocalStorage,
  path = require('path'),
  Q = require('dynamo-batchwrite-queue'),
  soap = require('soap'),
  pjson = require(path.resolve(__dirname + '/../../package.json'));

export var __MODULE_NAME__: string = pjson.name;

import { log, lastdmLog, lastmentionLog } from './logging';

const noCitationsFoundMessage = 'No citations found for plate #',
  noValidPlate =
    'No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #',
  citationQueryText = 'License #__LICENSE__ has been queried __COUNT__ times.';

const app = express(),
  config: any = {
    twitter: {
      consumer_key: process.env.CONSUMER_KEY,
      consumer_secret: process.env.CONSUMER_SECRET,
      access_token: process.env.ACCESS_TOKEN,
      access_token_secret: process.env.ACCESS_TOKEN_SECRET
    }
  };

// Local storage to keep track of our last processed tweet/dm
var localStorage = new LocalStorage('./.localstore');

// Mutex to ensure we don't post tweets in quick succession
const MUTEX_TWIT_POST_MAX_HOLD_MS: number = 100000,
  MUTEX_TWIT_POST_MAX_RETRIES: number = 5,
  MUTEX_TWIT_POST_MAX_WAIT_MS: number = 300000;

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
const botScreenNameRegexp: RegExp = new RegExp(
  '@' + process.env.TWITTER_HANDLE + '\\b',
  'i'
);

const MUTEX_KEY: { [index: string]: string } = {
  tweet_processing: '__HOWSMYDRIVING_TWEET_PROCESSING__',
  dm_processing: '__HOWSMYDRIVING_DM_PROCESSING__',
  request_processing: '__HOWSMYDRIVING_REQUEST_PROCESSING__',
  citation_processing: '__HOWSMYDRIVING_CITATION_PROCESSING__',
  report_item_processing: '__HOWSMYDRIVING_REPORT_ITEM_PROCESSING__'
};

app.use(express.static('public'));

var listener = app.listen(process.env.PORT, function() {
  log.info(
    `${pjson.name} version: '${pjson.version}', port: ${
      listener.address().port
    }, started.`
  );
});

// One global broker for the live-mutex clients.
let mutex_broker = new Broker({});

mutex_broker.emitter.on('warning', function() {
  log.warn(...arguments);
});

mutex_broker.emitter.on('error', function() {
  log.error(...arguments);
});

mutex_broker.ensure().then(() => {
  log.debug(`Successfully created mutex broker.`);
});

log.debug(`Creating mutex client.`);
var mutex_client: Client = new LMXClient();

mutex_client.emitter.on('info', function() {
  log.debug(...arguments);
});

mutex_client.emitter.on('warning', function() {
  log.warn(...arguments);
});

mutex_client.emitter.on('error', function() {
  log.error(...arguments);
});

mutex_client
  .connect()
  .then(client => {
    log.info(`Successfully created mutex client.`);
  })
  .catch((err: Error) => {
    log.info(`Failed to connect mutex client. Err: ${err}.`);
    handleError(err);
  });

// Initialize regions
log.info('Loading regions...');
const regions: { [key: string]: IRegion } = {};
let import_promises: Array<Promise<void>> = [];

process.env.REGIONS.split(',').forEach(region_package => {
  region_package = region_package.trim();
  log.info(`Loading package ${region_package}`);
  import_promises.push(
    new Promise<any>((resolve, reject) => {
      import(region_package)
        .then(module => {
          regions[module.Region.Name] = module.Region;
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
  const T: Twit = new Twit(config.twitter);
  var docClient: any = new AWS.DynamoDB.DocumentClient();

  // We need the bot's app id to detect tweets from the bot
  getAccountID(T)
    .then((app_id: number) => {
      try {
        var twitter_promises: Array<Promise<void>> = [];
        let tweet_process_promise: Promise<void> = processNewTweets(
          T,
          docClient,
          app_id
        );
        var dm_process_promise = processNewDMs();

        twitter_promises.push(tweet_process_promise);
        twitter_promises.push(dm_process_promise);

        // Now wait until processing of both tweets and dms is done.
        Promise.all(twitter_promises)
          .then(() => {
            response.sendStatus(200);
          })
          .catch(err => {
            response.status(500).send(err);
          });
      } catch (err) {
        response.status(500).send(err);
      }
    })
    .catch((err: Error) => {
      handleError(err);
    });
});

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
      const T = new Twit(config.twitter);
      var tweet = getTweetById(T, request.query.id);
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

// uptimerobot.com hits this every 5 minutes
app.all('/processrequests', (request: Request, response: Response) => {
  try {
    let request_promise = processRequestRecords();

    request_promise
      .then(() => {
        response.sendStatus(200);
      })
      .catch((err: Error) => {
        handleError(err);
      });
  } catch (err) {
    response.status(500).send(err);
  }
});

app.all('/processcitations', (request: Request, response: Response) => {
  try {
    let citation_promise = processCitationRecords();

    response.sendStatus(200);
  } catch (err) {
    response.status(500).send(err);
  }
});

app.all('/processreportitems', (request: Request, response: Response) => {
  try {
    let citation_promise = processReportItemRecords();

    response.sendStatus(200);
  } catch (err) {
    response.status(500).send(err);
  }
});

export function processNewTweets(
  T: Twit,
  docClient: AWS.DynamoDB.DocumentClient,
  bot_app_id: number
): Promise<void> {
  let maxTweetIdRead: string = '-1';

  // Collect promises from these operations so they can go in parallel
  var twitter_promises: Array<Promise<void>> = [];

  /* First, let's load the ID of the last tweet we responded to. */
  var last_mention_id = (maxTweetIdRead = getLastMentionId());

  var tweet_promises = [];
  if (!last_mention_id) {
    handleError(new Error('ERROR: No last tweet found!'));
  }

  var mentions_promise = new Promise<void>((resolve, reject) => {
    // Make sure we are the only process doing this or else we'll get dupes.
    let acquired_mutex = {
      key: '',
      id: ''
    };
    mutex_client
      .acquireLock(MUTEX_KEY['request_processing'], {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS,
        maxRetries: MUTEX_TWIT_POST_MAX_RETRIES,
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      })
      .then(v => {
        log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
        acquired_mutex.key = v.key;
        acquired_mutex.id = v.id;

        log.debug(`Checking for tweets greater than ${last_mention_id}.`);
        /* Next, let's search for Tweets that mention our bot, starting after the last mention we responded to. */
        T.get(
          'search/tweets',
          {
            q: '%40' + process.env.TWITTER_HANDLE,
            since_id: last_mention_id,
            tweet_mode: 'extended'
          },
          function(
            err: Error,
            data: Twit.Twitter.SearchResults,
            response: http.IncomingMessage
          ) {
            if (err) {
              handleError(err);
            }

            let num_tweets: number = data.statuses.length;
            let num_request_records: number = 0;
            if (data.statuses.length) {
              /* 
            Iterate over each tweet. 

            The replies can occur concurrently, but the threaded replies to each tweet must, 
            within that thread, execute sequentially. 

            Since each tweet with a mention is processed in parallel, keep track of largest ID
            and write that at the end.
            */
              data.statuses.forEach((status: Twit.Twitter.Status) => {
                var request_records: Array<any> = [];

                if (CompareNumericStrings(maxTweetIdRead, status.id_str) < 0) {
                  maxTweetIdRead = status.id_str;
                }

                /*
              Make sure this isn't a reply to one of the bot's tweets which would
              include the bot screen name in full_text, but only due to replies.
              */
                const { chomped, chomped_text } = chompTweet(status);

                if (!chomped || botScreenNameRegexp.test(chomped_text)) {
                  /* Don't reply to retweet or our own tweets. */
                  if (status.hasOwnProperty('retweet_status')) {
                    log.info(`Ignoring retweet: ${status.full_text}`);
                  } else if (status.user.id == bot_app_id) {
                    log.info('Ignoring our own tweet: ' + status.full_text);
                  } else {
                    const { state, plate } = parseTweet(chomped_text);
                    const noPlate: boolean =
                      !state || !plate || state === '' || plate === '';
                    var now = Date.now();

                    log.info(`Found ${PrintTweet(status)}`);

                    var item = {
                      PutRequest: {
                        Item: {
                          id: GetHowsMyDrivingId(),
                          license: `${state}:${plate}`, // TODO: Create a function for this plate formatting.
                          created: now,
                          modified: now,
                          processing_status: 'UNPROCESSED',
                          tweet_id: status.id,
                          tweet_id_str: status.id_str,
                          tweet_user_id: status.user.id,
                          tweet_user_id_str: status.user.id_str,
                          tweet_user_screen_name: status.user.screen_name
                        }
                      }
                    };

                    request_records.push(item);
                  }
                } else {
                  log.info(
                    "Ignoring reply that didn't actually reference bot: " +
                      status.full_text
                  );
                }

                if (request_records.length > 0) {
                  num_request_records += request_records.length;
                  twitter_promises.push(
                    batchWriteWithExponentialBackoff(
                      docClient,
                      tableNames['Request'],
                      request_records
                    )
                  );
                }
              });
            } else {
              /* No new mentions since the last time we checked. */
              log.info('No new mentions...');
            }

            Promise.all(twitter_promises)
              .then(() => {
                if (num_tweets > 0) {
                  log.info(
                    `Wrote ${num_request_records} request records for ${num_tweets} tweets.`
                  );
                }

                // Update the ids of the last tweet/dm if we processed
                // anything with larger ids.
                if (
                  CompareNumericStrings(maxTweetIdRead, last_mention_id) > 0
                ) {
                  setLastMentionId(maxTweetIdRead);
                }

                resolve();
              })
              .catch((err: Error) => {
                handleError(err);
              });
          }
        );
      })
      .catch((err: Error) => {
        handleError(err);
      })
      .finally(() => {
        log.debug(
          `Releasing mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
        mutex_client.releaseLock(acquired_mutex.key, {
          id: acquired_mutex.id,
          force: true
        });
        log.debug(
          `Released mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
      });
  });

  return mentions_promise;
}

function processNewDMs() {
  var maxDmIdRead = -1;

  /* Respond to DMs */
  /* Load the ID of the last DM we responded to. */
  var last_dm_id = (maxDmIdRead = getLastDmId());

  if (!last_dm_id) {
    handleError(new Error('ERROR: No last dm found! Defaulting to zero.'));
  }

  var dm_promise = Promise.resolve();
  // Make sure we are the only process doing this or else we'll get dupes.
  let acquired_mutex = {
    key: '',
    id: ''
  };
  mutex_client
    .acquireLock(MUTEX_KEY['dm_processing'], {
      ttl: MUTEX_TWIT_POST_MAX_HOLD_MS,
      maxRetries: MUTEX_TWIT_POST_MAX_RETRIES,
      lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
    })
    .then(v => {
      log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
      acquired_mutex.key = v.key;
      acquired_mutex.id = v.id;

      /**
       *  TODO: Implement DM handling.
       **/
    })
    .catch((err: Error) => {
      handleError(err);
    })
    .finally(() => {
      log.debug(
        `Releasing mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
      );
      mutex_client.releaseLock(acquired_mutex.key, {
        id: acquired_mutex.id,
        force: true
      });
      log.debug(
        `Released mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
      );
    });

  return dm_promise;
}

function processRequestRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let acquired_mutex = {
      key: '',
      id: ''
    };

    mutex_client
      .acquireLock(MUTEX_KEY['request_processing'], {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS,
        maxRetries: MUTEX_TWIT_POST_MAX_RETRIES,
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      })
      .then(v => {
        log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
        acquired_mutex.key = v.key;
        acquired_mutex.id = v.id;

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
                    region: 'dummy', // Not a valid region, but we don't pass this one down to region plugins.
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
                  request_promises.push(Promise.resolve());
                } else {
                  Object.keys(regions).forEach(region_name => {
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
                              var ttl_expire: number = new Date(
                                now
                              ).setFullYear(new Date(now).getFullYear() + 10);

                              var citation_record: ICitationRecord = {
                                id: GetHowsMyDrivingId(),
                                citation_id:
                                  CitationIds.CitationIDNoCitationsFound,
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
                                tweet_user_screen_name:
                                  item.tweet_user_screen_name
                              };

                              // TODO: Move this down to the region plugins
                              mungeObject(citation_record, column_overrides);

                              citation_records.push({
                                PutRequest: {
                                  Item: citation_record
                                }
                              });

                              if (
                                !(region_name in citations_written[item.id])
                              ) {
                                citations_written[item.id][region_name] = 0;
                              }
                              citations_written[item.id][region_name] += 1;
                            } else {
                              citations.forEach(citation => {
                                var now = Date.now();
                                // TTL is 10 years from now until the records are PROCESSED
                                var ttl_expire: number = new Date(
                                  now
                                ).setFullYear(new Date(now).getFullYear() + 10);

                                let citation_record: CitationRecord = new CitationRecord(
                                  citation
                                );

                                citation_record.id = GetHowsMyDrivingId();
                                citation_record.citation_id =
                                  citation.citation_id;
                                citation_record.request_id = item.id;
                                citation_record.region = region_name;
                                citation_record.processing_status =
                                  'UNPROCESSED';
                                citation_record.license = item.license;
                                citation_record.created = now;
                                citation_record.modified = now;
                                citation_record.ttl_expire = ttl_expire;
                                citation_record.tweet_id = item.tweet_id;
                                citation_record.tweet_id_str =
                                  item.tweet_id_str;
                                citation_record.tweet_user_id =
                                  item.tweet_user_id;
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

                              if (
                                !(region_name in citations_written[item.id])
                              ) {
                                citations_written[item.id][region_name] = 0;
                              }
                              citations_written[item.id][region_name] +=
                                citations.length;
                            }

                            // OK we have the citations for this region/plate combination,
                            // resolve this promise
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
              log.info('No request records found for any region.');
            }
          })
          .catch((err: Error) => {
            handleError(err);
          });
      })
      .catch((err: Error) => {
        handleError(err);
      })
      .finally(() => {
        log.debug(
          `Releasing mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
        mutex_client.releaseLock(acquired_mutex.key, {
          id: acquired_mutex.id,
          force: true
        });
        log.debug(
          `Released mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
      });

    resolve();
  });
}

function processCitationRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let acquired_mutex = {
      key: '',
      id: ''
    };
    mutex_client
      .acquireLock(MUTEX_KEY['citation_processing'], {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS,
        maxRetries: MUTEX_TWIT_POST_MAX_RETRIES,
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      })
      .then(v => {
        log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
        acquired_mutex.key = v.key;
        acquired_mutex.id = v.id;

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

                if (
                  !(citation.region in citationsByRequest[citation.request_id])
                ) {
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
                `Starting queries for query counts of ${citationsByPlate} licenses.`
              );

              // TODO: Is there a way to do this all in one query?
              Object.keys(citationsByPlate).forEach(license => {
                requestsforplate_promises[license] = GetQueryCount(license);
              });

              let report_items: Array<IReportItemRecord> = [];

              // Block until all those GetQueryCount calls are done.
              log.debug(
                `Waiting for ${citationsByPlate} license query count queries to complete...`
              );

              Promise.all(Object.values(requestsforplate_promises)).then(
                license_query_pairs => {
                  let license_query_hash: { [license: string]: number } = {};

                  license_query_pairs.forEach(pair => {
                    license_query_hash[pair.license] = pair.query_count;
                  });

                  // Now process the citations, on a per-request basis
                  Object.keys(citationsByRequest).forEach(request_id => {
                    Object.keys(citationsByRequest[request_id]).forEach(
                      region_name => {
                        // Get the first citation to access citation columns
                        let citation: ICitationRecord =
                          citationsByRequest[request_id][region_name][0];

                        // Check to see if there was only a dummy citation for this plate
                        if (
                          citationsByRequest[request_id][region_name].length ==
                            1 &&
                          citation.citation_id < CitationIds.MINIMUM_CITATION_ID
                        ) {
                          log.debug(
                            `No citations found for request ${request_id} license ${licenseByRequest[request_id]} in ${region_name} region.`
                          );

                          let message: string = GetReportItemForPseudoCitation(
                            citation,
                            license_query_hash[citation.license]
                          );

                          report_items.push(
                            new ReportItemRecord(message, 0, citation)
                          );
                          log.info(
                            `Created 1 report item record for request ${request_id} ${licenseByRequest[request_id]} in ${region_name} region.`
                          );
                        } else {
                          log.debug(
                            `Processing citations for request ${request_id} ${licenseByRequest[request_id]} in ${region_name} region.`
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

                          truncated_messages.forEach((message, index) => {
                            report_items.push(
                              new ReportItemRecord(
                                message,
                                index - 1,
                                citationsByRequest[request_id][region_name][0]
                              )
                            );
                          });

                          log.info(
                            `Created ${truncated_messages.length} report item records for request ${request_id} ${licenseByRequest[request_id]} in ${region_name} region.`
                          );
                        }
                      }
                    );
                  });

                  // Write report items.
                  log.debug(
                    `Writing ${report_items.length} total report item records for all regions.`
                  );

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
                              licenseByRequest[request_id]
                            } in ${region_name} region`;

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
                          log.debug(
                            'Processing citations completed successfully.'
                          );
                          resolve();
                        })
                        .catch((err: Error) => {
                          handleError(err);
                        });
                    })
                    .catch((e: Error) => {
                      handleError(e);
                    });
                }
              );
            } else {
              log.info('No citations found.');
            }
          })
          .catch((err: Error) => {
            handleError(err);
          });
      })
      .catch((err: Error) => {
        handleError(err);
      })
      .finally(() => {
        log.debug(
          `Releasing mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
        mutex_client.releaseLock(acquired_mutex.key, {
          id: acquired_mutex.id,
          force: true
        });
        log.debug(
          `Released mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
      });
  });
}

function processReportItemRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let acquired_mutex = {
      key: '',
      id: ''
    };
    mutex_client
      .acquireLock(MUTEX_KEY['report_item_processing'], {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS,
        maxRetries: MUTEX_TWIT_POST_MAX_RETRIES,
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      })
      .then(v => {
        log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
        acquired_mutex.key = v.key;
        acquired_mutex.id = v.id;

        var T = new Twit(config.twitter);
        var docClient = new AWS.DynamoDB.DocumentClient();
        var request_promises: Array<Promise<void>> = [];

        GetReportItemRecords().then(
          (report_items: Array<IReportItemRecord>) => {
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
              Object.keys(reportItemsByRequest).forEach(
                (request_id: string) => {
                  Object.keys(reportItemsByRequest[request_id]).forEach(
                    region_name => {
                      var request_promise: Promise<void> = new Promise<void>(
                        (resolve, reject) => {
                          // Get the first report_item to access report_item columns
                          var report_item =
                            reportItemsByRequest[request_id][region_name][0];

                          log.debug(
                            `Processing ${report_items.length} report items for ${region_name} region...`
                          );

                          // Build a fake tweet for the request report_item
                          let user: Twit.Twitter.User = {} as Twit.Twitter.User;
                          user.screen_name = report_item.tweet_user_screen_name;

                          let origTweet: Twit.Twitter.Status = {
                            id: report_item.tweet_id,
                            id_str: report_item.tweet_id_str,
                            user: user
                          } as Twit.Twitter.Status;

                          log.info(
                            `Posting ${reportItemsByRequest[request_id][region_name].length} tweets for request ${request_id} ${report_item.license} in ${region_name} region.`
                          );

                          SendResponses(
                            T,
                            origTweet,
                            reportItemsByRequest[request_id][region_name]
                          )
                            .then(tweets_sent_count => {
                              log.info(
                                `Finished sending ${reportItemsByRequest[request_id][region_name].length} tweets for request ${request_id} ${report_item.license} for ${region_name} region.`
                              );

                              if (
                                !(report_item.request_id in tweetCountByRequest)
                              ) {
                                tweetCountByRequest[
                                  report_item.request_id
                                ] = {};
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

                              // Set the processing status of all the report_items
                              var report_item_records: Array<object> = [];
                              var now = Date.now();
                              // Now that the record is PROCESSED, TTL is 30 days
                              var ttl_expire: number =
                                new Date().getTime() + 30 * 24 * 60 * 60 * 1000;

                              reportItemsByRequest[request_id][
                                region_name
                              ].forEach(report_item => {
                                report_item.processing_status = 'PROCESSED';
                                report_item.modified = now;
                                report_item.ttl_expire = ttl_expire;

                                report_item_records.push({
                                  PutRequest: {
                                    Item: report_item
                                  }
                                });
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
                                    `Updated ${report_item_records} report items to PROCESSED for all regions.`
                                  );
                                  resolve();
                                })
                                .catch((err: Error) => {
                                  handleError(err);
                                });
                            })
                            .catch((err: Error) => {
                              handleError(err);
                            });
                        }
                      );

                      request_promises.push(request_promise);
                    }
                  );
                }
              );
            } else {
              log.info('No report items found for any region.');
            }

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
                // Tweets for all the requests have completed successfully
                resolve();
              })
              .catch((err: Error) => {
                handleError(err);
              });
          }
        );
      })
      .catch((err: Error) => {
        handleError(err);
      })
      .finally(() => {
        log.debug(
          `Releasing mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
        mutex_client.releaseLock(acquired_mutex.key, {
          id: acquired_mutex.id,
          force: true
        });
        log.debug(
          `Released mutex key=${acquired_mutex.key}, id:${acquired_mutex.id}...`
        );
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
  query_count: number
): string {
  if (!citation || citation.citation_id >= CitationIds.MINIMUM_CITATION_ID) {
    throw new Error(`ERROR: Unexpected citation ID: ${citation.citation_id}.`);
  }

  switch (citation.citation_id) {
    case CitationIds.CitationIDNoPlateFound:
      return noValidPlate;
      break;

    case CitationIds.CitationIDNoCitationsFound:
      return (
        `${noCitationsFoundMessage}${formatPlate(citation.license)}` +
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

export function chompTweet(tweet: Twit.Twitter.Status) {
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
      handleError(new Error(`Invalid state: ${state}`));
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

function SendResponses(
  T: Twit,
  origTweet: Twit.Twitter.Status,
  report_items: Array<IReportItemRecord>
): Promise<number> {
  if (report_items.length == 0) {
    // return an promise that is already resolved, ending the recursive
    // chain of promises that have been built.
    return Promise.resolve(0);
  }

  // Clone the report_items array so we don't modify the one passed to us
  var report_items_clone: Array<IReportItemRecord> = [...report_items];
  var report_item: IReportItemRecord = report_items_clone.shift();
  var replyToScreenName: string = origTweet.user.screen_name;
  var replyToTweetId: string = origTweet.id_str;

  /* Now we can respond to each tweet. */
  // When doing the initial reply to the user's tweet, we need to include their
  // twitter account in the text of the tweet (i.e. @replyToScreenName).
  // But when replying to our own replies, we should not include our own mention
  // or else those tweets will show up in the timelines of everyone who
  // follows the bot.
  var tweetText = '';

  if (
    !(
      replyToScreenName.toUpperCase() ===
      process.env.TWITTER_HANDLE.toUpperCase()
    )
  ) {
    tweetText += '@' + replyToScreenName + ' ';
  }

  tweetText += report_item.tweet_text;
  log.debug(
    `Sending Tweet for request ${report_item.request_id} ${report_item.license} in ${report_item.region} region: ${tweetText}.`
  );
  return new Promise<number>((resolve, reject) => {
    let tweets_sent: number = 0;

    // There will be one thread running this for each request we are
    // processing. We need to make sure we don't send tweets in quick
    // succession or Twitter will tag them as spam and they won't
    // render i the thread of resposes.
    // So wait at least INTER_TWEET_DELAY_MS ms between posts.
    T.post(
      'statuses/update',
      {
        status: tweetText,
        in_reply_to_status_id: replyToTweetId
        /*,
        auto_populate_reply_metadata: true*/
      } as Twit.Params,
      (
        err: Error,
        data: Twit.Twitter.Status,
        response: http.IncomingMessage
      ) => {
        let twit_error_code: number = 0;

        if (err && err.hasOwnProperty('code')) {
          twit_error_code = (err as any)['code'];
        }

        if (err && twit_error_code != 187) {
          handleError(err);
        } else {
          if (err && twit_error_code == 187) {
            // This appears to be a "status is a duplicate" error which
            // means we are trying to resend a tweet we already sent.
            // Pretend we succeeded.
            log.info(
              `Received error 187 sending tweet for request ${report_item.request_id} ${report_item.license} in ${report_item.region} region: ${report_item.tweet_text}.`
            );

            // Keep replying to the tweet we were told to reply to.
            // This means that in this scenario, if any of the rest of the tweets in this
            // thread have not been sent, they will create a new thread off the parent of
            // this one.
            // Not ideal, but the other alternatives are:
            // 1) Query for the previous duplicate tweet and then pass that along
            // 2) set all of the replies for this request to be PROCESSED even if they did not
            //    all get tweeted.
            data = origTweet;
          } else {
            tweets_sent++;
            log.info(
              `Sent tweet for request ${report_item.request_id} ${
                report_item.license
              } in ${report_item.region} region: ${PrintTweet(data)}`
            );
          }

          // Wait a bit. It seems tweeting a whackload of tweets in quick succession
          // can cause Twitter to think you're a troll bot or something and then some
          // of the tweets will not display for users other than the bot account.
          // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
          sleep(report_items_clone.length > 0 ? INTER_TWEET_DELAY_MS : 0)
            .then(() => {
              // Send the rest of the responses. When those are sent, then resolve
              // the local Promise.
              SendResponses(T, data, report_items_clone)
                .then(tweets_sent_rest => {
                  tweets_sent += tweets_sent_rest;
                  resolve(tweets_sent);
                })
                .catch((err: Error) => {
                  handleError(err);
                });
            })
            .catch((err: Error) => {
              handleError(err);
            });
        }
      }
    );
  });
}

function getLastDmId() {
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

  return lastdm ? parseInt(lastdm, 10) : 0;
}

function getLastMentionId() {
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

function getTweetById(T: Twit, id: string) {
  // Quick check to fetch a specific tweet.
  var promise: Promise<Twit.Twitter.Status> = new Promise<Twit.Twitter.Status>(
    (resolve, reject) => {
      var retTweet;

      T.get(
        `statuses/show/${id}`,
        { tweet_mode: 'extended' },
        (
          err: Error,
          tweet: Twit.Twitter.Status,
          response: http.IncomingMessage
        ) => {
          if (err) {
            handleError(err);
            reject(tweet);
          }

          resolve(tweet);
        }
      );
    }
  );

  promise
    .then(tweet => {
      return tweet;
    })
    .catch(err => {
      handleError(err);
    });
}

// Fake a sleep function. Call this thusly:
// sleep(500).then(() => {
//   do stuff
// })
// Or similar pattrs that use a Promise
const sleep = (milliseconds: number) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
};

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

function getAccountID(T: Twit): Promise<number> {
  return new Promise((resolve, reject) => {
    T.get(
      'account/verify_credentials',
      {},
      (err: Error, data: any, response: http.IncomingMessage) => {
        if (err) {
          handleError(err);
        }
        resolve(data.id);
      }
    );
  });
}
