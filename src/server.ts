/* Setting things up. */
import * as AWS from 'aws-sdk';
import { DocumentClient, QueryOutput } from 'aws-sdk/clients/dynamodb';
import { Request, Response } from 'express';
import * as uuid from 'uuid';
import * as mutex from 'mutex';
// See https://redis.io/topics/quickstart
import * as redis_server from 'redis-server';

// howsmydriving-utils
import { Citation, CitationIds } from 'howsmydriving-utils';
import { DumpObject, PrintTweet } from 'howsmydriving-utils';
import { ICitation, IRegion } from 'howsmydriving-utils';
import { ICollision, Collision } from 'howsmydriving-utils';
import { CompareNumericStrings } from 'howsmydriving-utils';
import { SplitLongLines } from 'howsmydriving-utils';

import {
  getBotUser,
  GetNewTweets,
  GetNewDMs,
  GetTweetById,
  IGetTweetsResponse,
  SendTweets,
  UploadMedia
} from 'howsmydriving-twitter';

import { ITweet, ITwitterUser } from 'howsmydriving-utils';

// interfaces internal to project
import {
  ICitationRecord,
  CitationRecord,
  ICollisionRecord,
  CollisionRecord,
  formatPlate,
  GetHowsMyDrivingId,
  IMediaItemRecord,
  MediaItemRecord,
  IRequestRecord,
  IReportItemRecord,
  ReportItemRecord,
  IStateRecord,
  StatesAndProvinces
} from './interfaces';

import {
  IMediaItem,
  MediaItem,
  MediaItemsFromString
} from 'howsmydriving-utils';

import { FormatMilliseconds } from './util/stringutils';

import { getAppRootPath, getUnusedPort } from './util/process';
import { StateStore } from './util/statestore';

// legacy commonjs modules
const express = require('express'),
  fs = require('fs'),
  path = require('path'),
  Q = require('dynamo-batchwrite-queue');

const one_day: number = 1000 * 60 * 60 * 24;
const __HOWSMYDRIVING_DELIMITER__ = '||';

let package_json_path = path.resolve(__dirname + '/../package.json');

if (!fs.existsSync(package_json_path)) {
  package_json_path = path.resolve(__dirname + '/../../package.json');

  if (!fs.existsSync(package_json_path)) {
    throw new Error(`Cannot find package.json: ${__dirname}.`);
  }
}

let pjson = require(package_json_path);

export let __MODULE_NAME__: string = pjson.name;

import { log, lastdmLog, lastmentionLog } from './logging';

// See https://redis.io/topics/quickstart
const redis_port: number = getUnusedPort();
const redis_srv = new redis_server({
  port: redis_port,
  bin: path.join(getAppRootPath(), '/.data/redis-server')
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
  maxWait: 30 * 60 * 1000 // the more regions we have, the longer the last reghion will have to wait to start sending tweets
};

log.info(`Adding redis_srv handlers...`);

redis_srv
  .on('open', () => {
    log.debug(`redis_srv started on port ${redis_port}. Creating mutex...`);
    processing_mutex = mutex({
      id: uuid.v1(),
      strategy: {
        name: 'redis',
        connectionString: `redis://127.0.0.1:${redis_port}`
      }
    });
    log.debug(`Mutex created.`);
  })
  .on('opening', () => {
    log.trace('redis_srv opening...');
  })
  .on('closing', () => {
    log.trace('redis_srv closing...');
  })
  .on('close', () => {
    log.trace('redis_srv closed.');
  });

redis_srv
  .open()
  .then(() => {
    log.trace(`redis_srv open completed.`);
  })
  .catch((err: Error) => {
    handleError(err);
  });

log.info(`Finished adding redis_srv hanlders...`);

export var bot_info: ITwitterUser = {} as ITwitterUser;

// We need the bot's app id to detect tweets from the bot
getBotUser()
  .then(user => {
    bot_info = user;
    log.info(
      `Loaded Twitter bot's info: id: ${bot_info.id} id_str: ${bot_info.id_str} screen_name: ${bot_info.screen_name}.`
    );
  })
  .catch(err => {
    handleError(err);
  });

// Set the screen_name default to be what's in .env files
bot_info.screen_name = process.env.TWITTER_HANDLE
  ? process.env.TWITTER_HANDLE
  : '';

const noCitationsFoundMessage =
    'No __REGION_NAME__ citations found for plate #__LICENSE__.',
  // Included datetime since this tweet is the same all the time and therefore Twitter
  // will prevent it with error code 187 as a duplicate.
  noValidPlate =
    'No valid license found in tweet by @__USER__. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #.',
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
  State: `${process.env.DB_PREFIX}_State`,
  Request: `${process.env.DB_PREFIX}_Request`,
  Citations: `${process.env.DB_PREFIX}_Citations`,
  ReportItems: `${process.env.DB_PREFIX}_ReportItems`,
  Collisions: `${process.env.DB_PREFIX}_Collisions`,
  MediaItems: `${process.env.DB_PREFIX}_MediaItems`
};

AWS.config.update({ region: 'us-east-2' });

const maxTweetLength: number = 280 - 17; // Max username is 15 chars + '@' plus the space after the full username
const noCitations: string = 'No citations found for plate # ';
const parkingAndCameraViolationsText: string =
  'Total parking/camera violations for #';
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
  collision_processing: '__HOWSMYDRIVING_COLLISION_PROCESSING__',
  tweet_sending: '__HOWSMYDRIVING_TWEET_SENDING__',
  dm_sending: '__HOWSMYDRIVING_DM_SENDING__',
  safety_checking: '__HOWSMYDRIVING_SAFETY_CHECKING__'
};

log.info(`Calling app.use...`);

app.use(express.static('public'));

const listen_port: number = getUnusedPort();

var listener = app.listen(process.env.PORT, function() {
  log.info(
    `${pjson.name} version: '${pjson.version}', port: ${
      listener.address().port
    }, started.`
  );
});

log.info(`Configuring process event handlers...`);

process
  .on('exit', code => {
    log.info(`Process exiting with exit code: ${code}.`);
    listener.close();
  })
  .on('uncaughtException', err => {
    log.error(`Caught unhandled exception: ${err}\n ${err.stack}`);
  })
  .on('unhandledRejection', (reason, promise) => {
    log.error(
      `Handling unhandled Rejection at: promise: ${DumpObject(
        promise
      )}, reason: ${reason}.`
    );
  });

// Initialize regions
log.info('Loading regions...');
const regions: { [key: string]: IRegion } = {};
let import_promises: Array<Promise<void>> = [];

if (!process.env.REGIONS || process.env.REGIONS.length == 0) {
  log.error(`No regions configured in process.env.REGIONS. Aborting.`);
  process.exit(PROCESS_EXIT_CODES['no-regions']);
}

// TODO: This needs to be async
export let active_regions: Array<string> = [];

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
          log.trace(`Loaded ${region_package}. Creating region instance.`);

          module.Factory.createRegion(new StateStore(module.Factory.name))
            .then(region => {
              log.debug(
                `Instantiated region ${region.name} from module ${region_package}.`
              );

              regions[region.name] = region;
              active_regions.push(region.name);

              resolve(module);
            })
            .catch(err => {
              handleError(err);
            });
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
  const docClient: any = new AWS.DynamoDB.DocumentClient();

  let tweet_promises = [];

  log.trace(`Locking processing_mutex for ${MUTEX_KEY['tweet_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['tweet_processing'],
    mutex_lock_opts_send_tweets,
    async (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.trace(
          `Locked processing_mutex for ${MUTEX_KEY['tweet_processing']}.`
        );

        /* First, let's load the ID of the last tweet we responded to. */
        getLastMentionId()
          .then(async last_mention_id => {
            if (!last_mention_id) {
              handleError(new Error('ERROR: No last tweet found!'));
            }

            let twitter_promises: Array<Promise<IGetTweetsResponse>> = [];

            let get_tweets_promise: Promise<IGetTweetsResponse> = GetNewTweets(
              last_mention_id
            );

            // TODO: Add DM's
            twitter_promises.push(get_tweets_promise);

            // Now wait until processing of both tweets and dms is done.
            get_tweets_promise
              .then(async resp => {
                processTweets(resp.tweets, docClient)
                  .then(async requests_written => {
                    if (resp.tweets.length == 0) {
                      log.debug(`No new mentions found.`);
                    } else {
                      log.debug(
                        `Finished processing ${resp.tweets.length} tweets.`
                      );

                      await setLastMentionId(resp.last_tweet_read_id);
                    }

                    log.trace(
                      `Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} on error...`
                    );

                    processing_mutex.unlock(lock);

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
          })
          .catch(err => {
            handleError(err);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} on error...`
        );
        processing_mutex.unlock(lock);

        response.status(500).send(err);
      }
    }
  );
});

// uptimerobot.com hits this every 5 minutes
app.all('/processrequests', (request: Request, response: Response) => {
  log.trace(
    `Locking processing_mutex for ${MUTEX_KEY['request_processing']}...`
  );

  processing_mutex.lock(
    MUTEX_KEY['request_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      log.trace(
        `Locked processing_mutex for ${MUTEX_KEY['request_processing']}.`
      );

      try {
        processRequestRecords()
          .then(() => {
            log.info(`Finished processing requests.`);

            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['request_processing']}...`
            );
            processing_mutex.unlock(lock);

            response.sendStatus(200);
          })
          .catch((err: Error) => {
            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['request_processing']} in error...`
            );
            processing_mutex.unlock(lock);

            response.status(500).send(err);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['request_processing']} in outer catch...`
        );
        processing_mutex.unlock(lock);
      }
    }
  );
});

app.all('/processcitations', (request: Request, response: Response) => {
  log.trace(
    `Locking processing_mutex for ${MUTEX_KEY['citation_processing']}...`
  );

  processing_mutex.lock(
    MUTEX_KEY['citation_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.trace(
          `Locked processing_mutex for ${MUTEX_KEY['citation_processing']}.`
        );

        processCitationRecords()
          .then(() => {
            log.info(`Finished processing citations.`);

            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['citation_processing']}...`
            );
            processing_mutex.unlock(lock);

            response.sendStatus(200);
          })
          .catch(err => {
            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['citation_processing']} in error...`
            );
            processing_mutex.unlock(lock);

            response.status(500).send(err);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['citation_processing']} in outer catch...`
        );
        processing_mutex.unlock(lock);

        response.status(500).send(err);
      }
    }
  );
});

app.all('/processreportitems', (request: Request, response: Response) => {
  log.trace(
    `Locking processing_mutex for ${MUTEX_KEY['report_item_processing']}...`
  );

  try {
    processing_mutex.lock(
      MUTEX_KEY['report_item_processing'],
      mutex_lock_opts,
      (err, lock) => {
        if (err) {
          handleError(err);
        }

        log.trace(
          `Locked processing_mutex for ${MUTEX_KEY['report_item_processing']}.`
        );

        processReportItemRecordsNew(
          'UNPROCESSED',
          'PROCESSED',
          doSendTweetsForRegion
        )
          .then(tweets_sent_count => {
            log.debug(
              `Finished processing report items. ${tweets_sent_count} tweets sent.`
            );

            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['report_item_processing']}.`
            );
            processing_mutex.unlock(lock);

            response.sendStatus(200);
          })
          .catch(err => {
            log.info(`Error processing report item records: ${err}.`);

            response.status(500).send(err);
          })
          .finally(() => {
            log.trace(
              `Unlocking processing_mutex in error for ${MUTEX_KEY['report_item_processing']}.`
            );
            processing_mutex.unlock(lock);
          });
      }
    );
  } catch (err) {
    log.error(
      `Exception locking mutex before processing report item records: ${err}.`
    );

    response.status(500).send(err);
  }
});

app.all('/collisions', function(request: Request, response: Response) {
  log.trace(`Locking processing_mutex for ${MUTEX_KEY['safety_checking']}...`);

  processing_mutex.lock(
    MUTEX_KEY['safety_checking'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.trace(
          `Locked processing_mutex for ${MUTEX_KEY['safety_checking']}.`
        );

        let dynamodb_records: Array<any> = [];

        checkForCollisions()
          .then(() => {
            response.sendStatus(200);
          })
          .catch((err: Error) => {
            handleError(err);
          })
          .finally(() => {
            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['safety_checking']}...`
            );
            processing_mutex.unlock(lock);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['safety_checking']} in error...`
        );
        processing_mutex.unlock(lock);

        response.status(500).send(err);
      }
    }
  );
});

app.all('/processcollisions', function(request: Request, response: Response) {
  log.trace(
    `Locking processing_mutex for ${MUTEX_KEY['collision_processing']}...`
  );

  processing_mutex.lock(
    MUTEX_KEY['collision_processing'],
    mutex_lock_opts,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      log.trace(
        `Locked processing_mutex for ${MUTEX_KEY['collision_processing']}.`
      );

      try {
        processCollisionRecords()
          .then(() => {
            log.debug(`Finished processing collisions.`);

            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['collision_processing']}...`
            );
            processing_mutex.unlock(lock);

            response.sendStatus(200);
          })
          .catch((err: Error) => {
            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['collision_processing']} in error...`
            );
            processing_mutex.unlock(lock);

            response.status(500).send(err);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['collision_processing']} in outer catch...`
        );
        processing_mutex.unlock(lock);

        response.status(500).send(err);
      }
    }
  );
});

app.all('/searchtest', function(request: Request, response: Response) {
  const docClient: any = new AWS.DynamoDB.DocumentClient();

  let tweet_promises = [];

  log.trace(`Locking processing_mutex for ${MUTEX_KEY['tweet_processing']}...`);

  processing_mutex.lock(
    MUTEX_KEY['tweet_processing'],
    mutex_lock_opts_send_tweets,
    (err, lock) => {
      if (err) {
        handleError(err);
      }

      try {
        log.trace(
          `Locked processing_mutex for ${MUTEX_KEY['tweet_processing']}.`
        );

        if (!request.query.hasOwnProperty('since_id')) {
          handleError(new Error('Error: since_id is required for /searchtest'));
        }

        let twitter_promises: Array<Promise<IGetTweetsResponse>> = [];

        let get_tweets_promise: Promise<IGetTweetsResponse> = GetNewTweets(
          request.query.since_id as string,
          'HowsMyDrivingWA'
        );

        twitter_promises.push(get_tweets_promise);
        // TODO: Move DM's out, wrapped w their own mutex

        // Now wait until processing of both tweets and dms is done.
        get_tweets_promise
          .then(resp => {
            response.set('Cache-Control', 'no-store');
            response.json(resp.tweets);
          })
          .catch((err: Error) => {
            log.trace(
              `Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} in error...`
            );

            processing_mutex.unlock(lock);

            response.status(500).send(err);
          });
      } catch (err) {
        log.trace(
          `Unlocking processing_mutex for ${MUTEX_KEY['tweet_processing']} in outer catch...`
        );

        processing_mutex.unlock(lock);

        response.status(500).send(err);
      }
    }
  );
});

app.all(
  ['/errors', '/error', '/err'],
  (request: Request, response: Response) => {
    try {
      let fileName = path.resolve(`${__dirname}/../log/err.log`);

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
  let Mocha = require('mocha');
  // Instantiate a Mocha instance.
  let mocha = new Mocha();
  let testDir = path.resolve(path.join(__dirname, '../test'));

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

  let test_results = '';
  let failures = false;

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
    let fileName = path.resolve(`${__dirname}/../log/err.log`);

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
      log.info(`Getting tweet id ${request.query.id}...`);

      GetTweetById(request.query.id as string).then(tweet => {
        response.set('Cache-Control', 'no-store');
        response.json(tweet);
      });
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
      state = request.query.state as string;
      plate = request.query.plate as string;
    } else {
      throw new Error('state and plate query string parameters are required.');
    }

    Object.keys(regions).forEach(region_name => {
      regions[region_name]
        .GetCitationsByPlate(plate, state)
        .then((citations: Array<ICitation>) => {
          let body = 'Citations found:\n';

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
    let batch_write_promise: Promise<void>;
    let request_records: Array<any> = [];

    if (tweets.length > 0) {
      tweets.forEach((tweet: ITweet) => {
        const { state, plate } = parseTweet(tweet.full_text);
        let now = Date.now();

        log.debug(`Found ${PrintTweet(tweet)}`);

        let item = {
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
    if (batch_write_promise) {
      batch_write_promise
        .then(() => {
          log.debug(`Wrote ${request_records.length} request records.`);
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
    const docClient = new AWS.DynamoDB.DocumentClient();

    log.debug(`Checking for request records for all regions...`);

    GetRequestRecords()
      .then((request_records: Array<IRequestRecord>) => {
        let request_promises: Array<Promise<void>> = [];
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
          log.debug(
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
              let now = Date.now();
              // TTL is 10 years from now until the records are PROCESSED
              let ttl_expire: number = new Date(now).setFullYear(
                new Date(now).getFullYear() + 10
              );
              let citation: ICitationRecord = {
                id: GetHowsMyDrivingId(),
                citation_type: CitationIds.CitationIDNoPlateFound,
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
              request_promises.push(Promise.resolve());
            } else {
              Object.keys(regions).forEach(region_name => {
                log.debug(
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
                          let now = Date.now();
                          // TTL is 10 years from now until the records are PROCESSED
                          let ttl_expire: number = new Date(now).setFullYear(
                            new Date(now).getFullYear() + 10
                          );

                          let citation_record: ICitationRecord = {
                            id: GetHowsMyDrivingId(),
                            citation_type:
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
                            let now = Date.now();
                            // TTL is 10 years from now until the records are PROCESSED
                            let ttl_expire: number = new Date(now).setFullYear(
                              new Date(now).getFullYear() + 10
                            );

                            let citation_record: CitationRecord = new CitationRecord(
                              citation
                            );

                            Object.assign(citation_record, item);

                            citation_record.id = GetHowsMyDrivingId();
                            citation_record.citation_type =
                              CitationIds.STANDARD_CITATION_ID;
                            citation_record.request_id = item.id;
                            citation_record.region = region_name;
                            citation_record.processing_status = 'UNPROCESSED';
                            citation_record.created = now;
                            citation_record.modified = now;
                            citation_record.ttl_expire = ttl_expire;

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
                        resolve();
                      })
                      .catch((err: Error) => {
                        reject(err);
                      });
                  })
                );
              });
            }
          });

          log.debug(
            `Waiting for ${request_promises.length} region plugin queries to complete...`
          );

          Promise.all(request_promises)
            .then(() => {
              if (citation_records.length > 0) {
                log.debug(`Updating citation records....`);

                // We have all the citations for every request from every region.
                // Now write the citation records for all request/region combinations
                batchWriteWithExponentialBackoff(
                  new AWS.DynamoDB.DocumentClient(),
                  tableNames['Citations'],
                  citation_records
                )
                  .then(() => {
                    let total_citations: number = 0;
                    let msg: string = '';

                    Object.keys(citations_written).forEach(request_id => {
                      Object.keys(citations_written[request_id]).forEach(
                        region_name => {
                          if (region_name === 'Invalid Plate') {
                            msg += `\n  1 citations for request ${request_id} invalid plate in Global region.`;
                            total_citations += 1;
                          } else {
                            msg += `\n  ${citations_written[request_id][region_name]} citations for request ${request_id} ${licenses[request_id]} in ${region_name} region.`;
                            total_citations +=
                              citations_written[request_id][region_name];
                          }
                        }
                      );
                    });

                    msg =
                      `Finished writing ${total_citations} citation records:` +
                      msg;
                    log.info(msg);

                    let request_update_records: Array<any> = [];
                    let now = Date.now();
                    // Now that the record is PROCESSED, TTL is 30 days
                    let ttl_expire: number =
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
                        log.debug(
                          `Set ${request_update_records.length} request records to PROCESSED.`
                        );

                        // resolve the main promise returned from this function
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
                log.debug(`No citation records need to be updated.`);
              }
            })
            .catch((err: Error) => {
              handleError(err);
            });
        } else {
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
    const docClient = new AWS.DynamoDB.DocumentClient();

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
              ] = new Array<ICitationRecord>();
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
          let report_items_by_request: {
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
                      citation.citation_type < CitationIds.MINIMUM_CITATION_ID
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

                      log.debug(
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
                Object.keys(report_items_by_request[request_id]).forEach(
                  region_name => {
                    if (report_items_by_request[request_id][region_name])
                      log.info(
                        ` - ${
                          report_items_by_request[request_id][region_name]
                        } for request ${request_id} license ${
                          licenseByRequest[request_id] === ':'
                            ? 'invalid license'
                            : licenseByRequest[request_id]
                        } for ${region_name} region`
                      );
                  }
                );
              });

              WriteReportItemRecords(
                new AWS.DynamoDB.DocumentClient(),
                report_items
              )
                .then(() => {
                  log.debug(
                    `Wrote ${report_items.length} report item records for all regions.`
                  );

                  // Set the processing status of all the citations
                  let citation_records: any = [];
                  let now = Date.now();
                  // Now that the record is PROCESSED, TTL is 30 days
                  let ttl_expire: number =
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
                          licenseByRequest[request_id] === ':'
                            ? 'invalid license'
                            : licenseByRequest[request_id]
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
                      log.debug(msg);

                      // This is the one success point for all citations being processed.
                      // Every other codepath is a failure of some kind.
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
          resolve();
        }
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function doSendTweetsForRegion(
  report_items: Array<IReportItemRecord>,
  end_processing_status: string
): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    // Get the first report_item to access report_item columns
    let report_item = report_items[0];
    let region_name: string = report_items[0].region;
    let request_id = report_items[0].request_id;
    let tweetCountByRequest: {
      [request_id: string]: { [region_name: string]: number };
    } = {};

    log.debug(
      `Processing ${report_items.length} report items for ${region_name} region...`
    );

    // Build a fake tweet for the request report_item
    const user: ITwitterUser = {
      screen_name: report_item.tweet_user_screen_name,
      id: parseInt(report_item.tweet_user_id, 10),
      id_str: report_item.tweet_user_id_str
    } as ITwitterUser;

    const orig_tweet: ITweet = {
      id: report_item.tweet_id,
      id_str: report_item.tweet_id_str,
      user: user
    } as ITweet;

    log.debug(
      `Posting ${report_items.length} tweets for request ${request_id} ${
        report_item.license ? report_item.license : 'no license'
      } in ${report_item.region} region.`
    );

    SendTweets(
      region_name,
      orig_tweet,
      report_items.map(({ tweet_text }) => tweet_text)
    )
      .then(tweets_sent_count => {
        log.debug(
          `Finished sending ${report_items.length} tweets for ${region_name} region. tweets_sent_count: ${tweets_sent_count}.`
        );

        UpdateReportItemRecords(report_items, end_processing_status)
          .then(() => {
            resolve(tweets_sent_count);
          })
          .catch(err => {
            reject(err);
          });
      })
      .catch((err: Error) => {
        if (err.name === 'DuplicateError') {
          log.debug(`SendTweets Debugging: This is a DuplicateError error.`);
          if (
            !report_item.hasOwnProperty('tweet_retry_count') ||
            CompareNumericStrings(report_item.tweet_retry_count, '20') < 0
          ) {
            log.debug(
              `Duplicate tweet attempt that can be retried for request ${request_id} region ${region_name}.`
            );

            log.debug(
              `Updating ${report_items.length} report item records for request ${request_id} region ${region_name} to RETRY`
            );

            // This means Twitter detected a tweet as a duplicate in a way
            // that we can retry sending this thread of tweets later.
            UpdateReportItemRecords(report_items, 'RETRY')
              .then(() => {
                resolve(0);
              })
              .catch(err => {
                reject(err);
              });
          } else {
            log.warn(
              `Tweets for request ${request_id} region ${region_name} were retried the max number of times. Pretending they succeeded.`
            );

            log.debug(
              `Updating ${report_items.length} report item records for request ${request_id} region ${region_name} to ${end_processing_status}`
            );

            UpdateReportItemRecords(report_items, end_processing_status)
              .then(() => {
                log.debug(
                  `Updated ${report_items.length} report item records for request ${request_id} region ${region_name} to ${end_processing_status}`
                );

                resolve(0);
              })
              .catch(err => {
                handleError(err);
              });
          }
        } else {
          handleError(err);
        }
      });
  });
}

type processReportItemCallback = (
  p1: Array<IReportItemRecord>,
  p2: string
) => Promise<number>;

function processReportItemRecordsNew(
  status: string,
  end_status: string,
  process_cb: processReportItemCallback
): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    let request_promises: Array<Promise<number>> = [];

    GetReportItemRecords(status)
      .then((report_items: Array<IReportItemRecord>) => {
        let reportitem_count = report_items.length;
        let tweetCountByRequest: {
          [request_id: string]: { [region_name: string]: number };
        } = {};

        if (report_items && report_items.length > 0) {
          log.debug(
            `Processing ${report_items.length} report items for all regions...`
          );
          let reportItemsByRequest: {
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

          let now = Date.now();

          // Now process the report_items, on a per-request/region basis
          Object.keys(reportItemsByRequest).forEach((request_id: string) => {
            Object.keys(reportItemsByRequest[request_id]).forEach(
              region_name => {
                // For report items that are retries, only retry them every 4 hours
                let rep_item = reportItemsByRequest[request_id][region_name][0];

                if (
                  CompareNumericStrings(rep_item.tweet_retry_count, '0') > 0 &&
                  now - rep_item.modified <
                    parseInt(process.env.TWEET_RETRY_DELAY_HOURS) *
                      60 *
                      60 *
                      1000
                ) {
                  log.debug(
                    `Skipping report items for request ${request_id} region ${region_name}. Will retry after ${
                      process.env.TWEET_RETRY_DELAY_HOURS
                    }h, last attempt was ${FormatMilliseconds(
                      now - rep_item.modified
                    )} ago.`
                  );

                  reportItemsByRequest[request_id][region_name] = [];
                } else {
                  request_promises.push(
                    new Promise<number>((resolve, reject) => {
                      log.trace(
                        `Locking processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name}...`
                      );

                      processing_mutex.lock(
                        MUTEX_KEY['tweet_sending'],
                        mutex_lock_opts_send_tweets,
                        (err, lock) => {
                          if (err) {
                            reject(err);
                          }

                          try {
                            log.trace(
                              `Locked processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name}.`
                            );

                            process_cb(
                              reportItemsByRequest[request_id][region_name],
                              end_status
                            )
                              .then(changes_made => {
                                log.trace(
                                  `Promise resolved for call to process_cb from status ${status} to ${end_status}.`
                                );

                                log.trace(
                                  `Unlocking processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name}...`
                                );
                                processing_mutex.unlock(lock);

                                resolve(changes_made);
                              })
                              .catch(process_cb_err => {
                                log.trace(
                                  `Unlocking processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name} in error...`
                                );
                                processing_mutex.unlock(lock);

                                reject(err);
                              });
                          } catch (error) {
                            log.trace(
                              `Unlocking processing_mutex for ${MUTEX_KEY['tweet_sending']} for request ${request_id} region ${region_name} in outer catch...`
                            );
                            processing_mutex.unlock(lock);

                            reject(error);
                          }
                        }
                      );
                    })
                  );
                }
              }
            );
          });

          log.debug(
            `Waiting for ${request_promises.length} region plugin processes to complete...`
          );

          Promise.all(request_promises)
            .then(tweet_send_counts => {
              log.debug(
                `All ${
                  request_promises.length
                } request promises have all resolved returning: ${DumpObject(
                  tweet_send_counts
                )}`
              );

              let total_tweets_sent: number = tweet_send_counts.reduce(
                (a, b) => a + b,
                0
              );

              log.debug(`Total tweets sent: ${total_tweets_sent}.`);

              if (request_promises.length > 0) {
                // Report how many tweets we sent for each region.
                let msg: string = '';

                Object.keys(tweetCountByRequest).forEach(request_id => {
                  Object.keys(tweetCountByRequest[request_id]).forEach(
                    region_name => {
                      total_tweets_sent +=
                        tweetCountByRequest[request_id][region_name];

                      msg += `\n - ${tweetCountByRequest[request_id][region_name]} tweets for request ${request_id} ${reportItemsByRequest[request_id][region_name][0].license} in ${region_name} region for ${reportItemsByRequest[request_id][region_name].length} report items`;
                    }
                  );
                });

                msg =
                  `${
                    total_tweets_sent == 0 ? 'Zero' : total_tweets_sent
                  } tweets sent for ${report_items.length} report item records${
                    total_tweets_sent == 0 ? '.' : ': '
                  }.` + msg;

                log.debug(msg);
              } else {
                log.debug(
                  `No tweets were sent. No report item records need updating.`
                );
              }

              resolve(total_tweets_sent);
            })
            .catch((err: Error) => {
              throw err;
            });
        } else {
          log.debug('No report items found for any region.');
          resolve(0);
        }
      })
      .catch((err: Error) => {
        throw err;
      });
  });
}

function checkForCollisions(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const docClient: any = new AWS.DynamoDB.DocumentClient();

    // Query the current collision data
    let collision_promises: {
      [key: string]: Promise<Array<ICollision>>;
    } = {};
    Object.keys(regions).forEach(region_name => {
      log.debug(`Getting collisions for '${region_name}'...`);

      collision_promises[region_name] = regions[region_name]
        .GetRecentCollisions()
        .then((collisions: Array<ICollision>) => {
          log.debug(
            `Region ${region_name} returned ${collisions.length} collisions.`
          );
          return collisions;
        });
    });

    let collisions_returned: Array<ICollision> = [];

    // Query our store for unprocessed collisions
    log.debug(`Getting existing collision records...`);

    GetCollisionRecords()
      .then((existing_collision_records: Array<ICollisionRecord>) => {
        let existing_collision_records_by_region_id: {
          [key: string]: { [key: string]: ICollision };
        } = {};

        log.debug(
          `Retrieved ${existing_collision_records.length} existing collision records.`
        );

        existing_collision_records.forEach(collision => {
          if (!existing_collision_records_by_region_id[collision.region]) {
            existing_collision_records_by_region_id[collision.region] = {};
          }

          existing_collision_records_by_region_id[collision.region][
            collision.id
          ] = collision;
        });

        Promise.all(Object.values(collision_promises))
          .then(collisions_arrays => {
            // We will write all of the records returned by the region plugins
            // plus any existing records we have in our store which are not
            // returned by the region plugins (i.e. they are no longer a most
            // recent collision of interest)
            let collision_records_to_write: Array<any> = [];

            let collision_records_by_region_id: {
              [key: string]: { [key: string]: ICollision };
            } = {};

            let region_index: number = 0;

            collisions_arrays.forEach(
              (region_collisions: Array<ICollision>) => {
                let region_name = Object.keys(regions)[region_index];

                region_collisions.forEach((collision: ICollision) => {
                  if (!collision_records_by_region_id[region_name]) {
                    collision_records_by_region_id[region_name] = {};
                  }

                  collision_records_by_region_id[region_name][
                    collision.id
                  ] = collision;

                  let collision_record = new CollisionRecord(
                    collision,
                    region_name
                  );

                  if (
                    existing_collision_records[region_name] &&
                    existing_collision_records[region_name][collision.id]
                  ) {
                    // This collision already exists in our store.
                    // Update it but keep the created date from existing record
                    collision_record.created =
                      existing_collision_records[region_name][
                        collision.id
                      ].created;
                  }
                  let record = {
                    PutRequest: {
                      Item: collision_record
                    }
                  };

                  log.trace(
                    `Adding collision record id: ${collision_record.id} region: ${collision_record.region}.`
                  );

                  collision_records_to_write.push(record);
                });

                region_index += 1;
              }
            );

            // Any collision that is in our store but was not returned
            // by the region plugins, should be marked PROCESSED since
            // it is no longer a most recent collision
            let now = Date.now();
            // Now that the record is PROCESSED, TTL is 30 days
            let ttl_expire: number =
              new Date().getTime() + 30 * 24 * 60 * 60 * 1000;
            let processed_count: number = 0;

            existing_collision_records.forEach(
              (collision_record: ICollisionRecord) => {
                if (
                  !collision_records_by_region_id[collision_record.region] ||
                  !collision_records_by_region_id[collision_record.region][
                    collision_record.id
                  ]
                ) {
                  collision_record.processing_status = 'PROCESSED';
                  collision_record.modified = now;
                  collision_record.ttl_expire = ttl_expire;

                  let record = {
                    PutRequest: {
                      Item: collision_record
                    }
                  };

                  log.trace(
                    `Adding existing collision record setting to PROCESSED id: ${collision_record.id} region: ${collision_record.region}.`
                  );

                  collision_records_to_write.push(record);

                  processed_count += 1;
                }
              }
            );

            let batch_write_promise: Promise<void>;

            if (collision_records_to_write.length > 0) {
              batch_write_promise = batchWriteWithExponentialBackoff(
                docClient,
                tableNames['Collisions'],
                collision_records_to_write
              );
            }

            if (batch_write_promise) {
              batch_write_promise
                .then(() => {
                  log.debug(
                    `Wrote ${collision_records_to_write.length} collision records, setting ${processed_count} to PROCESSED.`
                  );

                  resolve();
                })
                .catch(err => {
                  handleError(err);
                });
            } else {
              resolve();
            }
          })
          .catch((err: Error) => {
            handleError(err);
          });
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function processCollisionRecords(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    GetCollisionRecords()
      .then((collisions: Array<ICollision>) => {
        log.trace(`Retrieved ${collisions.length} collisions.`);
        let collision_promises: { [key: string]: Promise<Array<string>> } = {};

        if (collisions && collisions.length > 0) {
          let collisionsByRegion: {
            [region_name: string]: Array<ICollisionRecord>;
          } = {};

          log.debug(
            `Processing ${collisions.length} collision records for all regions...`
          );

          // Break them up by region
          collisions.forEach((collision: ICollisionRecord) => {
            if (!Object.keys(collisionsByRegion).includes(collision.region)) {
              log.trace(
                `Adding region ${collision.region} to collisionsByRegion...`
              );
              collisionsByRegion[collision.region] = new Array<
                ICollisionRecord
              >();
            }

            log.debug(`Found ${collision.region} collision ${collision.id}`);

            collisionsByRegion[collision.region].push(collision);
          });

          log.info(`active_regions: ${DumpObject(active_regions)}`);

          Object.keys(collisionsByRegion).forEach(region_name => {
            if (active_regions.includes(region_name)) {
              collision_promises[
                region_name
              ] = processCollisionRecordsForRegion(
                collisionsByRegion[region_name],
                region_name
              );
            } else {
              log.warn(
                `Found collision for region ${region_name} which is not currently active.`
              );
            }
          });

          log.debug(
            `Waiting for all ${
              Object.keys(collision_promises).length
            } collision_promises to be resolved.`
          );

          let report_items: Array<IReportItemRecord> = [];

          Promise.all(Object.values(collision_promises))
            .then((tweets: Array<Array<string>>) => {
              let idx: number = 0;

              log.debug(
                `All ${
                  Object.keys(collision_promises).length
                } collision_promises have resolved.`
              );

              tweets.forEach(inner_tweets => {
                let region_name = Object.keys(collision_promises)[idx];

                log.debug(
                  `Region ${region_name} returned ${inner_tweets.length} tweets.`
                );

                inner_tweets.forEach((tweet: string) => {
                  let report_item_record: IReportItemRecord = new ReportItemRecord(
                    tweet,
                    0
                  );

                  report_item_record.region = region_name;

                  // Create a fake request ID to ensure each of these report records
                  // are considered independent and not posted in a thread.
                  report_item_record.request_id = GetHowsMyDrivingId();

                  report_items.push(report_item_record);
                });

                idx = idx + 1;
              });

              if (report_items.length > 0) {
                WriteReportItemRecords(
                  new AWS.DynamoDB.DocumentClient(),
                  report_items
                )
                  .then(() => {
                    log.debug(
                      `Wrote ${report_items.length} report item records for all regions.`
                    );

                    resolve();
                  })
                  .catch((e: Error) => {
                    handleError(e);
                  });
              } else {
                resolve();
              }
            })
            .catch(err => {
              handleError(err);
            });
        } else {
          resolve();
        }
      })
      .catch((err: Error) => {
        handleError(err);
      });
  });
}

function processCollisionRecordsForRegion(
  collisions: Array<ICollisionRecord>,
  region_name: string
): Promise<Array<string>> {
  return new Promise<Array<string>>((resolve, reject) => {
    log.debug(
      `Processing ${collisions.length} collisions for ${region_name}...`
    );

    regions[region_name].ProcessCollisions(collisions).then(tweets => {
      let tweet_promises: Array<Promise<string>> = [];
      let processed_tweets: Array<string> = [];

      log.debug(
        `Processing tweets looking for media for ${region_name} region.`
      );

      // Check if the tweets include any media items that need to be uploaded
      tweets.forEach(tweet => {
        // Check if there are images included with this tweet
        let parts = tweet.split(__HOWSMYDRIVING_DELIMITER__);

        if (parts.length == 2) {
          let media_items: Array<IMediaItem> = MediaItemsFromString(parts[1]);
          let upload_media_item_promises: Array<Promise<IMediaItem>> = [];

          media_items.forEach(async media_item => {
            if (media_item.twitter_media_id_str) {
              log.trace(
                `Found reference to existing twitter media id: ${media_item.twitter_media_id_str}`
              );
              upload_media_item_promises.push(Promise.resolve(media_item));
            } else if (media_item.url) {
              // This is an image that needs to be uploaded to Twitter
              log.trace(
                `Getting media id for url ${media_item.url} alt_text ${media_item.alt_text}...`
              );

              upload_media_item_promises.push(
                GetMediaItem(region_name, media_item.url, media_item.alt_text)
              );
            }
          });

          tweet_promises.push(
            new Promise<string>((resolve, reject) => {
              Promise.all(upload_media_item_promises)
                .then(media_items => {
                  // Media items for this tweet have been uploaded and cahced,
                  // build the tweet with the updated media items.
                  let result_tweet: string = `${
                    parts[0]
                  }${__HOWSMYDRIVING_DELIMITER__}${JSON.stringify(
                    media_items
                  )}`;
                  resolve(result_tweet);
                })
                .catch(err => {
                  handleError(err);
                });
            })
          );
        } else {
          tweet_promises.push(Promise.resolve(tweet));
        }
      });

      log.debug(`Waiting for ${tweet_promises.length} tweet promises...`);

      Promise.all(tweet_promises).then(processed_tweets => {
        log.debug(`All ${tweet_promises.length} tweet promises resolved.`);
        log.debug(`Resolving ${tweets.length} tweets for ${region_name}...`);

        resolve(processed_tweets);
      });
    });
  });
}

async function GetMediaItem(
  region_name: string,
  image_url: string,
  alt_text: string
): Promise<IMediaItem> {
  return new Promise<IMediaItem>((resolve, reject) => {
    log.debug(
      `Getting media item for url ${image_url} alt_text ${alt_text} in region ${region_name}...`
    );

    // Check if we have saved this media item before
    GetCachedMediaItem(region_name, image_url, alt_text)
      .then(media_item => {
        if (!media_item) {
          // Upload the media item
          log.debug(
            `Uploading media item url: ${image_url} alt_text: ${alt_text}...`
          );

          UploadMedia(region_name, image_url, alt_text)
            .then(media_item => {
              log.debug(
                `Uploaded media item url: ${image_url} alt_text: ${alt_text}: ${media_item.twitter_media_id_str}.`
              );

              // Save the media id in the cache
              UpdateCachedMediaItems([media_item], region_name)
                .then(media_item_record => {
                  resolve(media_item);
                })
                .catch(err => {
                  handleError(err);
                });
            })
            .catch(err => {
              handleError(err);
            });
        } else {
          log.debug(
            `Retrieved chached media item for url: ${image_url} alt_text: ${alt_text}: ${media_item.twitter_media_id_str}`
          );

          resolve(media_item);
        }
      })
      .catch(err => {
        handleError(err);
      });
  });
}

function GetCachedMediaItem(
  region_name: string,
  image_url: string,
  alt_text: string
): Promise<IMediaItem> {
  return new Promise<IMediaItem>((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();

    log.debug(
      `Getting cached media region: ${region_name} url: ${image_url} alt_text: ${alt_text}...`
    );

    // Query unprocessed requests
    let params = {
      TableName: tableNames['MediaItems'],
      Select: 'ALL_ATTRIBUTES',
      KeyConditionExpression: '#url = :url AND #alt_text= :alt_text',
      ExpressionAttributeNames: {
        '#url': 'url',
        '#alt_text': 'alt_text'
      },
      ExpressionAttributeValues: {
        ':url': image_url,
        ':alt_text': alt_text
      }
    };

    log.debug(
      `Querying for media item records url: ${image_url} alt_text: ${alt_text}...`
    );

    let media_item_record: IMediaItemRecord;
    docClient.query(params, async function(err, result) {
      if (err) {
        reject(err);
      }

      if (result.Items.length > 1) {
        throw new Error(
          `Multiple media items found with url '${image_url}' alt_text '${alt_text}', ${DumpObject(
            result.Items
          )}`
        );
      }

      if (result.Items.length == 1) {
        media_item_record = result.Items[0] as IMediaItemRecord;
      } else {
        log.debug(
          `No cached media item record found for url: ${image_url} alt_text: ${alt_text}.`
        );
      }

      resolve(media_item_record);
    });
  });
}

// TODO: Move this out to a db utility file
export function batchWriteWithExponentialBackoff(
  docClient: AWS.DynamoDB.DocumentClient,
  table: string,
  records: Array<object>
): Promise<void> {
  return new Promise((resolve, reject) => {
    log.trace(
      `Entering batchWriteWithExponentialBackoff with ${records.length} records for table ${table}...`
    );
    let qdb = docClient ? Q(docClient) : Q();
    qdb.drain = function() {
      log.trace(
        `in drain batchWriteWithExponentialBackoff for table ${table}...`
      );
      resolve();
    };

    qdb.error = function(err: Error, task: any) {
      log.trace(
        `in error batchWriteWithExponentialBackoff for table ${table}...`
      );
      reject(err);
    };

    let startPos: number = 0;
    let endPos: number;
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
  if (!citation || citation.citation_type >= CitationIds.MINIMUM_CITATION_ID) {
    throw new Error(
      `ERROR: Unexpected citation ID: ${citation.citation_type}.`
    );
  }
  switch (citation.citation_type) {
    case CitationIds.CitationIDNoPlateFound:
      // We need to ensure that we never create two of these 'no plate found'
      // messages that are identical or Twitter will prevent the second one
      // from being tweeted with a 187 error.
      // Add the full date/time and a process-wide unique increasing number at
      // the end. The increasing number resets to zero every time the process
      // restarts but as long as there is at least 1ms between each generation
      // of these messages, uniqueness will be ensured.
      let now = new Date();
      return noValidPlate.replace('__USER__', citation.tweet_user_screen_name);
      break;

    case CitationIds.CitationIDNoCitationsFound:
      return (
        noCitationsFoundMessage
          .replace('__LICENSE__', formatPlate(citation.license))
          .replace('__REGION_NAME__', region_name) +
        '\n\n' +
        citationQueryText
          .replace('__LICENSE__', formatPlate(citation.license))
          .replace('__COUNT__', query_count.toString())
      );
      break;

    default:
      throw new Error(
        `ERROR: Unexpected citation ID: ${citation.citation_type}.`
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
  let chomped = false;
  let text = tweet.full_text;

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
  let state: string;
  let plate: string;
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
        let val = o[p];

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
  const docClient = new AWS.DynamoDB.DocumentClient();

  // Query unprocessed requests
  let params = {
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
            reject(err);
          }

          let query_count: number = result.Count;

          resolve({ license, query_count });
        }
      );
    }
  );
}

function getLastDmId(): Promise<string> {
  return getStateValue('last_dm_id');
}

function getLastMentionId(): Promise<string> {
  return getStateValue('last_mention_id');
}

function getStateValue(keyname: string): Promise<string> {
  const docClient: any = new AWS.DynamoDB.DocumentClient();

  let params = {
    TableName: tableNames['State'],
    Key: {
      keyname: keyname
    }
  };

  return new Promise<string>((resolve, reject) => {
    let ret: string;
    docClient.get(params, async (err, result) => {
      if (err) {
        handleError(err);
      }

      if (!result.Item || !result.Item['keyname']) {
        log.warn(`getStateValue: State value of '${keyname}' not found.`);
      } else {
        ret = result.Item['keyvalue'].toString();
      }

      resolve(ret);
    });
  });
}

function putStateValue(keyname: string, keyvalue: string) {
  const docClient: any = new AWS.DynamoDB.DocumentClient();

  let params = {
    TableName: tableNames['State'],
    Item: {
      keyname: keyname,
      keyvalue: keyvalue
    }
  };

  return new Promise<void>((resolve, reject) => {
    docClient.put(params, async (err, result) => {
      if (err) {
        handleError(err);
      }

      resolve();
    });
  });
}

function setLastDmId(last_dm_id: string) {
  lastdmLog.info(`Writing last dm id ${last_dm_id}.`);
  return putStateValue('last_dm_id', last_dm_id);
}

async function setLastMentionId(last_mention_id: string): Promise<void> {
  lastdmLog.info(`Writing last mention id ${last_mention_id}.`);
  return putStateValue('last_mention_id', last_mention_id);
}

export function handleError(error: Error): void {
  // Truncate the callstack because only the first few lines are relevant to this code.
  let stacktrace = '';

  if (error.stack) {
    error.stack
      .split('\n')
      .slice(0, 10)
      .join('\n');
  }
  let formattedError = `===============================================================================\n${error.message}\n${stacktrace}`;

  log.error(formattedError);
  throw error;
}

// asynchronous query function to fetch all unprocessed request records.
// returns: promise
function GetRequestRecords(): Promise<Array<IRequestRecord>> {
  const docClient = new AWS.DynamoDB.DocumentClient();

  let request_records: Array<IRequestRecord> = [];

  // Query unprocessed requests
  let params = {
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
        reject(err);
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
  const docClient = new AWS.DynamoDB.DocumentClient();
  let citation_records: Array<ICitationRecord> = [];

  // Query unprocessed citations
  let params = {
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
        let requestIDs: { [key: string]: number } = {};

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
          let requestIndex: number = 0;

          while (
            citation_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            let requestID = requestIDs[requestIndex];
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
                reject(err);
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

function GetReportItemRecords(
  status: string
): Promise<Array<IReportItemRecord>> {
  const docClient = new AWS.DynamoDB.DocumentClient();

  let report_item_records: Array<IReportItemRecord> = [];

  let name = `':${status}'`;
  let params = {
    TableName: tableNames['ReportItems'],
    IndexName: 'processing_status-index',
    Select: 'ALL_ATTRIBUTES',
    KeyConditionExpression: `processing_status = :status`,
    ExpressionAttributeValues: {
      ':status': status
    },
    Limit: MAX_RECORDS_BATCH // If more than this, we'll have to handle it below
  };

  return new Promise<Array<IReportItemRecord>>((resolve, reject) => {
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
        let requestIDs: { [key: string]: number } = {};

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
          let requestIndex = 0;

          while (
            report_item_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            let requestID = requestIDs[requestIndex];
            requestIndex++;

            // 3a. Query for the report items for this request_id
            // Use the index which includes request_id.
            params.IndexName = 'request_id-processing_status-index';
            params['KeyConditionExpression'] = `request_id = ${requestID}`;
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

// asynchronous query function to fetch all collision records.
// returns: promise
function GetCollisionRecords(): Promise<Array<ICollisionRecord>> {
  const docClient = new AWS.DynamoDB.DocumentClient();
  let collision_records: Array<ICollisionRecord> = [];

  log.info(`Getting collision records...`);

  // Query unprocessed collisions
  let params = {
    TableName: tableNames['Collisions'],
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
    try {
      log.trace(`Making query for unprocessed collisions...`);

      docClient.query(params, async (err: Error, result: QueryOutput) => {
        if (err) {
          log.error(
            `Failed to query collision records. Err: ${DumpObject(err)}`
          );

          handleError(err);
        }

        log.trace(`Retrieved ${result.Items.length} collision records.`);

        // 3. Check if we retrieved all the records
        if (
          result.hasOwnProperty('LastEvaluatedKey') &&
          result.LastEvaluatedKey
        ) {
          // Paging!!!!
        } else {
          // TODO: There must be a type-safe way to do this...
          let collision_records_batch: any = result.Items;
          collision_records = collision_records_batch as Array<
            ICollisionRecord
          >;
        }

        log.debug(`Resolving ${collision_records.length} collision records.`);

        resolve(collision_records);
      });
    } catch (err) {
      log.error(
        `Caught exception trying to query collision records. Err: ${DumpObject(
          err
        )}`
      );

      reject(err);
    }
  });
}

function WriteReportItemRecords(
  docClient: AWS.DynamoDB.DocumentClient,
  report_items: Array<IReportItemRecord>
) {
  let report_item_records: Array<object> = [];

  report_items.forEach((report_item, index) => {
    let item = {
      PutRequest: {
        Item: report_item
      }
    };

    report_item_records.push(item);
  });

  log.trace(
    `Calling batchWriteWithExponentialBackoff with ${report_item_records.length} report item records.`
  );

  // Write the report item records, returning that Promise.
  return batchWriteWithExponentialBackoff(
    docClient,
    tableNames['ReportItems'],
    report_item_records
  );
}

function UpdateReportItemRecords(
  report_items: Array<IReportItemRecord>,
  status: string
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let now = Date.now();
    let report_item_records: Array<any> = [];

    report_items.forEach(report_item => {
      report_item.modified = now;
      if (status == 'PROCESSED') {
        // Now that the record is PROCESSED, TTL is 30 days
        let ttl_expire: number =
          new Date().getTime() + 30 * 24 * 60 * 60 * 1000;

        report_item.processing_status = 'PROCESSED';
        report_item.ttl_expire = ttl_expire;

        if (
          !report_item.hasOwnProperty('tweet_retry_count') ||
          !report_item.tweet_retry_count
        ) {
          report_item.tweet_retry_count = '0';
        }
      } else if (status == 'RETRY') {
        report_item.processing_status = 'UNPROCESSED';

        if (report_item.hasOwnProperty('tweet_retry_count')) {
          report_item.tweet_retry_count = (
            parseInt(report_item.tweet_retry_count, 10) + 1
          ).toString();
        } else {
          report_item.tweet_retry_count = '1';
        }
      } else {
        report_item.processing_status = status;
      }

      report_item_records.push({
        PutRequest: {
          Item: report_item
        }
      });
    });

    if (report_item_records.length > 0) {
      batchWriteWithExponentialBackoff(
        new AWS.DynamoDB.DocumentClient(),
        tableNames['ReportItems'],
        report_item_records
      )
        .then(() => {
          // This is the one and only success point for these report item records.
          // Every other codepath is an error of some kind.
          log.debug(
            `Updated ${report_item_records.length} report items to ${status} for request ${report_items[0].request_id} region ${report_items[0].region}.`
          );

          // Tweets for all the requests completed successfully and report item records updated.
          resolve();
        })
        .catch((err: Error) => {
          reject(err);
        });
    } else {
      resolve();
      log.debug(`No report items to update to ${status}.`);
    }
  });
}

function UpdateCachedMediaItems(
  media_items: Array<IMediaItem>,
  region_name: string
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let media_item_records: Array<any> = [];

    log.debug(
      `Updating ${media_items.length} media item records for ${region_name} region...`
    );

    media_items.forEach(media_item => {
      let media_item_record = new MediaItemRecord(media_item);

      media_item_records.push({
        PutRequest: {
          Item: media_item_record
        }
      });
    });

    if (media_item_records.length > 0) {
      batchWriteWithExponentialBackoff(
        new AWS.DynamoDB.DocumentClient(),
        tableNames['MediaItems'],
        media_item_records
      )
        .then(() => {
          // Tweets for all the requests completed successfully and media item records updated.
          resolve();
        })
        .catch((err: Error) => {
          reject(err);
        });
    } else {
      log.debug(`No media items to update.`);
    }
  });
}
