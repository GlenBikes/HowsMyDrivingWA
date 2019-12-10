/* Setting things up. */
import * as AWS from 'aws-sdk';
import {DocumentClient, QueryOutput} from 'aws-sdk/clients/dynamodb';
import {Request, Response} from 'express';
import * as http from 'http';
import {LMXClient, LMXBroker, Client, Broker} from 'live-mutex';
import * as Twit from 'twit';

// howsmydriving-utils
import {Citation} from 'howsmydriving-utils';
import {CitationIds} from 'howsmydriving-utils';
import {IRegion} from 'howsmydriving-utils';
import {ICitation} from 'howsmydriving-utils';
import {CompareNumericStrings} from 'howsmydriving-utils';
import {SplitLongLines} from 'howsmydriving-utils';
import {PrintTweet} from 'howsmydriving-utils';

// howsmydriving-seattle
// TODO: Put the configuration of regions in .env
import {SeattleRegion} from 'howsmydriving-seattle';

// interfaces internal to project
import {IRequestRecord} from './interfaces';
import {IReportItemRecord} from './interfaces';
import {ICitationRecord} from './interfaces';
import {CitationRecord} from './interfaces';
import {StatesAndProvinces, formatPlate} from './interfaces';
import {GetHowsMyDrivingId, DumpObject} from './interfaces';

import {log, lastdmLog, lastmentionLog} from './logging';

// legacy commonjs modules
const express = require("express"),
  fs = require("fs"),
  LocalStorage = require('node-localstorage').LocalStorage,
  path = require("path"),
  Q = require('dynamo-batchwrite-queue'),
  soap = require("soap");

const noCitationsFoundMessage = "No citations found for plate #",
  noValidPlate = "No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #",
  citationQueryText = "License #__LICENSE__ has been queried __COUNT__ times.";

// TODO: Put the regions in .env file
let seattle: SeattleRegion = new SeattleRegion();

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
    

const
  MAX_RECORDS_BATCH = 2000, 
  INTER_TWEET_DELAY_MS =
    process.env.hasOwnProperty("INTER_TWEET_DELAY_MS") &&
    CompareNumericStrings(process.env.INTER_TWEET_DELAY_MS, "0") < 0
      ? parseInt(process.env.INTER_TWEET_DELAY_MS, 10)
      : 5000;

export const tableNames: { [tabletype: string] : string; } = {
    Request: `${process.env.DB_PREFIX}_Request`,
    Citations: `${process.env.DB_PREFIX}_Citations`,
    ReportItems: `${process.env.DB_PREFIX}_ReportItems`
  };

log.info(`${process.env.TWITTER_HANDLE}: start`);

AWS.config.update({ region: "us-east-2" });

const maxTweetLength: number = 280 - 17; // Max username is 15 chars + '@' plus the space after the full username
const noCitations: string = "No citations found for plate # ";
const parkingAndCameraViolationsText: string =
  "Total parking and camera violations for #";
const violationsByYearText: string = "Violations by year for #";
const violationsByStatusText: string = "Violations by status for #";
const licenseQueriedCountText: string =
  "License __LICENSE__ has been queried __COUNT__ times.";
const licenseRegExp: RegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;
const botScreenNameRegexp: RegExp = new RegExp(
  "@" + process.env.TWITTER_HANDLE + "\\b",
  "i"
);

app.use(express.static("public"));

var listener = app.listen(process.env.PORT, function() {
  log.info(`Your bot is running on port ${listener.address().port}`);
});

// One global broker for the live-mutex clients.
let mutex_broker = new Broker( {} );

mutex_broker.emitter.on('warning', function () {
  log.warn(...arguments);
});

mutex_broker.emitter.on('error', function () {
  log.error(...arguments);
});

mutex_broker.ensure().then( () => {
  log.info(`Successfully created mutex broker.`);
});

/* tracks the largest tweet ID retweeted - they are not processed in order, due to parallelization  */
/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all("/tweet", function(request: Request, response: Response) {
  
  const T: Twit = new Twit(config.twitter);
  var docClient: any = new AWS.DynamoDB.DocumentClient();
  
  // We need the bot's app id to detect tweets from the bot
  getAccountID(T).then( ( app_id: number ) => {
    try {
      var twitter_promises: Array<Promise<void>> = [];
      let tweet_process_promise: Promise<void> = processNewTweets(T, docClient, app_id);
      var dm_process_promise = processNewDMs();

      twitter_promises.push(tweet_process_promise);
      twitter_promises.push(dm_process_promise);

      // Now wait until processing of both tweets and dms is done.
      Promise.all(twitter_promises).then( () => {
        response.sendStatus(200);
      }).catch( (err) => {
        response.status(500).send(err);
      });
    } catch ( err ) {
      response.status(500).send(err);
    }
  }).catch( (err: Error) => {
    handleError(err);
  });
});

app.all("/test", function(request: Request, response: Response) {
  // Doing the require here will cause the tests to rerun every
  // time the /test url is loaded even if no test or product
  // code has changed.
  var Mocha = require("mocha");

  // Instantiate a Mocha instance.
  var mocha = new Mocha();

  var testDir = "./test";

  // Add each .js file to the mocha instance
  fs.readdirSync(testDir)
    .filter( (file: string) => {
      // Only keep the .js files
      return file.substr(-3) === ".js";
    })
    .forEach( (file: string) => {
      mocha.addFile(path.join(testDir, file));
    });

  var test_results = '';
  var failures = false;

  // Run the tests.
  mocha
    .run()
    .on("test", (test: any) => {
      test_results += `Test started: ${test.title}\n`;
    })
    .on("test end", (test: any) => {
      //test_results += `Test done: ${test.title}\n`;
    })
    .on("pass", (test: any) => {
      test_results += `Test passed: ${test.title}\n\n`;
    })
    .on("fail", (test: any, err: Error) => {
      test_results += `Test failed: ${test.title}\nError:\n${err}\n\n`;
      failures = true;
    })
    .on("end", () => {
      test_results +=
        "*********************************************\n\nTests all finished!\n\n********************************************";
      // send your email here
      if (!failures) {
        response.sendStatus(200);
      } else {
        response.status(500).send(test_results);
      }
    });
});

app.all("/dumpfile", (request: Request, response: Response) => {
  try {
    log.info("dumpfile");
    var fileName = path.resolve(`${__dirname}/../log/err.log`);

    if (request.query.hasOwnProperty("filename")) {
      fileName = path.resolve(`${__dirname}/../${request.query.filename}`);
    }
    log.info(`Sending file: ${fileName}.`);
    response.sendFile(fileName);
  } catch( err ) {
    response.status(500).send(err);
  }
});

app.all("/dumptweet", (request: Request, response: Response) => {
  try {
    if (request.query.hasOwnProperty("id")) {
      const T = new Twit(config.twitter);
      var tweet = getTweetById(T, request.query.id);
      response.set("Cache-Control", "no-store");
      response.json(tweet);
    } else {
      handleError(new Error("Error: id is required for /dumptweet"));
    }
  } catch ( err ) {
    response.status(500).send(err);
  }
});

app.all("/dumpcitations", (request: Request, response: Response) => {
  try {
  let state: string;
  let plate: string;
  if (
    request.query.hasOwnProperty("state") &&
    request.query.hasOwnProperty("plate")
  ) {
    state = request.query.state;
    plate = request.query.plate;
  }
  else {
    throw new Error("state and plate query string parameters are required.")
  }

  seattle
    .GetCitationsByPlate(plate, state)
    .then( (citations: Array<ICitation>) => {
      var body = "Citations found:\n";

      if (!citations || citations.length == 0) {
        response.send(noCitations);
      } else {
        // TODO: Can we just send the whole array as json?
        response.json(citations);
      }
    })
    .catch( (err) => {
      handleError(err);
    });
  }
  catch ( err ) {
    response.status(500).send(err);
  }
});

app.all(["/errors", "/error", "/err"], (request: Request, response: Response) => {
  try {
    log.info("errors");
    var fileName = path.resolve(`${__dirname}/../log/err.log`);

    log.info(`Sending file: ${fileName}.`);
    response.sendFile(fileName);
  }
  catch ( err ) {
    response.status(500).send(err);
  }
});

// uptimerobot.com hits this every 5 minutes
app.all("/processrequests", (request: Request, response: Response) => {
  try {
    var docClient = new AWS.DynamoDB.DocumentClient();

    log.debug(`Checking for request records...`);
    
    GetRequestRecords()
      .then( (request_records: Array<IRequestRecord>) => {
        log.info(`Processing ${request_records.length} request records...`);

        // DynamoDB does not allow any property to be null or empty string.
        // Set these values to 'None' or a default number.
        const column_overrides: { [ key: string ]: any } = {
          "CaseNumber": -1,
          "ChargeDocNumber": "None",
          "Citation": "None",
          "CollectionsStatus": "None",
          "FilingDate": "None",
          "InCollections": "false",
          "Status": "None",
          "Type": "None",
          "ViolationDate": "None",
          "ViolationLocation": "None"
        };

      
        var request_promises: Array<Promise<void>> = [];
      
        if (request_records && request_records.length > 0) {
          request_records.forEach( (item) => {
            let citation_records: Array<object> = [];
            let tokens: Array<string> = item.license.split(":");
            let state: string;
            let plate: string;

            if (tokens.length == 2) {
              state = tokens[0];
              plate = tokens[1];
            }
            
            var request_promise = new Promise<void>( (resolve, reject) => {
              if (state == null || state == "" || plate == null || plate == "") {
                log.warn(
                  `Not a valid state/plate in this request (${state}/${plate}).`
                );
                // There was no valid plate found in the tweet. Add a dummy citation.
                var now = Date.now();
                // TTL is 10 years from now until the records are PROCESSED
                var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);
                var citation: ICitationRecord = {
                  id: GetHowsMyDrivingId(),
                  citation_id: CitationIds.CitationIDNoPlateFound,
                  Citation: CitationIds.CitationIDNoPlateFound,
                  processing_status: "UNPROCESSED",
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

                batchWriteWithExponentialBackoff(
                  new AWS.DynamoDB.DocumentClient(),
                  tableNames["Citations"],
                  citation_records
                ).then( () => {
                      log.info(`Finished writing ${citation_records.length} citation records for ${state}:${plate}.`);          

                      var params = {
                        TableName: tableNames["Request"],
                        Key: {
                          id: item.id
                        },
                        AttributeUpdates: {
                          processing_status: {
                            Action: "PUT",
                            Value: "PROCESSED"
                          },
                          modified: {
                            Action: "PUT",
                            Value: Date.now()
                          }
                        }
                      };

                      docClient.update(params, function(err: Error, data: any) {
                        if (err) {
                          handleError(err);
                        }
                        log.info(`Updated request ${item.id} record.`);
                        
                        resolve();
                      });
                    })
                    .catch( (e: Error) => {
                      handleError(e);
                    });
              } else {
                seattle.GetCitationsByPlate(plate, state).then( (citations) => {
                  if (!citations || citations.length == 0) {
                    var now = Date.now();
                    // TTL is 10 years from now until the records are PROCESSED
                    var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);

                    var citation: ICitationRecord = {
                      id: GetHowsMyDrivingId(),
                      citation_id: CitationIds.CitationIDNoCitationsFound,
                      request_id: item.id,
                      processing_status: "UNPROCESSED",
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

                    mungeObject(citation, column_overrides);

                    citation_records.push({
                      PutRequest: {
                        Item: citation
                      }
                    });
                  } else {
                    citations.forEach(citation => {
                      var now = Date.now();
                      // TTL is 10 years from now until the records are PROCESSED
                      var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);
                      
                      let citation_record:CitationRecord = new CitationRecord(citation);
                      
                      citation_record.id = GetHowsMyDrivingId();
                      citation_record.citation_id = citation.citation_id;
                      citation_record.request_id = item.id;
                      citation_record.processing_status = "UNPROCESSED";
                      citation_record.license = item.license;
                      citation_record.created = now;
                      citation_record.modified = now;
                      citation_record.ttl_expire = ttl_expire;
                      citation_record.tweet_id = item.tweet_id;
                      citation_record.tweet_id_str = item.tweet_id_str;
                      citation_record.tweet_user_id = item.tweet_user_id;
                      citation_record.tweet_user_id_str = item.tweet_user_id_str;
                      citation_record.tweet_user_screen_name = item.tweet_user_screen_name;

                      // DynamoDB does nonnt allow any property to be null or empty string.
                      // Set these values to 'None'.
                      mungeObject(citation_record, column_overrides);

                      citation_records.push({
                        PutRequest: {
                          Item: citation_record
                        }
                      });
                    });
                  }
                  
                  batchWriteWithExponentialBackoff(
                    new AWS.DynamoDB.DocumentClient(),
                    tableNames["Citations"], 
                    citation_records).then ( () => {
                      log.info(`Finished writing ${citation_records.length} citation records for ${state}:${plate}.`);          
                    
                      var params = {
                        TableName: tableNames["Request"],
                        Key: {
                          id: item.id
                        },
                        AttributeUpdates: {
                          processing_status: {
                            Action: "PUT",
                            Value: "PROCESSED"
                          },
                          modified: {
                            Action: "PUT",
                            Value: Date.now()
                          }
                        }
                      };

                      // What happens if update gets throttled?
                      // I don't see any info on that. Does that mean it doesn't?
                      // It just succeeds or fails?
                      docClient.update(params, (err: Error, data: any) => {
                        if (err) {
                          handleError(err);
                        }
                        
                        log.info(`Updated request ${item.id} record.`);
                      
                      // OK this is the success point for processing this reqeuest.
                      // Resolve the Promise we are in.
                      resolve();
                    });
                  }).catch( (err: Error) => {
                    handleError(err);
                  });
                }).catch( (err: Error) => {
                  handleError(err);
                });
              }
            });
            
            request_promises.push(request_promise);
          });
        } else {
          log.debug("No request records found.");
        }
      
      Promise.all(request_promises).then( () => {
        // This is the only success. Every other codepath represents a failure.
        response.sendStatus(200);
      }).catch( (err: Error) => {
        handleError(err);
      })
    })
    .catch( (err: Error) => {
      handleError(err);
    });
  } catch ( err ) {
    response.status(500).send(err);
  }
});

app.all("/processcitations", (request: Request, response: Response) => {
  try {
    var docClient = new AWS.DynamoDB.DocumentClient();

    GetCitationRecords().then( (citations: Array<ICitationRecord>) => {
      let request_promises: Array<Promise<void>> = [];
      
      if (citations && citations.length > 0) {
        let citationsByRequest: { [request_id: string] : Array<ICitationRecord> } = {};
        let citationsByPlate: { [plate: string] : number } = {};

        log.info(`Processing ${citations.length} citation records...`);

        // Sort them based on request
        citations.forEach( (citation: ICitationRecord) => {
          if (!(citation.request_id in citationsByRequest)) {
            citationsByRequest[citation.request_id] = new Array();
          }

          citationsByRequest[citation.request_id].push(citation);
        });
        
        let requestsforplate_promises: {[plate: string] : Promise<number>; } = {};
        
        // Kick of the DB calls to get query counts for each of these requests
        citations.forEach( (citation) => {
          citationsByPlate[citation.license] = 1;
        })
        
        Object.keys(citationsByPlate).forEach( (license) => {
          requestsforplate_promises[license] = GetQueryCount(license);
        })

        // Now process the citations, on a per-request basis
        Object.keys(citationsByRequest).forEach( (request_id) => {
          var request_promise: Promise<void> = new Promise<void>( (resolve, reject) => {  
            // Get the first citation to access citation columns
            let citation: ICitationRecord = citationsByRequest[request_id][0];
            requestsforplate_promises[citation.license].then( (query_count ) => {
              let report_items: Array<string> = []
              // Check to see if there was only a dummy citation for this plate
              if (citationsByRequest[request_id].length == 1 && citationsByRequest[request_id][0].citation_id < CitationIds.MINIMUM_CITATION_ID) {
               report_items = GetReportItemForPseudoCitation(citationsByRequest[request_id][0], query_count);
              }                
              else {
                report_items = seattle
                  .ProcessCitationsForRequest(citationsByRequest[request_id], query_count);
              }

                // Write report items
              WriteReportItemRecords(docClient, request_id, citation, report_items)
                .then( () => {
                  log.info(`Wrote ${report_items.length} report item records for request ${request_id}.`)
                  // Set the processing status of all the citations
                  var citation_records: any = [];
                  var now = Date.now();
                  // Now that the record is PROCESSED, TTL is 1 month 
                  var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);

                  citationsByRequest[request_id].forEach(citation => {
                    citation.processing_status = "PROCESSED";
                    citation.modified = now;
                    citation.ttl_expire = ttl_expire;

                    citation_records.push({
                      PutRequest: {
                        Item: citation
                      }
                    });
                  });

                  batchWriteWithExponentialBackoff(
                    new AWS.DynamoDB.DocumentClient(),
                    tableNames["Citations"], 
                    citation_records
                  ).then( () => {
                    log.info(`Set ${citation_records.length} citation records for request ${request_id} to PROCESSED.`)
                    // This is the one success point for this request.
                    // All other codepaths indicate a failure.
                    resolve();
                  }).catch ( (err: Error) => {
                    handleError(err);
                  });

                })
                .catch( (e: Error) => {
                  handleError(e);
                })
              });
          });

          request_promises.push(request_promise);
        });
      } else {
        log.debug("No citations found.");
      }
      
      Promise.all(request_promises).then( () => {
        // This is the one success point for all citations being processed.
        // Every other codepath is a failure of some kind.
        response.sendStatus(200);
      })
    }).catch( (err: Error) => {
      handleError(err);
    });
  } catch (err ) {
    response.status(500).send(err);
  }
});

app.all("/processreportitems", (request: Request, response: Response) => {
  var T = new Twit(config.twitter);
  var docClient = new AWS.DynamoDB.DocumentClient();
  var request_promises: Array<Promise<void>> = [];

  log.info(`Checking for report items...`);

  GetReportItemRecords()
    .then( (report_items: Array<IReportItemRecord>) => {
      var reportitem_count = report_items.length;
      var tweet_count = 0;

      if (report_items && report_items.length > 0) {
        log.info(`Processing ${report_items.length} report items...`)
        var reportItemsByRequest: { [request_id: string] : Array<IReportItemRecord>; } = {};

        // Sort them based on request
        report_items.forEach(report_item => {
          if (!(report_item.request_id in reportItemsByRequest)) {
            reportItemsByRequest[report_item.request_id] = new Array();
          }

          reportItemsByRequest[report_item.request_id].push(report_item);
        });

        // For each request, we need to sort the report items since the order
        // they are tweeted in matters.
        Object.keys(reportItemsByRequest).forEach(request_id => {
          reportItemsByRequest[request_id] = reportItemsByRequest[
            request_id
          ].sort(function(a, b) {
            return a.record_num - b.record_num;
          });
        });

        // Now process the report_items, on a per-request basis
        Object.keys(reportItemsByRequest).forEach(request_id => {
          var request_promise: Promise<void> = new Promise<void> ( (resolve, reject) => {
            // Get the first report_item to access report_item columns
            var report_item = reportItemsByRequest[request_id][0];
            // Build a fake tweet for the request report_item
            let user: Twit.Twitter.User = {} as Twit.Twitter.User;
            
            
            user.screen_name = report_item.tweet_user_screen_name;

            let origTweet: Twit.Twitter.Status = {
              id: report_item.tweet_id,
              id_str: report_item.tweet_id_str,
              user: user
            } as Twit.Twitter.Status;

            log.debug(`Creating mutex for SendResponses...`);
            var mutex_client: Client = new LMXClient();
            
            mutex_client.emitter.on('info', function () {
              log.debug(...arguments);
            });

            mutex_client.emitter.on('warning', function () {
              log.warn(...arguments);
            });

            mutex_client.emitter.on('error', function () {
              log.error(...arguments);
            });

            mutex_client.connect().then( (client) => {
              log.info(`Successfully created mutex client.`);
            }).catch( (err: Error) => {
              log.info(`Failed to create mutex client/broker. Proceeding without them. Err: ${err}.`);
              mutex_client = undefined;
            }).finally( () => {

              log.info(`Posting tweets for ${reportItemsByRequest[request_id][0].license}, request ${request_id}.`);
              // Send a copy of the report items to SendResponse since we need to
              SendResponses(
                mutex_client,
                T,
                origTweet,
                reportItemsByRequest[request_id]
              )
                .then( ( tweets_sent_count ) => {
                  log.info(
                    `Finished sending ${reportItemsByRequest[request_id].length} tweets for request ${reportItemsByRequest[request_id][0].request_id}.`
                  );

                  tweet_count =
                    tweet_count + tweets_sent_count;

                  log.debug(`Closing mutex for SendResponses.`);
                  if (mutex_client) {
                    mutex_client.close();
                  }

                  // Set the processing status of all the report_items
                  var report_item_records: Array<object> = [];
                  var now = Date.now();
                  // Now that the record is PROCESSED, TTL is 1 month
                  var ttl_expire = new Date(now).setFullYear(
                    new Date(now).getFullYear() + 10
                  );

                  reportItemsByRequest[request_id].forEach(report_item => {
                    report_item.processing_status = "PROCESSED";
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
                    tableNames["ReportItems"],
                    report_item_records
                  )
                  .then( () => {
                    // This is the one and only success point for these report item records.
                    // Every other codepath is an error of some kind.
                    resolve();
                  })
                  .catch( (err: Error) => {
                    handleError(err);
                  });
                })
                .catch( (err: Error) => {
                  handleError(err);
                });
            })
          });
          
          request_promises.push(request_promise);
        });
      } else {
        log.info("No report items found.");
      }

    log.debug(`Waiting for ${request_promises.length} request_promises.`);  
    Promise.all(request_promises)
      .then( () => {
        if (request_promises.length > 0) {
          log.info(
            `Sent ${tweet_count} tweets for ${reportitem_count} report items.`
          );
        }

        // Tweets for all the requests have completed successfully
        response.sendStatus(200);
      })
      .catch( (err: Error) => {
        handleError(err);
      });
  });
});

export function processNewTweets(T: Twit, docClient: AWS.DynamoDB.DocumentClient, bot_app_id: number): Promise<void> {
  let maxTweetIdRead: string = "-1";
  
  // Collect promises from these operations so they can go in parallel
  var twitter_promises: Array<Promise<void>> = [];

  /* First, let's load the ID of the last tweet we responded to. */
  var last_mention_id = (maxTweetIdRead = getLastMentionId());

  var tweet_promises = [];
  if (!last_mention_id) {
    handleError(new Error("ERROR: No last dm found! Defaulting to zero."));
  }
  var mentions_promise = new Promise<void>((resolve, reject) => {
    log.info(`Checking for tweets greater than ${last_mention_id}.`);
    /* Next, let's search for Tweets that mention our bot, starting after the last mention we responded to. */
    T.get(
      "search/tweets",
      {
        q: "%40" + process.env.TWITTER_HANDLE,
        since_id: last_mention_id,
        tweet_mode: "extended"
      },
      function(err: Error, data: Twit.Twitter.SearchResults, response: http.IncomingMessage) {
        if (err) {
          handleError(err);
          return false;
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
          data.statuses.forEach( (status: Twit.Twitter.Status) => {
            var request_records = [];

            log.debug(`Found ${PrintTweet(status)}`);

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
              if (status.hasOwnProperty("retweet_status")) {
                log.debug(`Ignoring retweet: ${status.full_text}`);
              } else if (status.user.id == bot_app_id) {
                log.debug("Ignoring our own tweet: " + status.full_text);
              } else {
                const { state, plate } = parseTweet(chomped_text);
                var now = Date.now();
                var item = {
                  PutRequest: {
                    Item: {
                      id: GetHowsMyDrivingId(),
                      license: `${state}:${plate}`, // TODO: Create a function for this plate formatting.
                      created: now,
                      modified: now,
                      processing_status: "UNPROCESSED",
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
              log.debug(
                "Ignoring reply that didn't actually reference bot: " +
                  status.full_text
              );
            }
            
            if (request_records.length > 0) {
              num_request_records += request_records.length;
              twitter_promises.push(
                batchWriteWithExponentialBackoff(
                  docClient,
                  tableNames["Request"],
                  request_records
                )
              );
            }
          });
        } else {
          /* No new mentions since the last time we checked. */
          log.info("No new mentions...");
        }

        Promise.all(twitter_promises)
          .then( () => {
            if (num_tweets > 0) {
              log.info(`Wrote ${num_request_records} request records for ${num_tweets} tweets.`)
            }
          
            // Update the ids of the last tweet/dm if we processed
            // anything with larger ids.
            if (CompareNumericStrings(maxTweetIdRead, last_mention_id) > 0) {
              setLastMentionId(maxTweetIdRead);
            }

            resolve();
          })
          .catch( (err: Error) => {
            handleError(err);
          });
      }
    );
  });
  
  return mentions_promise;
}

function processNewDMs() {
  var maxDmIdRead = -1;

  /* Respond to DMs */
  /* Load the ID of the last DM we responded to. */
  var last_dm_id = (maxDmIdRead = getLastDmId());

  if (!last_dm_id) {
    handleError(new Error("ERROR: No last dm found! Defaulting to zero."));
  }

  var dm_promise = Promise.resolve();
  /*
    TODO: Implement DM handling.
    var dm_promise = new Promise( (resolve, reject) => {
    T.get("direct_messages", { since_id: last_dm_id, count: 200 }, function(
      err,
      dms,
      response
    ) {
      // Next, let's DM's to our bot, starting after the last DM we responded to.
      var dm_post_promises = [];
      if (dms.length) {
        
        dms.forEach(function(dm) {
          log.debug(
            `Direct message: sender (${dm.sender_id}) id_str (${dm.id_str}) ${dm.text}`
          );

          // Now we can respond to each tweet.
          var dm_post_promise = new Promise( (resolve, reject) => {
          T.post(
            "direct_messages/new",
            {
              user_id: dm.sender_id,
              text: "This is a test response."
            },
            function(err, data, response) {
              if (err) {
                // TODO: Proper error handling?
                handleError(err);
              } else {
                
                if (maxDmIdRead < dm.id_str) {
                  maxDmIdRead = dm.id_str;
                }
                
                // TODO: Implement this.
                resolve();
              }
            }
          );
          });
          
          dm_post_promises.push(dm_post_promise);
        });
        
      } else {
        // No new DMs since the last time we checked.
        log.debug("No new DMs...");
      }
      
      Promise.all(dm_post_promises).then( () => {
        // resolve the outer promise for all dm's
        resolve();
      }).catch( (err) => {
        handleError(err);
      })
    });
    });
    
    tweet_promises.push(dm_promise);
    */
  
  return dm_promise;
}

function batchWriteWithExponentialBackoff(docClient: AWS.DynamoDB.DocumentClient, table:string, records: Array<object>): Promise<void> {
  return new Promise( (resolve, reject) => {
    var qdb = docClient ? Q(docClient) : Q();
    qdb.drain = function () {
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
  })
}

function GetReportItemForPseudoCitation(citation: ICitation, query_count: number): Array<string> {
  if (!citation || citation.citation_id >= CitationIds.MINIMUM_CITATION_ID) {
    throw new Error(`ERROR: Unexpected citation ID: ${citation.citation_id}.`);
  }

  switch ( citation.citation_id ) {
    case CitationIds.CitationIDNoPlateFound:
      return [noValidPlate];
      break;

    case CitationIds.CitationIDNoCitationsFound:
      return [
        `${noCitationsFoundMessage}${formatPlate(citation.license)}` +
        "\n\n" +
        citationQueryText.replace('__LICENSE__', formatPlate(citation.license)).replace('__COUNT__', query_count.toString())
      ];
      break;

    default:
      throw new Error(`ERROR: Unexpected citation ID: ${citation.citation_id}.`);
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

  if (matches == null || matches.length < 2 || matches[1] == "XX") {
    log.warn(`No license found in tweet: ${text}`);
  } else {
    state = matches[1];
    plate = matches[2];

    if (StatesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      handleError(new Error(`Invalid state: ${state}`));
    }
  }

  return {
    state: state ? state.toUpperCase() : "",
    plate: plate ? plate.toUpperCase() : ""
  };
}

function mungeObject(o: any, propertyOverrides: { [ key: string ]: any }): void {
  for (var p in o) {
    if (o.hasOwnProperty(p)) {
      if (p in propertyOverrides) {
        // OK, we need to override this property if it is undefined, null or empty string
        var val = o[p];
        
        if (val == undefined || val == null || val == "") {
          o[p] = propertyOverrides[p];
        }
      }
    }
  }
}

function GetQueryCount(license: string): Promise<number> {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var request_records = [];

  // Query unprocessed requests
  var params = {
    TableName: tableNames["Request"],
    IndexName: "license-index",
    Select: "COUNT",
    KeyConditionExpression: "#license = :lic",
    ExpressionAttributeNames: {
      "#license": "license"
    },
    ExpressionAttributeValues: {
      ":lic": license
    }
  };

  return new Promise<number>(function(resolve, reject) {
    // 1. Do a query to get just the request rcords for this license.
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED citations for
    //    each request_id. This ensures we process all the citations for a given
    //    request together. This is required cause we tweet out summaries/totals.
    docClient.query(params, function(err: Error, result) {
      if (err) {
        handleError(err);
      } else {
        resolve( result.Count );
      }
    });
  })
}

function SendResponses(mutex_client: Client, T: Twit, origTweet: Twit.Twitter.Status, report_items: Array<IReportItemRecord>): Promise<number> {
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
  var tweetText = "";
  
  if (!(replyToScreenName.toUpperCase() === process.env.TWITTER_HANDLE.toUpperCase())) {
    log.info(`Account ${replyToScreenName} != ${process.env.TWITTER_HANDLE}.`);
    tweetText += "@" + replyToScreenName + " ";
  } else {
    log.info(`Account ${replyToScreenName} === ${process.env.TWITTER_HANDLE}.`)
  }
  tweetText += report_item.tweet_text;
  log.debug(`Sending Tweet: ${tweetText}.`);
  return new Promise<number>( (resolve, reject) => {
    let tweets_sent: number = 0;
    
    // There will be one thread running this for each request we are
    // processing. We need to make sure we don't send tweets in quick
    // succession or Twitter will tag them as spam and they won't
    // render i the thread of resposes.
    // So wait at least INTER_TWEET_DELAY_MS ms between posts.
    let mutex_key = GetHowsMyDrivingId();
    
    log.debug(`Acquiring mutex ${mutex_key}...`);
    var mutex_promise;
    
    if (mutex_client) {
      mutex_promise = mutex_client.acquireLock(mutex_key, {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS, 
        maxRetries: MUTEX_TWIT_POST_MAX_RETRIES, 
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      });
    }
    else {
      mutex_promise = Promise.resolve( { id: "id", key: "key"} );
    }
    
    mutex_promise.then( (v) => {
      log.debug(`Acquired mutex ${v.id} and received key ${v.key}.`);
      T.post(
        "statuses/update",
        {
          status: tweetText,
          in_reply_to_status_id: replyToTweetId
          /*,
          auto_populate_reply_metadata: true*/
        } as Twit.Params,
        (err: Error, data: Twit.Twitter.Status, response: http.IncomingMessage) => {
          let twit_error_code: number = 0;
          
          if (err && err.hasOwnProperty("code")) {
            twit_error_code = (err as any)["code"];
          }
          
          if (err && twit_error_code != 187) {
            if (mutex_client) {
              log.debug(`Releasing mutex key=${v.key}, id:${v.id}...`);
              mutex_client.releaseLock(v.key, { id: v.id, force: true });
              log.debug(`Released mutex key=${v.key}, id:${v.id}...`);
            }
            handleError(err);
          } else {
            if (err && twit_error_code == 187) {
              // This appears to be a "status is a duplicate" error which
              // means we are trying to resend a tweet we already sent.
              // Pretend we succeeded.
              log.warn('Received error 187 from T.post which means we already posted this tweet. Pretend we succeeded.');

              // Keep replying to the tweet we were told to reply to.
              // This means that in this scenario, if any of the rest of the tweets in this
              // thread have not been sent, they will create a new thread off the parent of
              // this one.
              // Not ideal, but the other alternatives are:
              // 1) Query for the previous duplicate tweet and then pass that along
              // 2) set all of the replies for this request to be PROCESSED even if they did not 
              //    all get tweeted.
              data = origTweet;
            }
            else {
              tweets_sent++;
              log.info(`Sent tweet: ${PrintTweet(data)}.`);
            }

            // Wait a bit. It seems tweeting a whackload of tweets in quick succession
            // can cause Twitter to think you're a troll bot or something and then some
            // of the tweets will not display for users other than the bot account.
            // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
            sleep(report_items_clone.length > 0 ? INTER_TWEET_DELAY_MS : 0).then( () => {
              // Don't release the mutex until after we sleep.
              let release_promise: Promise<any>;

              if (mutex_client) {
                log.debug(`Releasing mutex key=${v.key}, id:${v.id}...`);
                release_promise = mutex_client.releaseLock(v.key, { id: v.id, force: true });
                log.debug(`Released mutex key=${v.key}, id:${v.id}...`);
              } else {
                log.debug(`Faking release of mutex key=${v.key}, id:${v.id}...`);
                let dummy:Object = null;
                release_promise = Promise.resolve(dummy);
              }
              
              release_promise
                .then( () => {
                  log.debug(`Released mutex key=${v.key}, id:${v.id}...`);
                })
                .catch( ( err: Error ) => {
                  handleError( err );
                })
                .finally( () => {
                  // Send the rest of the responses. When those are sent, then resolve
                  // the local Promise.
                  SendResponses(mutex_client, T, data, report_items_clone)
                    .then(tweets_sent_rest => {
                      tweets_sent += tweets_sent_rest
                      resolve(tweets_sent);
                    })
                    .catch( (err: Error) => {
                      handleError(err);
                    });
                });
            }).catch( (err: Error) => {
              handleError(err);
            });
          }
        });
    })
    .catch ( (err: Error) => {
      handleError(err);  
    });
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
    if (process.env.hasOwnProperty("FALLBACK_LAST_DM_ID")) {
      lastdm = process.env.FALLBACK_LAST_DM_ID;
      
      if (lastdm <= 0) {
        throw new Error("No last dm id found.");
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
    if (process.env.hasOwnProperty("FALLBACK_LAST_MENTION_ID")) {
      lastmention = process.env.FALLBACK_LAST_MENTION_ID;
      
      if (lastmention <= 0) {
        throw new Error("No last mention id found.");
      } else {
        setLastMentionId(lastmention);
      }
    }  
  }
  
  return lastmention ? lastmention : "0";
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
  var stacktrace = error.stack
    .split("\n")
    .slice(0, 10)
    .join("\n");
  var formattedError = `===============================================================================\n${error.message}\n${stacktrace}`;

  log.error(formattedError);
  throw error;
}

function getTweetById(T: Twit, id: string) {
  // Quick check to fetch a specific tweet.
  var promise: Promise<Twit.Twitter.Status> = new Promise<Twit.Twitter.Status>((resolve, reject) => {
    var retTweet;

    T.get(`statuses/show/${id}`, { tweet_mode: "extended" }, 
      (err: Error, tweet: Twit.Twitter.Status, response: http.IncomingMessage) => {
        if (err) {
          handleError(err);
          reject(tweet);
        }

        resolve(tweet);
      });
  });

  promise.then( (tweet) => {
    return tweet;
  }).catch( (err) => {
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
    TableName: tableNames["Request"],
    IndexName: "processing_status-index",
    Select: "ALL_ATTRIBUTES",
    KeyConditionExpression: "#processing_status = :pkey",
    ExpressionAttributeNames: {
      "#processing_status": "processing_status"
    },
    ExpressionAttributeValues: {
      ":pkey": "UNPROCESSED"
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
    TableName: tableNames["Citations"],
    IndexName: "processing_status-index",
    Select: "ALL_ATTRIBUTES",

    KeyConditionExpression: "#processing_status = :pkey",
    ExpressionAttributeNames: {
      "#processing_status": "processing_status"
    },
    ExpressionAttributeValues: {
      ":pkey": "UNPROCESSED"
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
        var requestIDs: { [key: string ]: number} = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id as string] = 1;
        });

        // 3. Check if we retrieved all the unprocessed citations
        if (
          result.hasOwnProperty("LastEvaluatedKey") &&
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
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression =
              `request_id = ${requestID} AND processing_status = 'UNPROCESSED'`;
            params.Limit = MAX_RECORDS_BATCH; // If there is a license with more citations than this... good enough

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH citations.
            //       I'm sure some gasshole will eventually prove us overly optimistic. 
            await docClient.query(params, (err: Error, result: QueryOutput) => {
              if (err) {
                handleError(err);
              } else {
                // TODO: There must be a type-safe way to do this...
                let citation_records_batch: any = result.Items;
                citation_records.concat(citation_records_batch as Array<ICitationRecord>);
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
    TableName: tableNames["ReportItems"],
    IndexName: "processing_status-index",
    Select: "ALL_ATTRIBUTES",

    KeyConditionExpression: "#processing_status = :pkey",
    ExpressionAttributeNames: {
      "#processing_status": "processing_status"
    },
    ExpressionAttributeValues: {
      ":pkey": "UNPROCESSED"
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
        var requestIDs: { [key: string ]: number} = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id] = 1;
        });

        // 3. Check if we retrieved all the unprocessed report items
        if (
          result.hasOwnProperty("LastEvaluatedKey") &&
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
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression =
              `request_id = ${requestID} AND processing_status = 'UNPROCESSED'`;
            params.Limit = MAX_RECORDS_BATCH;

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH report items.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                report_item_records.concat(result.Items as Array<IReportItemRecord>);
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

function WriteReportItemRecords(docClient: AWS.DynamoDB.DocumentClient, request_id: string, citation: ICitationRecord, report_items: Array<string>) {
  var docClient = new AWS.DynamoDB.DocumentClient();
  var truncated_report_items: Array<string> = [];

  // 1. Go through all report_items and split up any that will be > 280 characters when tweeted.
  // TODO: We should probably do this when processing the records, not before writing them.
  truncated_report_items = SplitLongLines(report_items, maxTweetLength);

  // 2. Build the report item records
  var now = Date.now();
  // TTL is 10 years from now until the records are PROCESSED
  var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);
  var report_item_records: Array<object> = [];
  var record_num = 0;

  truncated_report_items.forEach(report_text => {
    var item = {
      PutRequest: {
        Item: {
          id: GetHowsMyDrivingId(),
          request_id: request_id,
          record_num: record_num++,
          created: now,
          modified: now,
          ttl_expire: ttl_expire,
          processing_status: "UNPROCESSED",
          license: citation.license,
          tweet_id: citation.tweet_id,
          tweet_id_str: citation.tweet_id_str,
          tweet_user_id: citation.tweet_user_id,
          tweet_user_id_str: citation.tweet_user_id_str,
          tweet_user_screen_name: citation.tweet_user_screen_name,
          tweet_text: report_text
        }
      }
    };

    report_item_records.push(item);
  });

  // 3. Write the report item records, returning that Promise. 
  return batchWriteWithExponentialBackoff(docClient, tableNames["ReportItems"], report_item_records);
}

function getAccountID(T: Twit): Promise<number> {
  return new Promise( ( resolve, reject) => {
    T.get("account/verify_credentials", {}, (err: Error, data: any, response: http.IncomingMessage) => {
      if (err) {
        handleError(err);
      }
      resolve(data.id);
    });    
  });
}

