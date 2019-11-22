// Exported functions for tests
module.exports = {
  _chompTweet: chompTweet,
  _splitLines: SplitLongLines,
  _getQueryCount: GetQueryCount,
  _processNewTweets: processNewTweets,
  _processNewDMs: processNewDMs
};

/* Setting things up. */
const AWS = require("aws-sdk"),
  howsmydriving_utils = require("howsmydriving-utils"),
  chokidar = require('chokidar'),
  express = require("express"),
  fs = require("fs"),
  license = require("./opendata/licensehelper"),
  LocalStorage = require('node-localstorage').LocalStorage,
  path = require("path"),
  Q = require('./util/batch-write-queue.js'),
  seattle = require("./opendata/seattle"),
  soap = require("soap"),
  strUtils = require('./util/stringutils.js'),
  Twit = require("twit"),
  uuidv1 = require("uuid/v1");


var {LMXClient, LMXBroker} = require('live-mutex');

//console.log(`hmdUtils:\n${strUtils._printObject(hmdUtils)}\nEND.`);

var app = express(),
  config = {
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
const MUTEX_NAME_TWIT_POST = 'twitter_post',
      MUTEX_TWIT_POST_MAX_HOLD_MS = 100000,
      MUTEX_TWIT_POST_MAX_RETRIES = 5,
      MUTEX_TWIT_POST_MAX_WAIT_MS = 300000;

process.setMaxListeners(15);
    

const MAX_RECORDS_BATCH = 2000, 
  INTER_TWEET_DELAY_MS =
    process.env.hasOwnProperty("INTER_TWEET_DELAY_MS") &&
    process.env.INTER_TWEET_DELAY_MS > 0
      ? process.env.INTER_TWEET_DELAY_MS
      : 5000,
  tableNames = {
    Request: `${process.env.DB_PREFIX}_Request`,
    Citations: `${process.env.DB_PREFIX}_Citations`,
    ReportItems: `${process.env.DB_PREFIX}_ReportItems`
  };

module.exports._tableNames = tableNames;

var log = howsmydriving_utils.getLog(howsmydriving_utils.LogType.app),
    lastdmLog = howsmydriving_utils.getLog(howsmydriving_utils.LogType.last_dm),  
    lastmentionLog = howsmydriving_utils.getLog(howsmydriving_utils.LogType.last_mention);

log.info(`${process.env.TWITTER_HANDLE}: start`);

AWS.config.update({ region: "us-east-2" });

var maxTweetLength = 280 - 17; // Max username is 15 chars + '@' plus the space after the full username
var tweets = [];
var noCitations = "No citations found for plate # ";
var parkingAndCameraViolationsText =
  "Total parking and camera violations for #";
var violationsByYearText = "Violations by year for #";
var violationsByStatusText = "Violations by status for #";
var licenseQueriedCountText =
  "License __LICENSE__ has been queried __COUNT__ times.";
var licenseRegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;
var botScreenNameRegexp = new RegExp(
  "@" + process.env.TWITTER_HANDLE + "\\b",
  "i"
);

app.use(express.static("public"));

var listener = app.listen(process.env.PORT, function() {
  log.info(`Your bot is running on port ${listener.address().port}`);
});

/* tracks the largest tweet ID retweeted - they are not processed in order, due to parallelization  */
/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all("/tweet", function(request, response) {
  const T = new Twit(config.twitter);
  var docClient = new AWS.DynamoDB.DocumentClient();
  
  // We need the bot's app id to detect tweets from the bot
  getAccountID(T).then( ( app_id ) => {
    try {
      var twitter_promises = [];
      var tweet_process_promise = processNewTweets(T, docClient, app_id);
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
  }).catch( (err) => {
    handleError(err);
  });
});

app.all("/test", function(request, response) {
  // Doing the require here will cause the tests to rerun every
  // time the /test url is loaded even if no test or product
  // code has changed.
  var Mocha = require("mocha");

  // Instantiate a Mocha instance.
  var mocha = new Mocha();

  var testDir = "./test";

  // Add each .js file to the mocha instance
  fs.readdirSync(testDir)
    .filter(function(file) {
      // Only keep the .js files
      return file.substr(-3) === ".js";
    })
    .forEach(function(file) {
      mocha.addFile(path.join(testDir, file));
    });

  var test_results = '';
  var failures = false;

  // Run the tests.
  mocha
    .run()
    .on("test", function(test) {
      test_results += `Test started: ${test.title}\n`;
    })
    .on("test end", function(test) {
      //test_results += `Test done: ${test.title}\n`;
    })
    .on("pass", function(test) {
      test_results += `Test passed: ${test.title}\n\n`;
    })
    .on("fail", function(test, err) {
      test_results += `Test failed: ${test.title}\nError:\n${err}\n\n`;
      failures = true;
    })
    .on("end", function() {
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

app.all("/dumpfile", function(request, response) {
  var fileName = `${__dirname}/log/err.log`;

  if (request.query.hasOwnProperty("filename")) {
    fileName = `${__dirname}/${request.query.filename}`;
  }
  log.info(`Sending file: ${fileName}.`);
  response.sendFile(fileName);
});

app.all("/dumptweet", function(request, response) {
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

app.all("/dumpcitations", function(request, response) {
  try {
  var state;
  var plate;
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
    .then( (citations) => {
      var body = "Citations found:\n";

      if (!citations || citations.length == 0) {
        response.send(noCitations);
      } else {
        // TODO: Can we just send the whole array as json?
        response.json(citations);
      }
    })
    .catch(function(err) {
      handleError(err);
    });
  }
  catch ( err ) {
    response.status(500).send(err);
  }
});

app.all("/errors", function(request, response) {
  try {
    var fileName = `${__dirname}/log/err.log`;

    log.info(`Sending file: ${fileName}.`);
    response.sendFile(fileName);
  }
  catch ( err ) {
    response.status(500).send(err);
  }
});

// uptimerobot.com hits this every 5 minutes
app.all("/processrequests", function(request, response) {
  try {
    var docClient = new AWS.DynamoDB.DocumentClient();

    log.debug(`Checking for request records...`);
    GetRequestRecords()
      .then( (request_records) => {
        log.info(`Processing ${request_records.length} request records...`);
        
        var request_promises = [];
      
        if (request_records && request_records.length > 0) {
          request_records.forEach( (item) => {
            var batch_write_promises = [];
            var citation_records = [];
            var tokens = item.license.split(":");
            var state;
            var plate;

            if (tokens.length == 2) {
              state = tokens[0];
              plate = tokens[1];
            }
            
            var request_promise = new Promise( (resolve, reject) => {
              if (state == null || state == "" || plate == null || plate == "") {
                log.warn(
                  `Not a valid state/plate in this request (${state}/${plate}).`
                );
                // There was no valid plate found in the tweet. Add a dummy citation.
                var now = Date.now();
                // TTL is 10 years from now until the records are PROCESSED
                var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);
                var citation = {
                  id: strUtils._getUUID(),
                  Citation: seattle.CitationIDNoPlateFound,
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

                      docClient.update(params, function(err, data) {
                        if (err) {
                          handleError(err);
                        }
                        resolve();
                      });
                    })
                    .catch(function(e) {
                      handleError(e);
                    });
              } else {
                seattle.GetCitationsByPlate(plate, state).then( (citations) => {
                  if (!citations || citations.length == 0) {
                    var now = Date.now();
                    // TTL is 10 years from now until the records are PROCESSED
                    var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);

                    var citation = {
                      id: strUtils._getUUID(),
                      Citation: seattle.CitationIDNoCitationsFound,
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

                    // DynamoDB does not allow any property to be null or empty string.
                    // Set these values to 'None'.
                    mungeCitation(citation);

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

                      citation.id = strUtils._getUUID();
                      citation.request_id = item.id;
                      citation.processing_status = "UNPROCESSED";
                      citation.license = item.license;
                      citation.created = now;
                      citation.modified = now;
                      citation.ttl_expire = ttl_expire;
                      citation.tweet_id = item.tweet_id;
                      citation.tweet_id_str = item.tweet_id_str;
                      citation.tweet_user_id = item.tweet_user_id;
                      citation.tweet_user_id_str = item.tweet_user_id_str;
                      citation.tweet_user_screen_name = item.tweet_user_screen_name;

                      // DynamoDB does not allow any property to be null or empty string.
                      // Set these values to 'None'.
                      mungeCitation(citation);

                      citation_records.push({
                        PutRequest: {
                          Item: citation
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
                      docClient.update(params, function(err, data) {
                        if (err) {
                          handleError(err);
                        }
                        
                        log.info(`Updated request ${item.id} record.`);
                      
                      // OK this is the success point for processing this reqeuest.
                      // Resolve the Promise we are in.
                      resolve();
                    });
                  }).catch( (err) => {
                    handleError(err);
                  });
                }).catch( (err) => {
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
      }).catch( (err) => {
        handleError(err);
      })
    })
    .catch(err => {
      handleError(err);
    });
  }
  catch ( err ) {
    response.status(500).send(err);
  }
});

app.all("/processcitations", function(request, response) {
  try {
    var docClient = new AWS.DynamoDB.DocumentClient();

    GetCitationRecords().then( (citations) => {
      var request_promises = [];
      
      if (citations && citations.length > 0) {
        var citationsByRequest = {};
        var citationsByPlate = {};

        log.info(`Processing ${citations.length} citation records...`);

        // Sort them based on request
        citations.forEach(function(citation) {
          if (!(citation.request_id in citationsByRequest)) {
            citationsByRequest[citation.request_id] = new Array();
          }

          citationsByRequest[citation.request_id].push(citation);
        });
        
        var request_promises = [];
        var requestsforplate_promises = {};
        
        // Kick of the DB calls to get query counts for each of these requests
        citations.forEach( (citation) => {
          citationsByPlate[citation.license] = 1;
        })
        
        Object.keys(citationsByPlate).forEach( (license) => {
          requestsforplate_promises[license] = GetQueryCount(license);
        })

        // Now process the citations, on a per-request basis
        Object.keys(citationsByRequest).forEach( (request_id) => {
          var request_promise = new Promise( (resolve, reject) => {  
            // Get the first citation to access citation columns
            var citation = citationsByRequest[request_id][0];
            requestsforplate_promises[citation.license].then( (query_count ) => {
              seattle
                .ProcessCitationsForRequest(citationsByRequest[request_id], query_count)
                .then( (report_items) => {
                  // Write report items
                  WriteReportItemRecords(docClient, request_id, citation, report_items)
                    .then( (results) => {
                      log.info(`Wrote ${report_items.length} report item records for request ${request_id}.`)
                      // Set the processing status of all the citations
                      var citation_records = [];
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
                      }).catch ( (err) => {
                        handleError(err);
                      });

                    })
                    .catch(function(e) {
                      handleError(e);
                    });
                })
                .catch(e => {
                  handleError(e);
                });
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
      }).catch( (err) => {
        handleError(err);
      });
    }).catch( (err) => {
      handleError(err);
    });
  } catch (err) {
    response.status(500).send(err);
  }
});

app.all("/processreportitems", function(request, response) {
  var T = new Twit(config.twitter);
  var docClient = new AWS.DynamoDB.DocumentClient();
  var request_promises = [];

  log.info(`Checking for report items...`);

  GetReportItemRecords()
    .then( (report_items) => {
      var reportitem_count = report_items.length;
      var tweet_count = 0;

      if (report_items && report_items.length > 0) {
        log.info(`Processing ${report_items.length} report items...`)
        var reportItemsByRequest = {};

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
          var request_promise = new Promise((resolve, reject) => {
            // Get the first report_item to access report_item columns
            var report_item = reportItemsByRequest[request_id][0];
            // Build a fake tweet for the request report_item
            var origTweet = {
              id: report_item.tweet_id,
              id_str: report_item.tweet_id_str,
              user: {
                screen_name: report_item.tweet_user_screen_name
              }
            };

            log.debug(`Creating mutex for SendResponses...`);
            var mutex_client,
                mutex_broker;
            
            Promise.all([new LMXBroker().ensure(), new LMXClient().connect()]).then( ([broker, client]) => {
              mutex_client = client;
              mutex_broker = broker;
              log.info(`Successfully created mutex client/broker.`);
            }).catch( (err) => {
              log.info(`Failed to create mutex client/broker. Proceeding without them. Err: ${err}.`);
            }).finally( () => {
              log.debug(`Created mutex for SendResponses.`);

              if (mutex_broker && mutex_client) {
                mutex_broker.emitter.on("warning", function() {
                  log.debug(...arguments);
                });

                mutex_client.emitter.on("warning", function() {
                  log.debug(...arguments);
                });
              }

              log.info(`Posting tweets for ${reportItemsByRequest[request_id][0].license}, request ${request_id}.`);
              // Send a copy of the report items to SendResponse since we need to
              SendResponses(
                mutex_client,
                T,
                origTweet,
                reportItemsByRequest[request_id]
              )
                .then( () => {
                  log.info(
                    `Finished sending ${reportItemsByRequest[request_id].length} tweets for request ${reportItemsByRequest[request_id][0].request_id}.`
                  );

                  tweet_count =
                    tweet_count + reportItemsByRequest[request_id].length;
                  log.info(`Interim tweet_count: ${tweet_count}.`);

                  log.debug(`Closing mutex for SendResponses.`);
                  if (mutex_client) {
                    mutex_client.close();
                  }

                  // Set the processing status of all the report_items
                  var report_item_records = [];
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
                  .catch(err => {
                    handleError(err);
                  });
                })
                .catch(err => {
                  handleError(err);
                });

              // TODO: What do I have to do with the broker/client?!!
              if (mutex_broker) {
                mutex_broker.close();
              }
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
        .catch(err => {
          handleError(err);
        });
    })
    .catch(function(err) {
      response.status(500).send(err);
    });
});

function processNewTweets(T, docClient, bot_app_id) {
  var maxTweetIdRead = -1;
  
  // Collect promises from these operations so they can go in parallel
  var twitter_promises = [];

  /* First, let's load the ID of the last tweet we responded to. */
  var last_mention_id = (maxTweetIdRead = getLastMentionId());

  var tweet_promises = [];
  if (!last_mention_id) {
    handleError(new Error("ERROR: No last dm found! Defaulting to zero."));
  }
  var mentions_promise = new Promise((resolve, reject) => {
    log.info(`Checking for tweets greater than ${last_mention_id}.`);
    /* Next, let's search for Tweets that mention our bot, starting after the last mention we responded to. */
    T.get(
      "search/tweets",
      {
        q: "%40" + process.env.TWITTER_HANDLE,
        since_id: last_mention_id,
        tweet_mode: "extended"
      },
      function(err, data, response) {
        if (err) {
          handleError(err);
          return false;
        }

        var num_tweets = data.statuses.length;
        var num_request_records = 0;
        if (data.statuses.length) {
          /* 
          Iterate over each tweet. 

          The replies can occur concurrently, but the threaded replies to each tweet must, 
          within that thread, execute sequentially. 

          Since each tweet with a mention is processed in parallel, keep track of largest ID
          and write that at the end.
          */
          data.statuses.forEach(function(status) {
            var request_records = [];

            log.debug(`Found ${printTweet(status)}`);

            if (strUtils._compare_numeric_strings(maxTweetIdRead, status.id_str) < 0) {
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
                      id: strUtils._getUUID(),
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
            if (strUtils._compare_numeric_strings(maxTweetIdRead, last_mention_id) > 0) {
              setLastMentionId(maxTweetIdRead);
            }

            resolve();
          })
          .catch(err => {
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

function batchWriteWithExponentialBackoff(docClient, table, records) {
  return new Promise( (resolve, reject) => {
    var qdb = docClient ? Q(docClient) : Q();
    qdb.set_drain( function() {
      resolve();
    });
    
    qdb.set_error(function(err, task) {
      reject(err);
    });

    var startPos = 0;
    var endPos;
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

function chompTweet(tweet) {
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
function parseTweet(text) {
  var state;
  var plate;
  const matches = licenseRegExp.exec(text);

  if (matches == null || matches.length < 2 || matches[1] == "XX") {
    log.warn(`No license found in tweet: ${text}`);
  } else {
    state = matches[1];
    plate = matches[2];

    if (license.StatesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      handleError(new Error(`Invalid state: ${state}`));
    }
  }

  return {
    state: state ? state.toUpperCase() : "",
    plate: plate ? plate.toUpperCase() : ""
  };
}

function mungeCitation(citation) {
  // This munging needs to move into the Seattle module
  // DynamoDB does not allow writing null or empty string,
  // which is f*cking stupid but...
  var columns = {
    CaseNumber: -1,
    ChargeDocNumber: "None",
    Citation: "None",
    CollectionsStatus: "None",
    FilingDate: "None",
    InCollections: "false",
    Status: "None",
    Type: "None",
    ViolationDate: "None",
    ViolationLocation: "None"
  };

  for (var columnName in columns) {
    if (citation.hasOwnProperty(columnName)) {
      // Only update the column value if it is null or empty string
      if (citation[columnName] == null || citation[columnName] == "") {
        citation[columnName] = columns[columnName];
      }
    } else {
      // add the column
      citation[columnName] = columns[columnName];
    }
  }
}

async function GetQueryCount(license) {
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

  return new Promise(function(resolve, reject) {
    // 1. Do a query to get just the citations that are UNPROCESSED.
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED citations for
    //    each request_id. This ensures we process all the citations for a given
    //    request together. This is required cause we tweet out summaries/totals.
    docClient.query(params, function(err, result) {
      if (err) {
        handleError(err);
      } else {
        resolve(result.Count);
      }
    });
  });
}

function SendResponses(mutex_client, T, origTweet, report_items) {
  if (report_items.length == 0) {
    // return an promise that is already resolved, ending the recursive
    // chain of promises that have been built.
    return Promise.resolve();
  }

  // Clone the report_items array so we don't modify the one passed to us
  var report_items_clone = [...report_items];
  var report_item = report_items_clone.shift();
  var replyToScreenName = origTweet.user.screen_name;
  var replyToTweetId = origTweet.id_str;

  /* Now we can respond to each tweet. */
  var tweetText = "@" + replyToScreenName + " " + report_item.tweet_text;
  log.debug(`Sending Tweet: ${tweetText}.`);
  return new Promise((resolve, reject) => {
    // There will be one thread running this for each request we are
    // processing. We need to make sure we don't send tweets in quick
    // succession or Twitter will tag them as spam and they won't
    // render i the thread of resposes.
    // So wait at least INTER_TWEET_DELAY_MS ms between posts.
    log.debug(`Acquiring mutex ${MUTEX_NAME_TWIT_POST}...`);
    var mutex_promise;
    
    if (mutex_client) {
      mutex_promise = mutex_client.acquire(MUTEX_NAME_TWIT_POST, {
        ttl: MUTEX_TWIT_POST_MAX_HOLD_MS, 
        retries: MUTEX_TWIT_POST_MAX_RETRIES, 
        lockRequestTimeout: MUTEX_TWIT_POST_MAX_WAIT_MS
      });
    }
    else {
      mutex_promise = Promise.resolve( { id: "id", key: "key"} );
    }
    
    mutex_promise.then( ({id, key}) => {
      log.debug(`Acquired mutex ${MUTEX_NAME_TWIT_POST}.`);
      T.post(
        "statuses/update",
        {
          status: tweetText,
          in_reply_to_status_id: replyToTweetId,
          auto_populate_reply_metadata: true
        },
        function(err, data, response) {
          if (err && err.code != 187) {
            log.debug(`Releasing mutex ${MUTEX_NAME_TWIT_POST}...`);
            log.debug(`Released mutex ${MUTEX_NAME_TWIT_POST}.`);
            handleError(err);
          } else {
            if (err && err.code == 187) {
              // This appears to be a "status is a duplicate" error which
              // means we are trying to resend a tweet we already sent.
              // Pretend we succeeded.
              log.error(`Received error 187 from T.post which means we already posted this tweet. Pretend we succeeded.`);

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
              log.info("Sent tweet...");
              log.debug(`Sent tweet: ${printTweet(data)}.`);
            }

            // Wait a bit. It seems tweeting a whackload of tweets in quick succession
            // can cause Twitter to think you're a troll bot or something and then some
            // of the tweets will not display for users other than the bot account.
            // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
            sleep(report_items_clone.length > 0 ? INTER_TWEET_DELAY_MS : 0).then( () => {
              // Don't release the mutex until after we sleep.
              log.debug(`Releasing mutex ${MUTEX_NAME_TWIT_POST}...`);

              if (mutex_client) {
                mutex_client.release(key, id).then( (v) => {
                  log.debug(`Released mutex ${MUTEX_NAME_TWIT_POST}.`);

                  // Send the rest of the responses. When those are sent, then resolve
                  // the local Promise.
                  SendResponses(mutex_client, T, data, report_items_clone)
                    .then(tweet => {
                      resolve(data);
                    })
                    .catch(err => {
                      handleError(err);
                    });
                }).catch( (err) => {
                  log.debug(`Error closing mutex ${MUTEX_NAME_TWIT_POST}.`);
                  handleError(err);
                });
              }
              else {
                log.info(`No mutex, skipping release.`);

                // Send the rest of the responses. When those are sent, then resolve
                // the local Promise.
                SendResponses(mutex_client, T, data, report_items_clone)
                  .then(tweet => {
                    resolve(data);
                  })
                  .catch(err => {
                    handleError(err);
                  });
              }

            }).catch( (err) => {
              handleError(err);
            });
          }
        }
      );
    }).catch( (err) => {
      handleError(err);
    });
  })
  .catch(e => {
    handleError(e);
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

function setLastDmId(lastDmId) {
  lastdmLog.info(`Writing last dm id ${lastDmId}.`);
  localStorage.setItem('lastdm', lastDmId);
}

function setLastMentionId(lastMentionId) {
  lastmentionLog.info(`Writing last mention id ${lastMentionId}.`);
  localStorage.setItem('lastmention', lastMentionId);
}

// Print out subset of tweet object properties.
function printTweet(tweet) {
  return (
    "Tweet: id: " +
    tweet.id +
    ", id_str: " +
    tweet.id_str +
    ", user: " +
    tweet.user.screen_name +
    ", in_reply_to_screen_name: " +
    tweet.in_reply_to_screen_name +
    ", in_reply_to_status_id: " +
    tweet.in_reply_to_status_id +
    ", in_reply_to_status_id_str: " +
    tweet.in_reply_to_status_id_str +
    ", " +
    tweet.full_text
  );
}

function handleError(error) {
  // Truncate the callstack because only the first few lines are relevant to this code.
  var stacktrace = error.stack
    .split("\n")
    .slice(0, 10)
    .join("\n");
  var formattedError = `===============================================================================\n${error.message}\n${stacktrace}`;

  log.error(formattedError);
  throw error;
}

function getTweetById(T, id) {
  // Quick check to fetch a specific tweet.
  var promise = Promise((resolve, reject) => {
    var retTweet;

    T.get(`statuses/show/${id}`, { tweet_mode: "extended" }, function(
      err,
      tweet,
      response
    ) {
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
const sleep = milliseconds => {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
};

// asynchronous query function to fetch all unprocessed request records.
// returns: promise
function GetRequestRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var request_records = [];

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
        request_records = result.Items;
      }
      resolve(request_records);
    });
  });
}

// asynchronous query function to fetch all citation records.
// returns: promise
function GetCitationRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();
  var citation_records = [];

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
    docClient.query(params, async function(err, result) {
      if (err) {
        handleError(err);
      } else {
        // 2. De-dupe the returned request_id's.
        var requestIDs = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id] = 1;
        });

        // 3. Check if we retrieved all the unprocessed citations
        if (
          result.hasOwnProperty("last_evaluated_key") &&
          result.last_evaluated_key
        ) {
          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex = 0;

          while (
            citation_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            var requestID = requestIDs[Object.keys(requestIDs[requestIndex])];
            requestIndex++;

            // 3a. Query for the citations for this request_id
            // Use the index which includes request_id.
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression =
              "#request_id = :rkey AND #processing_status = :pkey";
            params.ExpressionAttributeNames["#request_id"] = "request_id";
            params.ExpressionAttributeValues[":rkey"] = requestID;
            params.Limit = MAX_RECORDS_BATCH; // If there is a license with more citations than this... good enough

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH citations.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                citation_records.push(result.Items);
              }
            });
          }
        } else {
          citation_records = result.Items;
        }

        resolve(citation_records);
      }
    });
  });
}

function GetReportItemRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var report_item_records = [];

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
        var requestIDs = {};

        result.Items.forEach(function(item) {
          requestIDs[item.request_id] = 1;
        });

        // 3. Check if we retrieved all the unprocessed report items
        if (
          result.hasOwnProperty("last_evaluated_key") &&
          result.last_evaluated_key
        ) {
          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex = 0;

          while (
            report_item_records.length < MAX_RECORDS_BATCH &&
            requestIndex < Object.keys(requestIDs).length
          ) {
            var requestID = requestIDs[Object.keys(requestIDs[requestIndex])];
            requestIndex++;

            // 3a. Query for the report items for this request_id
            // Use the index which includes request_id.
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression =
              "#request_id = :rkey AND #processing_status = :pkey";
            params.ExpressionAttributeNames["#request_id"] = "request_id";
            params.ExpressionAttributeValues[":rkey"] = requestID;
            params.Limit = MAX_RECORDS_BATCH;

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH report items.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                report_item_records.push(result.Items);
              }
            });
          }
        } else {
          report_item_records = result.Items;
        }

        resolve(report_item_records);
      }
    });
  });
}

function WriteReportItemRecords(docClient, request_id, citation, report_items) {
  var docClient = new AWS.DynamoDB.DocumentClient();
  var truncated_report_items = [];

  // 1. Go through all report_items and split up any that will be > 280 characters when tweeted.
  // TODO: We should probably do this when processing the records, not before writing them.
  truncated_report_items = SplitLongLines(report_items, maxTweetLength);

  // 2. Build the report item records
  var now = Date.now();
  // TTL is 10 years from now until the records are PROCESSED
  var ttl_expire = new Date(now).setFullYear(new Date(now).getFullYear() + 10);
  var report_item_records = [];
  var record_num = 0;

  truncated_report_items.forEach(report_text => {
    var item = {
      PutRequest: {
        Item: {
          id: strUtils._getUUID(),
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

function getAccountID(T) {
  return new Promise( ( resolve, reject) => {
    T.get("account/verify_credentials", {}, function(err, data, response) {
      if (err) {
        handleError(err);
      }
      resolve(data.id);
    });    
  });
}

/*
 * Split array of strings to ensure each string is <= maxLen
 *
 * Params:
 *   source_lines: array of strings (each one may be multi-line)
 *   maxLen:       maximum length for each element in source_lines
 * Returns:
 *   array of strings matching source_lines but with any elements longer
 *   than maxLen, broken up into multiple entries, breaking on in order:
 *   - newlines (trailing newlines on broken elements are removed)
 *   - word breaks
 *   - if neither above exist, then just split at maxLen characters
 *
 * Note: elements in source_lines are not joined if < maxLen, only broken
 *       up if > maxLen
 **/
function SplitLongLines(source_lines, maxLen) {
  var truncated_lines = [];

  var index = 0;
  source_lines.forEach(source_line => {
    if (source_line.length > maxLen) {
      // break it up into lines to start with
      var chopped_lines = source_line.split("\n");
      var current_line = "";
      var first_line = true;

      chopped_lines.forEach(line => {
        if (line.length > maxLen) {
          // OK we have a single line that is too long for a tweet
          if (current_line.length > 0) {
            truncated_lines.push(current_line);
            current_line = "";
            first_line = true;
          }

          // word break it into multiple items
          var truncate_index = maxLen - 1;

          // Go back until we hit a whitespace characater
          while (truncate_index > 0 && !/\s/.test(line[truncate_index])) {
            truncate_index--;
          }

          if (truncate_index == 0) {
            // The line has no whitespace in it, just chop it in two
            truncate_index = maxLen - 1;
          }

          truncated_lines.push(line.substring(0, truncate_index + 1));

          // The rest of the string may still be too long.
          // Call ourselves recursively to split it up.
          var rest_truncated_lines = SplitLongLines(
            [line.substring(truncate_index + 1)],
            maxLen
          );
          truncated_lines = truncated_lines.concat(rest_truncated_lines);
        } else {
          if (current_line.length + line.length + 1 <= maxLen) {
            if (!first_line) {
              current_line += "\n";
            }
            current_line += line;
            first_line = false;
          } else {
            truncated_lines.push(current_line);

            // Start again
            current_line = line;
            first_line = true;
          }
        }
      });

      if (current_line.length > 0) {
        truncated_lines.push(current_line);
      }
    } else {
      truncated_lines.push(source_line);
    }
  });

  return truncated_lines;
}

