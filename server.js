// Exported functions for tests
module.exports = {
  _chompTweet: chompTweet,
  _parseTweet: parseTweet,
  _splitLines: SplitLongLines
};

import JSON2HTML from "./json2html/json2html.js";

/* Setting things up. */
var AWS = require("aws-sdk"),
  fs = require("fs"),
  path = require("path"),
  express = require("express"),
  app = express(),
  soap = require("soap"),
  Twit = require("twit"),
  convert = require("xml-js"),
  uuidv1 = require("uuid/v1"),
  license = require("./opendata/licensehelper"),
  seattle = require("./opendata/seattle"),
  config = {
    twitter: {
      consumer_key: process.env.CONSUMER_KEY,
      consumer_secret: process.env.CONSUMER_SECRET,
      access_token: process.env.ACCESS_TOKEN,
      access_token_secret: process.env.ACCESS_TOKEN_SECRET
    }
  },
  T = new Twit(config.twitter);

const MAX_RECORDS_BATCH = 2000,
  INTER_TWEET_DELAY_MS = (process.env.hasOwnProperty("INTER_TWEET_DELAY_MS") && process.env.INTER_TWEET_DELAY_MS > 0) ? process.env.INTER_TWEET_DELAY_MS : 1000,
  maxIdFileLen = 100,
  maxErrorFileLen = 100,
  lastDMFilename = "last_dm_id.txt",
  lastMentionFilename = "last_mention_id.txt",
  errorFilename = "error.txt",
  tableNames = {
    Request: `${process.env.DB_PREFIX}_Request`,
    Citations: `${process.env.DB_PREFIX}_Citations`,
    ReportItems: `${process.env.DB_PREFIX}_ReportItems`
  }

console.log(`${process.env.TWITTER_HANDLE}: start`);

AWS.config.update({ region: "us-east-2" });

var maxTweetLength = 280 - 17; // Max username is 15 chars + '@' plus the space after the full username
var tweets = [];
var noCitations = "No citations found for plate # ";
var parkingAndCameraViolationsText =
  "Total parking and camera violations for #";
var violationsByYearText = "Violations by year for #";
var violationsByStatusText = "Violations by status for #";
var licenseRegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;
var botScreenNameRegexp = new RegExp(
  "@" + process.env.TWITTER_HANDLE + "\\b",
  "ig"
);

app.use(express.static("public"));

var listener = app.listen(process.env.PORT, function() {
  console.log(`Your bot is running on port ${listener.address().port}`);
});

/* tracks the largest tweet ID retweeted - they are not processed in order, due to parallelization  */
var app_id = -1;

/* Get the current user account (victimblame) */
T.get("account/verify_credentials", {}, function(err, data, response) {
  if (err) {
    handleError(err);
    return false;
  }
  app_id = data.id;
});

/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all("/tweet", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  /* First, let's load the ID of the last tweet we responded to. */
  var last_mention_id = getLastMentionId();
  if (last_mention_id != undefined) {
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

        if (data.statuses.length) {
          var request_records = [];

          /* 
          Iterate over each tweet. 

          The replies can occur concurrently, but the threaded replies to each tweet must, 
          within that thread, execute sequentially. 

          Since each tweet with a mention is processed in parallel, keep track of largest ID
          and write that at the end.
          */
          var maxTweetIdRead = -1;
          data.statuses.forEach(function(status) {
            console.log(`Found ${printTweet(status)}`);

            if (maxTweetIdRead < status.id_str) {
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
                console.log(`Ignoring retweet: ${status.full_text}`);
              } else if (status.user.id == app_id) {
                console.log("Ignoring our own tweet: " + status.full_text);
              } else {
                const { state, plate, verbose } = parseTweet(chomped_text);
                var now = new Date().valueOf();
                var item = {
                  PutRequest: {
                    Item: {
                      id: uuidv1(),
                      license: `${state}:${plate}`,
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
              console.log(
                "Ignoring reply that didn't actually reference bot: " +
                  status.full_text
              );
            }
          });

          var startPos = 0;
          var endPos = 25;

          var startPos = 0;
          var endPos;
          var batchWritePromises = [];
          while (startPos < request_records.length) {
            endPos = startPos + 25;
            if (endPos > request_records.length) {
              endPos = request_records.length;
            }

            let params = {
              RequestItems: {
                [`${tableNames['Request']}`]: request_records.slice(startPos, endPos)
              }
            };

            var batchWritePromise = new Promise((resolve, reject) => {
              docClient.batchWrite(params, function(err, data) {
                if (err) {
                  handleError(err);
                }
                
                resolve(data);
              });
            });

            batchWritePromises.push(batchWritePromise);

            startPos = endPos;
          }

          // Wait for all the request writes to complete to
          // ensure none of the writes failed.
          Promise.all(batchWritePromises)
            .then(function(results) {
              if (maxTweetIdRead > last_mention_id) {
                setLastMentionId(maxTweetIdRead);
              }
            })
            .catch(function(e) {
              handleError(e);
            });
        } else {
          /* No new mentions since the last time we checked. */
          console.log("No new mentions...");
        }
      }
    );
  }

  /* Respond to DMs */
  /* Load the ID of the last DM we responded to. */
  var last_dm_id = getLastDmId();
  if (last_dm_id == undefined) {
    handleError(new Error("ERROR: No last dm found! Defaulting to zero."));
    last_dm_id = 0;
  }

  T.get("direct_messages", { since_id: last_dm_id, count: 200 }, function(
    err,
    dms,
    response
  ) {
    /* Next, let's DM's to our bot, starting after the last DM we responded to. */
    if (dms.length) {
      dms.forEach(function(dm) {
        console.log(
          `Direct message: sender (${dm.sender_id}) id_str (${dm.id_str}) ${dm.text}`
        );

        /* Now we can respond to each tweet. */
        T.post(
          "direct_messages/new",
          {
            user_id: dm.sender_id,
            text: "This is a test response."
          },
          function(err, data, response) {
            if (err) {
              /* TODO: Proper error handling? */
              handleError(err);
            } else {
              setLastDmId(dm.id_str);
            }
          }
        );
      });
    } else {
      /* No new DMs since the last time we checked. */
      console.log("No new DMs...");
    }
  });

  /* TODO: Handle proper responses based on whether the tweets succeed, using Promises. For now, let's just return a success message no matter what. */
  response.sendStatus(200);
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
  mocha.run()
  .on('test', function(test) {
      test_results += `Test started: ${test.title}\n`;
  })
  .on('test end', function(test) {
      //test_results += `Test done: ${test.title}\n`;
  })
  .on('pass', function(test) {
      test_results += `Test passed: ${test.title}\n\n`;
  })
  .on('fail', function(test, err) {
      test_results += `Test failed: ${test.title}\nError:\n${err}\n\n`;
      failures = true;
  })
  .on('end', function() {
      test_results += "*********************************************\n\nTests all finished!\n\n********************************************";
      // send your email here
      console.log(test_results);
      if (!failures) {  
        response.sendStatus(200);
      }
      else {
        // Send back the test results.
        response.status(500).send(test_results);
      }
  });  
});

app.all("/dumpfile", function(request, response) {
  var fileName = `${__dirname}/${lastMentionFilename}`;

  if (request.query.hasOwnProperty("filename")) {
    fileName = `${__dirname}/${request.query.filename}`;
  }
  console.log(`Sending file: ${fileName}.`);
  response.sendFile(fileName);
});

app.all("/dumptweet", function(request, response) {
  if (request.query.hasOwnProperty("id")) {
    var tweet = getTweetById(request.query.id);
    response.set("Cache-Control", "no-store");
    response.json(tweet);
  } else {
    handleError(new Error("Error: id is required for /dumptweet"));
  }
});

app.all("/dumpcitations", function(request, response) {
  var state = "WA";
  var plate = "334XYB";
  if (
    request.query.hasOwnProperty("state") &&
    request.query.hasOwnProperty("plate")
  ) {
    state = request.query.state;
    plate = request.query.plate;
  }

  seattle
    .GetCitationsByPlate(plate, state)
    .then(function(citations) {
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

  response.sendStatus(200);
});

app.all("/errors", function(request, response) {
  var fileName = `${__dirname}/${errorFilename}`;

  console.log(`Sending file: ${fileName}.`);
  response.sendFile(fileName);
});

// uptimerobot.com hits this every 5 minutes
app.all("/processrequests", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  GetRequestRecords()
    .then(request_records => {
      if (request_records && request_records.length > 0) {
        debugger;
        request_records.forEach(function(item) {
          var citation_records = [];
          var tokens = item.license.split(":");
          var state;
          var plate;

          if (tokens.length == 2) {
            state = tokens[0];
            plate = tokens[1];
          }

          if (state == null || state == "" || plate == null || plate == "") {
            console.log(
              `Not a valid state/plate in this request (${state}/${plate}).`
            );
            // There was no valid plate found in the tweet. Add a dummy citation.
            var now = new Date().valueOf();
            var citation = {
              id: uuidv1(),
              Citation: seattle.CitationIDNoPlateFound,
              request_id: item.id,
              license: item.license,
              created: now,
              modified: now,
              tweet_id: item.tweet_id,
              tweet_id_str: item.tweet_id_str,
              tweet_user_id: item.tweet_user_id,
              tweet_user_id_str: item.tweet_user_id_str,
              tweet_user_screen_name: item.tweet_user_screen_name,
              processing_status: "UNPROCESSED"
            };

            citation_records.push({
              PutRequest: {
                Item: citation
              }
            });
            
            
            // Write the citations to DB
            // DynamoDB batchWrite only supports up to 25 items
            var startPos = 0;
            var endPos;
            var batchWritePromises = [];
            while (startPos < citation_records.length) {
              endPos = startPos + 25;
              if (endPos > citation_records.length) {
                endPos = citation_records.length;
              }

              let params = {
                RequestItems: {
                  [`${tableNames['Citations']}`]: citation_records.slice(startPos, endPos)
                }
              };

              var batchWritePromise = new Promise((resolve, reject) => {
                docClient.batchWrite(params, function(err, data) {
                  if (err) {
                    handleError(err);
                  }

                  // TODO: Handle data.UnprocessedItems
                  resolve(data);
                });
              });

              batchWritePromises.push(batchWritePromise);

              startPos = endPos;
            }

            // Wait for all the citation writes to complete to
            // ensure we don't update the request in the case
            // where one or more of these writes failed.
            Promise.all(batchWritePromises)
              .then(function(results) {
                var params = {
                  TableName: tableNames['Request'],
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
                      Value: new Date().valueOf()
                    }
                  }
                };

                docClient.update(params, function(err, data) {
                  if (err) {
                    handleError(err);
                  }
                });
              })
              .catch(function(e) {
                handleError(e);
              });

          
          } else {
            seattle.GetCitationsByPlate(plate, state).then(function(citations) {
              if (!citations || citations.length == 0) {
                var now = new Date().valueOf();
                var citation = {
                  id: uuidv1(),
                  Citation: seattle.CitationIDNoCitationsFound,
                  request_id: item.id,
                  license: item.license,
                  created: now,
                  modified: now,
                  tweet_id: item.tweet_id,
                  tweet_id_str: item.tweet_id_str,
                  tweet_user_id: item.tweet_user_id,
                  tweet_user_id_str: item.tweet_user_id_str,
                  tweet_user_screen_name: item.tweet_user_screen_name,
                  processing_status: "UNPROCESSED"
                }

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
                  var now = new Date().valueOf();

                  citation.id = uuidv1();
                  citation.request_id = item.id;
                  citation.processing_status = "UNPROCESSED";
                  citation.license = item.license;
                  citation.created = now;
                  citation.modified = now;
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
              
              
              // Write the citations to DB
              // DynamoDB batchWrite only supports up to 25 items
              var startPos = 0;
              var endPos;
              var batchWritePromises = [];
              while (startPos < citation_records.length) {
                endPos = startPos + 25;
                if (endPos > citation_records.length) {
                  endPos = citation_records.length;
                }

                let params = {
                  RequestItems: {
                    [`${tableNames['Citations']}`]: citation_records.slice(startPos, endPos)
                  }
                };

                var batchWritePromise = new Promise((resolve, reject) => {
                  docClient.batchWrite(params, function(err, data) {
                    if (err) {
                      handleError(err);
                    }

                    // TODO: Handle data.UnprocessedItems
                    resolve(data);
                  });
                });

                batchWritePromises.push(batchWritePromise);

                startPos = endPos;
              }

              // Wait for all the citation writes to complete to
              // ensure we don't update the request in the case
              // where one or more of these writes failed.
              Promise.all(batchWritePromises)
                .then(function(results) {
                  var params = {
                    TableName: tableNames['Request'],
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
                        Value: new Date().valueOf()
                      }
                    }
                  };

                  docClient.update(params, function(err, data) {
                    if (err) {
                      handleError(err);
                    }
                  });
                })
                .catch(function(e) {
                  handleError(e);
                });
              
              
            });
          }
        })
      } else {
        console.log("No request records found.");
      }
    })
    .catch(err => {
      handleError(err);
    });
  response.sendStatus(200);
});

app.all("/processcitations", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  GetCitationRecords().then(function(citations) {
    if (citations && citations.length > 0) {
      var citationsByRequest = {};

      // Sort them based on request
      citations.forEach(function(citation) {
        if (!(citation.request_id in citationsByRequest)) {
          citationsByRequest[citation.request_id] = new Array();
        }

        citationsByRequest[citation.request_id].push(citation);
      });

      // Now process the citations, on a per-request basis
      Object.keys(citationsByRequest).forEach(function(request_id) {
        // Get the first citation to access citation columns
        var citation = citationsByRequest[request_id][0];
        var report_items = seattle.ProcessCitationsForRequest(
          citationsByRequest[request_id]
        );

        // Write report items
        WriteReportItemRecords(request_id, citation, report_items)
          .then(function(results) {
            // Set the processing status of all the citations
            // DynamoDB has no way to do a bulk update of items based on a query
            // of an index which is in the WFT?! bucket. But here we are.
            // Build an explicit update.
            // Write the tweets to the db
            // DynamoDB batchWrite only supports up to 25 items
            var citation_records = [];
            var now = new Date().valueOf();

            citationsByRequest[request_id].forEach(citation => {
              citation.processing_status = "PROCESSED";
              citation.modified = now;

              citation_records.push({
                PutRequest: {
                  Item: citation
                }
              });
            });

            var startPos = 0;
            var endPos;

            var batchWritePromises = [];
            while (startPos < citation_records.length) {
              endPos = startPos + 25;
              if (endPos > citation_records.length) {
                endPos = citation_records.length;
              }

              let params = {
                RequestItems: {
                  [`${tableNames['Citations']}`]: citation_records.slice(startPos, endPos)
                }
              };

              // Let this go asynchronously.
              var batchWritePromise = docClient.batchWrite(params, function(
                err,
                data
              ) {
                if (err) {
                  handleError(err);
                }
              });

              batchWritePromises.push(batchWritePromise);

              startPos = endPos;
            }

            // Wait for all the batchWrite operations to succeed.
            Promise.all(batchWritePromises)
              .then(function(results) {
              })
              .catch(function(err) {
                handleError(err);
              });
          })
          .catch(function(e) {
            handleError(e);
          });
      });
    } else {
      console.log("No citations found.");
    }

    response.sendStatus(200);
  });
});

app.all("/processreportitems", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  GetReportItemRecords()
    .then(function(report_items) {
      if (report_items && report_items.length > 0) {
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

          // Send a copy of the report items to SendResponse since we need to
          SendResponses(origTweet, reportItemsByRequest[request_id])
            .then(() => {
              // Set the processing status of all the report_items
              // DynamoDB has no way to do a bulk update of items based on a query
              // of an index which is in the WFT?! bucket. But here we are.
              // Build an explicit update.
              // Write the report_items to the db
              // DynamoDB batchWrite only supports up to 25 items
              var report_item_records = [];
              var now = new Date().valueOf();

              reportItemsByRequest[request_id].forEach(report_item => {
                report_item.processing_status = "PROCESSED";
                report_item.modified = now;

                report_item_records.push({
                  PutRequest: {
                    Item: report_item
                  }
                });
              });

              var startPos = 0;
              var endPos;

              var batchWritePromises = [];
              while (startPos < report_item_records.length) {
                endPos = startPos + 25;
                if (endPos > report_item_records.length) {
                  endPos = report_item_records.length;
                }

                let params = {
                  RequestItems: {
                    [`${tableNames['ReportItems']}`]: report_item_records.slice(
                      startPos,
                      endPos
                    )
                  }
                };

                // Do the updates in batches of 25.
                var batchWritePromise = docClient.batchWrite(params, function(
                  err,
                  data
                ) {
                  if (err) {
                    handleError(err);
                  } 
                });

                batchWritePromises.push(batchWritePromise);

                startPos = endPos;
              }

              // Wait for all the batchWrite operations to succeed.
              Promise.all(batchWritePromises)
                .then(function(results) {
                })
                .catch(function(err) {
                  handleError(err);
                });
            })
            .catch(err => {
              handleError(err);
            });
        });
      } else {
        console.log("No report items found.");
      }
    })
    .catch(function(err) {
      handleError(err);
    });

  response.sendStatus(200);
});

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
  var verbose = false;

  if (/verbose/i.test(text)) {
    verbose = true;
  }

  const matches = licenseRegExp.exec(text);

  if (matches == null || matches.length < 2 || matches[1] == "XX") {
    handleError(new Error(`Error: No license found in tweet: ${text}`));
  } else {
    state = matches[1];
    plate = matches[2];

    if (license.StatesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      handleError(new Error(`Invalid state: ${state}`));
    }
  }

  return {
    state: state ? state.toUpperCase() : '',
    plate: plate ? plate.toUpperCase() : '',
    verbose: verbose
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
      if (citation[columnName] == null || citation[columnName] == "") {
        citation[columnName] = columns[columnName];
      }
    }
  }
}

function SendResponses(origTweet, report_items) {
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
  console.log(`Sending Tweet: ${tweetText}.`);
  return new Promise((resolve, reject) => {
    T.post(
      "statuses/update",
      {
        status: tweetText,
        in_reply_to_status_id: replyToTweetId,
        auto_populate_reply_metadata: true
      },
      function(err, data, response) {
        if (err) {
          handleError(err);
        } else {
          console.log(`Sent tweet: ${printTweet(data)}.`);

          // Wait a bit. It seems tweeting a whackload of tweets in quick succession
          // can cause Twitter to think you're a troll bot or something and then some
          // of the tweets will not display for users other than the bot account.
          // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
          sleep(INTER_TWEET_DELAY_MS).then(() => {
            // Send the rest of the responses. When those are sent, then resolve
            // the local Promise.
            SendResponses(data, report_items_clone)
              .then(tweet => {
                resolve(data);
              })
              .catch(err => {
                handleError(err);
              });
          });
        }
      }
    );
  });
}

/**
 * When investigating a selenium test failure on a remote headless browser that couldn't be reproduced
 * locally, I wanted to add some javascript to the site under test that would dump some state to the
 * page (so it could be captured by Selenium as a screenshot when the test failed). JSON.stringify()
 * didn't work because the object declared a toJSON() method, and JSON.stringify() just calls that
 * method if it's present. This was a Moment object, so toJSON() returned a string but I wanted to see
 * the internal state of the object instead.
 *
 * So, this is a rough and ready function that recursively dumps any old javascript object.
 */
function printObject(o, indent) {
  var out = "";
  if (typeof indent === "undefined") {
    indent = 0;
  }
  for (var p in o) {
    if (o.hasOwnProperty(p)) {
      var val = o[p];
      out += new Array(4 * indent + 1).join(" ") + p + ": ";
      if (typeof val === "object") {
        if (val instanceof Date) {
          out += 'Date "' + val.toISOString() + '"';
        } else {
          out +=
            "{\n" +
            printObject(val, indent + 1) +
            new Array(4 * indent + 1).join(" ") +
            "}";
        }
      } else if (typeof val === "function") {
      } else {
        out += '"' + val + '"';
      }
      out += ",\n";
    }
  }
  return out;
}

function getLastDmId() {
  return getLastIdFromFile(lastDMFilename);
}

function getLastMentionId() {
  return getLastIdFromFile(lastMentionFilename);
}

function getLastIdFromFile(filename) {
  const lineByLine = require("n-readlines", function(err) {
    if (err) {
      handleError(err);
    }
  });

  try {
    const filepath = `${__dirname}/${filename}`;
    const liner = new lineByLine(filepath);
    var lastIdRegExp = /\b(\d+)(: [\d\.\: ])?\b/;
    var lastId;
    let line;

    while ((line = liner.next())) {
      // strip off the date if present, it's only used for debugging.
      // First, let's load the ID of the last tweet we responded to.
      const matches = lastIdRegExp.exec(line);
      if (matches == null || matches.length < 1) {
        handleError(new Error(`Error: No last mention found: ${line}`));
      } else if (lastId == undefined) {
        lastId = matches[1];
        break;
      }
    }
  } catch (err) {
    handleError(err);
  }

  return lastId;
}

function setLastDmId(lastDmId) {
  console.log(`Writing last dm id ${lastDmId}.`);
  setLastIdInFile(lastDMFilename, lastDmId, maxIdFileLen);
}

function setLastMentionId(lastMentionId) {
  console.log(`Writing last mention id ${lastMentionId}.`);
  setLastIdInFile(lastMentionFilename, lastMentionId, maxIdFileLen);
}

function setLastIdInFile(filename, lastId, maxLines) {
  var filepath = `${__dirname}/${filename}`;
  var today = new Date();
  var newLine =
    lastId +
    ": " +
    today.toLocaleDateString() +
    " " +
    today.toLocaleTimeString();

  prependFile(filepath, newLine, maxLines);
}

function prependFile(filePath, newLines, maxLines) {
  // Read file into memory
  var textBuf = fs.readFileSync(filePath);
  var textByLines = textBuf.toString().split("\n");

  textByLines = newLines.split("\n").concat(textByLines);

  // truncate to specified number of lines
  textByLines = textByLines.slice(0, maxLines);

  fs.writeFileSync(filePath, textByLines.join("\n"), function(err) {
    if (err) {
      handleError(err);
    }
  });
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
  var filepath = `${__dirname}/${errorFilename}`;
  var today = new Date();
  var date = today.toLocaleDateString();

  //var time = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();
  var time = today.toLocaleTimeString();
  var dateTime = date + " " + time;

  console.log(`ERROR: ${error}`);

  // Truncate the callstack because only the first few lines are relevant to this code.
  var stacktrace = error.stack
    .split("\n")
    .slice(0, 10)
    .join("\n");
  var formattedError = `============================== ${dateTime} =========================================\n${error.message}\n${stacktrace}`;

  prependFile(filepath, formattedError, maxErrorFileLen);
}

function logTweetById(id) {
  // Quick check to fetch a specific tweet and dump it fullyt
  var tweet = getTweetById(id);

  if (tweet) {
    console.log(`logTweetById (${id}): ${printObject(tweet)}`);
  }
}

function getTweetById(id) {
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

  promise.then(function(tweet) {
    return tweet;
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
    TableName: tableNames['Request'],
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
    TableName: tableNames['Citations'],
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
    TableName: tableNames['ReportItems'],
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

function WriteReportItemRecords(request_id, citation, report_items) {
  var docClient = new AWS.DynamoDB.DocumentClient();
  var truncated_report_items = [];

  // 1. Go through all report_items and split up any that will be > 280 characters when tweeted.
  // TODO: We should probably do this when processing the records, not before writing them.
  truncated_report_items = SplitLongLines(report_items, maxTweetLength);

  // 2. Build the report item records
  var now = new Date().valueOf();
  var report_item_records = [];
  var record_num = 0;

  truncated_report_items.forEach(report_text => {
    var item = {
      PutRequest: {
        Item: {
          id: uuidv1(),
          request_id: request_id,
          record_num: record_num++,
          created: now,
          modified: now,
          processing_status: "UNPROCESSED",
          license: citation.lencense,
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

  // 3. Write the report item records. DynamoDB batchWrite only supports up to 25 items at a time.
  var startPos = 0;
  var endPos;
  var batchWritePromises = [];
  while (startPos < report_item_records.length) {
    endPos = startPos + 25;
    if (endPos > report_item_records.length) {
      endPos = report_item_records.length;
    }

    let params = {
      RequestItems: {
        [`${tableNames['ReportItems']}`]: report_item_records.slice(startPos, endPos)
      }
    };

    var batchWritePromise = new Promise((resolve, reject) => {
      docClient.batchWrite(params, function(err, data) {
        if (err) {
          handleError(err);
        }

        // TODO: Handle data.UnprocessedItems
        resolve(data);
      });
    });

    batchWritePromises.push(batchWritePromise);

    startPos = endPos;
  }

  // 4. Wait for all the report item writes to complete.
  return Promise.all(batchWritePromises);
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
            current_line = '';
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
