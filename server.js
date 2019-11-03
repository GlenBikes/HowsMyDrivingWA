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
  Mocha = require("mocha"),
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
  maxIdFileLen = 100,
  maxErrorFileLen = 100,
  lastDMFilename = "last_dm_id.txt",
  lastMentionFilename = "last_mention_id.txt",
  errorFilename = "error.txt";

console.log(`${process.env.TWITTER_HANDLE}: start`);

AWS.config.update({ region: "us-east-2" });

/*
console.log(`***Getting vehicle IDs for WA:334XYB.`);
seattle.GetVehicleIDs('334XYB', 'WA').then( function ( vehicleNums ) {
  console.log(`***Got ${vehicleNums.length} vehicles for WA:334XYB.`)
});
console.log(`***After getting vehicle IDs for WA:334XYB.`);

console.log(`***Getting citations for WA:334XYB.`);
seattle.GetCitationsByPlate('334XYB', 'WA').then( function ( citations ) {
  console.log(`***Got ${citations.length} citations for WA:334XYB.`)
});
console.log(`***After getting citations for WA:334XYB.`);
*/

/*
logTweetById("1185765762973065217");
logTweetById("1184633777714335746");
logTweetById("1184651910126530560");
*/

/*
AWS.config.getCredentials(function(err) {
  if (err) {
    console.log(err);
    console.log(err.stack);
  // credentials not loaded
  }
  else {
    console.log("Access key:", AWS.config.credentials.accessKeyId);
    console.log("Secret access key:", AWS.config.credentials.secretAccessKey);
  }
});
*/
var maxTweetLength = 280;  // TODO: Shorten this to account for the replying-to user account so we don't have calculations scattered all over the place
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

var myTokens = ":".split(":");

/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all("/tweet", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  /* Respond to @ mentions */
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
          var requests = [];

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
              if (status.hasOwnProperty('retweet_status')) {
                console.log(`Ignoring retweet: ${status.full_text}`);
              }
              else if (status.user.id == app_id) {
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
                requests.push(item);
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
          while (startPos < requests.length) {
            endPos = startPos + 25;
            if (endPos > requests.length) {
              endPos = requests.length;
            }

            let params = {
              RequestItems: {
                HMDWA_Request: requests.slice(startPos, endPos)
              }
            }

            var batchWritePromise = new Promise( (resolve, reject) => {
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
          console.log(`About to wait for batchWritePromises (${batchWritePromises.length}).`);
          Promise.all(batchWritePromises)
            .then(function(results) {
              console.log(`batchWritePromises resolved. Setting last_mention_id.txt (${maxTweetIdRead})`);
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

  // Run the tests.
  mocha.run(function(failures) {
    //T ODO: process.exitCode = failures ? 1 : 0;  // exit with non-zero status if there were failures
    response.sendStatus(200);
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
  if (request.query.hasOwnProperty("state") &&
    request.query.hasOwnProperty("plate")) {
    state = request.query.state;
    plate = request.query.plate;
  }

  seattle.GetCitationsByPlate(plate, state).then(function(citations) {
    var body = "Citations found:\n";

    if (!citations || citations.length == 0) {
      response.send(noCitations);
    } else {
      // TODO: Can we just send the whole array as json?
      response.json(citations);
    }
  }).catch( function ( err ) {
    handleError(err);
  });

  response.sendStatus(200);
});

app.all("/errors", function(request, response) {
  var fileName = `${__dirname}/${errorFilename}`;

  console.log(`Sending file: ${fileName}.`);
  response.sendFile(fileName);
});

app.all("/processrequests", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  var citations = [];

  // Query unprocessed requests
  var params = {
    TableName: "HMDWA_Request",
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

  docClient.query(params, function(err, data) {
    if (err) {
      handleError(err);
    } else {
      console.log(`Found ${data.Count} requests to process.`)

      if (data.Count > 0) {
        console.log(`Processing requests.`);
        // HACK! HACK! HACK! What to do about verbose?
        var verbose = true;

        data.Items.forEach( function( item ) {
          console.log(`Processing request ${item.id}.`);
          var citation_records = [];
          var tokens = item.license.split(":");
          var state;
          var plate;

          if (tokens.length == 2) {
            state = tokens[0];
            plate = tokens[1];
          }

          if (state == null || state == "" || plate == null || plate == "") {
            console.log(`Not a valid state/plate in this request (${state}/${plate}).`);
            // There was no valid plate found in the tweet. Add a dummy citation.
            var citation = new Object();
            var now = new Date().valueOf();
            citation.id = uuidv1(),
            citation.Citation = seattle.CitationIDNoCitationsFound;
            citation.request_id = item.id;
            citation.license = item.license;
            citation.created = now,
            citation.updated = now,
            citation.tweet_id = item.tweet_id;
            citation.tweet_id_str = item.tweet_id_str;
            citation.tweet_user_id = item.tweet_user_id;
            citation.tweet_user_id_str = item.tweet_user_id_str;
            citation.tweet_user_screen_name = item.tweet_user_screen_name;
            citation.processing_status = "UNPROCESSED";

            citation_records.push({
              PutRequest: {
                Item: citation
              }
            });
          } else {
            console.log(`Getting citations for ${state}:${plate}.`);
            
            seattle.GetCitationsByPlate(plate, state)
              .then(function(citations) {
              console.log(`Got ${citations.length} citations for ${state}:${plate}.`);
              if (!citations || citations.length == 0) {
                console.log(`No citations found for request ${item.id}.`);
                
                var citation = new Object();
                var now = new Date().valueOf();
                citation.id = uuidv1(),
                citation.Citation = seattle.CitationIDNoPlateFound;
                citation.request_id = item.id;
                citation.license = item.license;
                citation.created = now,
                citation.updated = now,
                citation.tweet_id = item.tweet_id;
                citation.tweet_id_str = item.tweet_id_str;
                citation.tweet_user_id = item.tweet_user_id;
                citation.tweet_user_id_str = item.tweet_user_id_str;
                citation.tweet_user_screen_name = item.tweet_user_screen_name;
                citation.processing_status = "UNPROCESSED";

                mungeCitation(citation);

                citation_records.push({
                  PutRequest: {
                    Item: citation
                  }
                });
              } else {
                citations.forEach( (citation) => {
                  console.log(`Processing citation ${citation.Citation}.`);
                  var now = new Date().valueOf();

                  // Add the primary and sort keys
                  citation.id = uuidv1(),
                  citation.created = now,
                  citation.modified = now,
                  citation.request_id = item.id;
                  citation.license = item.license;
                  citation.tweet_id = item.tweet_id;
                  citation.tweet_id_str = item.tweet_id_str;
                  citation.tweet_user_id = item.tweet_user_id;
                  citation.tweet_user_id_str = item.tweet_user_id_str;
                  citation.tweet_user_screen_name = item.tweet_user_screen_name;
                  citation.processing_status = "UNPROCESSED";

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
                    HMDWA_Citations: citation_records.slice(startPos, endPos)
                  }
                };

                var batchWritePromise = new Promise( (resolve, reject) => {
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
                  console.log(`batchWrite of citations successful, updating request ${item.id}.`);
                  var params = {
                    TableName: "HMDWA_Request",
                    Key: {
                      id: item.id
                    },
                    AttributeUpdates: {
                      'processing_status': {
                        Action: 'PUT',
                        Value: "CITATIONS_WRITTEN"
                      },
                      'modified': {
                        Action: 'PUT',
                        Value: new Date().valueOf()
                      }
                    }
                  };

                  console.log(`About to update request ${item.id}.`);
                
                  docClient.update(params, function(err, data) {
                    if (err) {
                      handleError(err);
                    } else {
                      console.log(`Updated request id=${item.id}.`);
                    }
                  });
                })
                .catch(function(e) {
                  handleError(e);
                });
            });
          }
        });
      }
      else {
        console.log("No requests found to process.");
      }
    }
  });

  response.sendStatus(200);
});

app.all("/processcitations", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  GetCitationRecords().then( function(citations) {
    console.log(`Processing ${citations.length} citations...`);
    
    var citationsByRequest = {};
    
    // Sort them based on request
    citations.forEach( function( citation ) {
      if (!(citation.request_id in citationsByRequest)) {
        citationsByRequest[citation.request_id] = new Array();
      }

      citationsByRequest[citation.request_id].push(citation);
    });
    
    console.log(`Got ${citations.length} citations from ${Object.keys(citationsByRequest).length} requests.`)

    // Now process the citations, on a per-request basis
    Object.keys(citationsByRequest).forEach(function(request_id) {
      console.log(`Processing citations for request_id: ${request_id}.`);
      debugger;
      
      // Get the first citation to access citation columns
      var citation = citationsByRequest[request_id][0];
      console.log(`Calling seattle.ProcessCitationsForRequest with ${citationsByRequest[request_id].length} citations.`);
      var report_items = seattle.ProcessCitationsForRequest(
        citationsByRequest[request_id]
      );

      console.log("general_summary: " + report_items[0]);
      console.log("detailed_list: " + report_items[1]);
      console.log("type_summary: " + report_items[2]);
      console.log("temporal_summary: " + report_items[3]);
      console.log("monetary_summary: " + "not implemented");

      var report_records = [];
      var now = new Date().valueOf();
      
      report_items.forEach( report_item => {
        var report_record = {
          id: uuidv1(),
          created: now,
          modified: now,
          full_text: report_item,
          request_id: citation.request_id,
          tweet_id: citation.tweet_id,
          tweet_id_str: citation.tweet_id_str,
          license: citation.license,
          processing_status: "UNPROCESSED",
          tweet_user_id: citation.tweet_user_id,
          tweet_user_id_str: citation.tweet_user_id_str,
          tweet_user_screen_name: citation.tweet_user_screen_name
        }
        report_records.push({
          PutRequest: {
            Item: report_record
          }
        });
      });

      // Write the tweets to the db
      // DynamoDB batchWrite only supports up to 25 items
      var batchWritePromises = [];
      var startPos = 0;
      var endPos;

      while (startPos < report_items.length) {
        endPos = startPos + 25;
        if (endPos > report_items.length) {
          endPos = report_items.length;
        }

        let params = {
          RequestItems: {
            HMDWA_ReportItems: report_records.slice(startPos, endPos)
          }
        };

        var batchWritePromise = new Promise( (resolve, reject) => {
            docClient.batchWrite(params, function(err, data) {
            if (err) {
              handleError(err);
            } else {
              resolve(data);
            }
          });
        });
        
        batchWritePromises.push(batchWritePromise);

        startPos = endPos;
      }

      // Wait for all the citation writes to complete to
      // ensure we don't update the request in the case
      // where one or more of these writes failed.
      console.log(`Waiting for all ${batchWritePromises.length} promises to resolve for tweet batchWrite operations.`);
      Promise.all(batchWritePromises)
        .then(function(results) {
        console.log(`batchWrite promises for tweets have finished.`)
      }).catch(function(err) {
        handleError(err);
      });

      // Set the processing status of all the citations
      // DynamoDB has no way to do a bulk update of items based on a query
      // of an index which is in the WFT?! bucket. But here we are.
      // Build an explicit update.
      // Write the tweets to the db
      // DynamoDB batchWrite only supports up to 25 items
      var citation_records = [];
      var now = new Date().valueOf();
      
      citationsByRequest[request_id].forEach( (citation) => {
        console.log(`In for loop. citation: ${printObject(citation)}, citationsByRequest: ${printObject(citationsByRequest)})`);
        
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

      batchWritePromises = [];
      while (startPos < citation_records.length) {
        endPos = startPos + 25;
        if (endPos > citation_records.length) {
          endPos = citation_records.length;
        }

        let params = {
          RequestItems: {
            HMDWA_Citations: citation_records.slice(startPos, endPos)
          }
        };

        // Let this go asynchronously.
        var batchWritePromise = docClient.batchWrite(params, function(err, data) {
          if (err) {
            handleError(err);
          } else {
            console.log(
              "Batch update of citations to PROCESSED: Success",
              data
            );
          }
        });
        
        batchWritePromises.push(batchWritePromise);

        startPos = endPos;
      }
      
      // Wait for all the batchWrite operations to succeed.
      console.log(`Waiting for all ${batchWritePromises.length} promises to resolve for updating citation batchWrite operations.`);
      Promise.all(batchWritePromises)
        .then(function(results) {
        console.log(`batchWrite promises for citation updates have finished.`)
      }).catch(function(err) {
        handleError(err);
      });
    });
  }).catch ( err => {
    handleError(err);
  });

  response.sendStatus(200);
});

app.all("/processreportitems", function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();

  GetReportItemRecords().then( function( report_items ) {
    console.log(`Processing ${report_items.length} report_items...`);
    
    debugger;
    
    var reportItemsByRequest = {};
    
    // Sort them based on request
    report_items.forEach( report_item => {
      if (!(report_item.request_id in reportItemsByRequest)) {
        reportItemsByRequest[report_item.request_id] = new Array();
      }

      reportItemsByRequest[report_item.request_id].push(report_item);
    });

    // Now process the report_items, on a per-request basis
    Object.keys(reportItemsByRequest).forEach( request_id => {
      // Get the first report_item to access report_item columns
      var report_item = reportItemsByRequest[request_id][0];
      // Build a fake tweet for the request report_item
      var origTweet = {
        id: report_item.tweet_id,
        id_str: report_item.tweet_id_str,
        user: {
          screen_name: report_item.tweet_user_screen_name
        }
      }
    
      SendResponses(origTweet, report_items);

      // Set the processing status of all the report_items
      // DynamoDB has no way to do a bulk update of items based on a query
      // of an index which is in the WFT?! bucket. But here we are.
      // Build an explicit update.
      // Write the report_items to the db
      // DynamoDB batchWrite only supports up to 25 items
      var report_item_records = [];
      var now = new Date().valueOf();
      
      reportItemsByRequest[request_id].forEach( report_item => {
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
            HMDWA_ReportItems: report_item_records.slice(startPos, endPos)
          }
        };

        // Do the updates in batches of 25.
        var batchWritePromise = docClient.batchWrite(params, function(err, data) {
          if (err) {
            handleError(err);
          } else {
            console.log(
              "Batch update of report_items to PROCESSED: Success",
              data
            );
          }
        });
        
        batchWritePromises.push(batchWritePromise);

        startPos = endPos;
      }
      
      // Wait for all the batchWrite operations to succeed.
      console.log(`Waiting for all ${batchWritePromises.length} promises to resolve for updating report_item batchWrite operations.`);
      Promise.all(batchWritePromises)
        .then(function(results) {
        console.log(`batchWrite promises for report_item updates have finished.`)
      }).catch(function(err) {
        handleError(err);
      });
    })
  }).catch ( function ( err ) {
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
    state: state.toUpperCase(),
    plate: plate.toUpperCase(),
    verbose: verbose
  };
}

function mungeCitation( citation ) {
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

  //Object.keys(columns).forEach(function(key) {
  for (var columnName in columns) {
    if (citation.hasOwnProperty(columnName)) {
      if (citation[columnName] == null || citation[columnName] == "") {
        citation[columnName] = columns[columnName];
      }
    }
  }
}

function SendResponses(origTweet, report_items) {
  if (tweets.report_items == 0) {
    return;
  }
  
  debugger;
  
  var report_item = report_items.shift();
  var replyToScreenName = origTweet.user.screen_name;
  var replyToTweetId = origTweet.id_str;

  try {
    /* Now we can respond to each tweet. */
    var tweetText = "@" + replyToScreenName + " " + report_item.full_text;
    new Promise(function(resolve, reject) {
      T.post(
        "statuses/update",
        {
          status: tweetText,
          in_reply_to_status_id: replyToTweetId,
          auto_populate_reply_metadata: true
        },
        function(err, data, response) {
          if (err) {
            reject(err);
          } else {
            console.log(`Sent tweet: ${printTweet(data)}.`)
            resolve(data);
          }
        }
      );
    }).then( function( sentTweet ) {
      // Wait a bit. It seems tweeting a whackload of tweets in quick succession
      // can cause Twitter to think you're a troll bot or something and then some
      // of the tweets will not display for users other than the bot account.
      // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
      SendResponses(sentTweet, report_items);
    })
    .catch( function( err ) {
      handleError(err);
    });
  } catch ( e ) {
    handleError(e);
  }
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
      /* strip off the date if present, it's only used for debugging. */
      /* First, let's load the ID of the last tweet we responded to. */
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

// synchronous query function to fetch all citation records
// TODO: Rewrite to only make multiple queries to citation table if there
//       are more than some max # of citations we can handle in memory.
// If the bot that processes citations (or tweets) is down for an extended
// period of time, this can easily return an amount of data that would
// consume more than available memory.
// However we can't just process the citations a page at a time
// because we wouldn't know if we have all the citations for a given request.
// In order to report the summaries of type/year/etc we need all of them.
// Perhaps in the case where paging happens, we could alternately just take
// the first citation returned and then query for all citations from that
// request and return only those, or do the citations for the the first n
// citations returned until we hit a max of 500 citations. This would eventually
// allow the bot processing citations to still process citations performantly
// and allow it to catch up as quickly as reasonable in the case where it gets
// overloaded due to itself or an upstream bot having been down for an extended
// period of time.
// NOTE: Could do a full table scan to find list of unique request_id's, but
// you pay AWS for the full table scan (i.e. $$$).
function GetCitationRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();
  
  console.log(`Getting citation records.`);
  
  var citation_records = [];
  
  // Query unprocessed requests
  var params = {
    TableName: "HMDWA_Citations",
    IndexName: "processing_status-index",
    Select: "ALL_ATTRIBUTES",

    KeyConditionExpression: "#processing_status = :pkey",
    ExpressionAttributeNames: {
      "#processing_status": "processing_status"
    },
    ExpressionAttributeValues: {
      ":pkey": "UNPROCESSED"
    },
    Limit: MAX_RECORDS_BATCH,  // If more than this, we'll have to handle it below
  };

  return new Promise( function (resolve, reject) {
    // 1. Do a query to get just the citations that are UNPROCESSED. 
    //    If the result is not complete, then we have to take the request_id's
    //    we got back and do individual queries for UNPROCESSED citations for
    //    each request_id. This ensures we process all the citations for a given
    //    request together. This is required cause we tweet out summaries/totals.
    docClient.query(params, async function(err, result) {
      if (err) {
        handleError(err);
      } else {
        console.log(`Citation query returned with ${result.Items.length} citations.`);

        // 2. De-dupe the returned request_id's.
        var requestIDs = {};

        result.Items.forEach( function (item) {
          requestIDs[item.request_id] = 1;
        })

        // 3. Check if we retrieved all the unprocessed citations
        if (result.hasOwnProperty("last_evaluated_key") && result.last_evaluated_key) {
          console.log("We are paging!");

          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex = 0;
          
          while (citation_records.length < MAX_RECORDS_BATCH && requestIndex < Object.keys(requestIDs).length) {
            var requestID = requestIDs[Object.keys(requestIDs[requestIndex])];
            requestIndex++;
            
            // 3a. Query for the citations for this request_id
            console.log(`Querying for citations for request_id ${requestID}, processing status UNPROCESSED.`);

            // Use the index which includes request_id.
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression = "#request_id = :rkey AND #processing_status = :pkey";
            params.ExpressionAttributeNames["#request_id"] = "request_id";
            params.ExpressionAttributeValues[":rkey"] = requestID;
            params.Limit = MAX_RECORDS_BATCH; // If there is a license with more citations than this... good enough

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH citations.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                console.log(`Citation query for request_id ${requestID} returned with ${result.Items.length} citations.`);

                citation_records.push(result.Items);
              }
            });
          }
        }
        else {
          console.log(`Not paging. Returning all ${result.Items.length} citations.`);
          citation_records = result.Items;
        }
        
        console.log(`Resolving GetCitationRecords promise with ${citation_records.length} citations.`);
        resolve(citation_records);
      }
    });
  });
}

function GetReportItemRecords() {
  var docClient = new AWS.DynamoDB.DocumentClient();
  
  console.log(`Getting report item records.`);
  
  var report_item_records = [];
  
  // Query unprocessed requests
  var params = {
    TableName: "HMDWA_ReportItems",
    IndexName: "processing_status-index",
    Select: "ALL_ATTRIBUTES",

    KeyConditionExpression: "#processing_status = :pkey",
    ExpressionAttributeNames: {
      "#processing_status": "processing_status"
    },
    ExpressionAttributeValues: {
      ":pkey": "UNPROCESSED"
    },
    Limit: MAX_RECORDS_BATCH,  // If more than this, we'll have to handle it below
  };

  return new Promise( function (resolve, reject) {
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
        console.log(`Report item query returned with ${result.Items.length} report items.`);

        // 2. De-dupe the returned request_id's.
        var requestIDs = {};

        result.Items.forEach( function (item) {
          requestIDs[item.request_id] = 1;
        })

        // 3. Check if we retrieved all the unprocessed report items
        if (result.hasOwnProperty("last_evaluated_key") && result.last_evaluated_key) {
          console.log("We are paging through report items!");

          // Paging!!!!
          // Fall back to making additional query for each request_id we have, looping
          // until we get MAX_RECORDS_BATCH records.
          var requestIndex = 0;
          
          while (report_item_records.length < MAX_RECORDS_BATCH && requestIndex < Object.keys(requestIDs).length) {
            var requestID = requestIDs[Object.keys(requestIDs[requestIndex])];
            requestIndex++;
            
            // 3a. Query for the report items for this request_id
            console.log(`Querying for report items for request_id ${requestID}, processing status UNPROCESSED.`);

            // Use the index which includes request_id.
            params.IndexName = "request_id-processing_status-index";
            params.KeyConditionExpression = "#request_id = :rkey AND #processing_status = :pkey";
            params.ExpressionAttributeNames["#request_id"] = "request_id";
            params.ExpressionAttributeValues[":rkey"] = requestID;
            params.Limit = MAX_RECORDS_BATCH;

            // Note: We are assuming that no single request will result in > MAX_RECORDS_BATCH report items.
            await docClient.query(params, function(err, result) {
              if (err) {
                handleError(err);
              } else {
                console.log(`Report ite, query for request_id ${requestID} returned with ${result.Items.length} report items.`);

                report_item_records.push(result.Items);
              }
            });
          }
        }
        else {
          console.log(`Not paging. Returning all ${result.Items.length} report items.`);
          report_item_records = result.Items;
        }
        
        console.log(`Resolving GetReportItemRecords promise with ${report_item_records.length} report items.`);
        resolve(report_item_records);
      }
    });
  });
}

function WriteReportItemRecords( request_id, report_items ) {
  var truncated_report_items = [];
  var docClient = new AWS.DynamoDB.DocumentClient();

  // 1. Go through all report_items and split up any that will be > 280 characters when tweeted.
  // TODO: We should probably do this when processing the records, not before writing them.
  report_items = SplitLongLines(report_items, maxTweetLength);
  
  // 2. Build the tweet records
  var now = new Date().valueOf();
  var report_item_records = [];
  for (const tweet_text in truncated_report_items) {
    var item = {
      PutRequest: {
        Item: {
          id: uuidv1(),
          request_id: request_id,
          created: now,
          modified: now,
          processing_status: "UNPROCESSED",
          tweet_text: tweet_text
        }
      }
    };
    
    report_item_records.push(item);
  }
  
  // 3. Write the tweet records. DynamoDB batchWrite only supports up to 25 items at a time.
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
        HMDWA_ReportItems: report_item_records.slice(startPos, endPos)
      }
    };

    var batchWritePromise = new Promise( (resolve, reject) => {
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

  // 4. Wait for all the tweet writes to complete.
  console.log(`About to wait for batchWritePromises (${batchWritePromises.length}).`);
  Promise.all(batchWritePromises)
    .then(function(results) {
      console.log(`Finished writing ${report_item_records.length} tweet records for request ${request_id}.`);
    
      var params = {
        TableName: "HMDWA_Citations",
        IndexName: "request_id-index",
        Select: "ALL_ATTRIBUTES",

        KeyConditionExpression: "#request_id = :reqid",
        ExpressionAttributeNames: {
          "#request_id": "request_id"
        },
        ExpressionAttributeValues: {
          ":reqid": request_id
        },
        Limit: MAX_RECORDS_BATCH  // If more than this, we already ignored them. // TODO: We should still page here to set them all to PROCESSED.
      };
    
      // 5. Query for the citation records for this request_id.
      docClient.query(params, function(err, result) {
        if (err) {
          handleError(err);
        } else {
          console.log(`Citation query (for update to PROCESSED) returned with ${result.Items.length} citations.`);
          
          // 6. Buid the update records.
        }
      });
    
    
    
    
    

      // 1. Do a query to get just the request_id values for citations that are UNPROCESSED. 
      //    If the result is not complete, that's fine, we will get the rest of them 
      //    the next time the bot wakes us up.
      docClient.query(params, function(err, result) {
        if (err) {
          handleError(err);
        } else {
          console.log(`Citation query returned with ${result.Items.length} citations.`);
        }
      });   
    })
    .catch(function(e) {
      handleError(e);
    });
}

function SplitLongLines(source_lines, maxLen) {
  var truncated_lines = [];
  
  source_lines.forEach( (source_line) => {
    // Break up long lines into lines < maxLen characters.
    if (source_line.length > maxLen) {
      // break it up into lines to start with
      var chopped_lines = source_line.split('\n');

      chopped_lines.forEach( (line) => {
        if (line.length > maxLen) {
          // OK we have a single line that is too long for a tweet
          // word break it into multiple report_items.
          var truncate_index = maxLen-1;

          // Go back until we hit a whitespace characater
          while (truncate_index > 0 && !(/\s/.test(line[truncate_index]))) {
            truncate_index--;
          }

          if (truncate_index == 0) {
            // The line has no whitespace in it, just chop it in two
            truncate_index = maxLen-1;
          }

          truncated_lines.push(line.substring(0, truncate_index+1));
          
          // The rest of the string may still be too long.
          // Call ourselves recursively to split it up.
          var rest_truncated_lines = SplitLongLines([ line.substring(truncate_index + 1) ], maxLen);
          truncated_lines = truncated_lines.concat(rest_truncated_lines);
        } else {
          truncated_lines.push(line);
        }
      });
    }
    else {
      truncated_lines.push(source_line);
    }
  });
  
  return truncated_lines;
}

// Shorten a string to less than maxLen characters without truncating words.
function shorten(str, maxLen, separator = ' ') {
  if (str.length <= maxLen) return str;
  return str.substr(0, str.lastIndexOf(separator, maxLen));
}
