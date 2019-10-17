// Exported functions
module.exports = {
  _chompTweet: chompTweet,
  _parseTweet: parseTweet
};

/* Setting things up. */
var fs = require('fs'),
    path = require('path'),
    express = require('express'),
    app = express(),   
    soap = require('soap'),
    Twit = require('twit'),
    convert = require('xml-js'),
    Mocha = require('mocha'),
    license = require('./opendata/licensehelper'),
    seattle = require('./opendata/seattle'),
    config = {
    /* Be sure to update the .env file with your API keys. See how to get them: https://botwiki.org/tutorials/make-an-image-posting-twitter-bot/#creating-a-twitter-app*/      
      twitter: {
        consumer_key: process.env.CONSUMER_KEY,
        consumer_secret: process.env.CONSUMER_SECRET,
        access_token: process.env.ACCESS_TOKEN,
        access_token_secret: process.env.ACCESS_TOKEN_SECRET
      }
    },
    T = new Twit(config.twitter),
    maxIdFileLen = 100,
    maxErrorFileLen = 100,
    lastDMFilename = "last_dm_id.txt",
    lastMentionFilename = "last_mention_id.txt",
    errorFilename = "error.txt";

console.log(`${process.env.TWITTER_HANDLE}: start`);
/*
logTweetById("1184633776762228736");
logTweetById("1184633777714335746");
logTweetById("1184651910126530560");
*/


/*
  // Quick check to fetch a specific tweet and dump it fully.
  // TODO: Move to app.all("/dumptweet" ...
  T.get('statuses/show/1184608363344216066', function(err, tweet, response) {
    if (err){
      handleError(err);
      return false;
    }

    console.log(`Retrieved tweet: ${printObject(tweet)}`);

    // Now get replies to that tweet. No way in the API to do this.
    console.log(`Searching for tweets to ${tweet.user.screen_name} since tweet ${tweet.id}`)
    
    T.get('search/tweets', { q: '@HMDWAUser', since_id: tweet.id, tweet_mode: 'extended' }, function(err, data, response) {
      if (err) {
        handleError(err);
        return false;
      }

      if (data.statuses.length) {
        var maxTweetIdRead = -1;
        data.statuses.forEach(function(replyTweet) {
          if (replyTweet.in_reply_to_status_id == tweet.id) {
            console.log(`Found reply!!!!\n ${printObject(replyTweet)}`);
          }
          else {
            console.log(`Found tweet ${printObject(replyTweet)}`)
          }
        });
      }
      else {
        console.log("Found no replies.");
      }
  });
});
*/
var maxTweetLength = 280;
var tweets = [];
var noCitations = "No citations found for plate # ";
var noValidPlate = "No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #";
var parkingAndCameraViolationsText = "Total parking and camera violations for #";
var violationsByYearText = "Violations by year for #";
var violationsByStatusText = "Violations by status for #";
var statesAndProvinces = [ 'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA', 'WA', 'WV', 'WI', 'WY', 'AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT' ];
var licenseRegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;
var botScreenNameRegexp = new RegExp("@" + process.env.TWITTER_HANDLE + "\\b", "ig");

app.use(express.static('public'));

var listener = app.listen(process.env.PORT, function () {
  console.log(`Your bot is running on port ${listener.address().port}`);
});

/* tracks the largest tweet ID retweeted - they are not processed in order, due to parallelization  */
var app_id = -1;

/* Get the current user account (victimblame) */
T.get('account/verify_credentials', { }, function(err, data, response) {
  if (err){
    handleError(err);
    return false;
  }
  app_id = data.id;
  console.log("Bot's id: " + app_id);
});

/* uptimerobot.com is hitting this URL every 5 minutes. */
app.all("/tweet", function (request, response) {
  /* Respond to @ mentions */
  var last_mention_id = getLastMentionId();
  if (last_mention_id != undefined) {
    /* First, let's load the ID of the last tweet we responded to. */
    console.log(`last_mention_id: ${last_mention_id}`);

    /* Next, let's search for Tweets that mention our bot, starting after the last mention we responded to. */
    T.get('search/tweets', { q: '%40' + process.env.TWITTER_HANDLE, since_id: last_mention_id, tweet_mode: 'extended' }, function(err, data, response) {
      if (err) {
        handleError(err);
        return false;
      }
      
      if (data.statuses.length){
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

          debugger;
      
          /*
          Make sure this isn't a reply to one of the bot's tweets which would
          include the bot screen name in full_text, but only due to replies.
          */
          var displayText = chompTweet(status);
          
          if (botScreenNameRegexp.test(displayText)) {
            /* Don't reply to our own tweets. */
            if (status.user.id == app_id) {
              console.log('Ignoring our own tweet: ' + status.full_text);
            }
            else {
              const {state, plate, verbose} = parseTweet(displayText);

              if (state == null || plate == null) {
                var tweets = [
                  noValidPlate
                ];

                SendResponses(status, tweets, verbose);
              }
              else {
                /* 
                These replies must be executed sequentially 
                with each one referencing the previous tweet in the thread. 
                */
                GetReplies(plate, state, verbose).then( function(tweets) {
                  SendResponses(status, tweets, verbose);
                });
              }
            }
          }
          else {
            console.log("Ignoring reply that didn't actually reference bot: " + status.full_text);
          }
        });
        
        if (maxTweetIdRead > last_mention_id) {
          setLastMentionId(maxTweetIdRead);
        }
      } else {
        /* No new mentions since the last time we checked. */
        console.log('No new mentions...');      
      }
    });    
  }

  /* Respond to DMs */
  /* Load the ID of the last DM we responded to. */
  var last_dm_id = getLastDmId();
  if (last_dm_id == undefined) {
    handleError(new Error("ERROR: No last dm found! Defaulting to zero."));
    last_dm_id = 0;
  }
  
  console.log(`last_dm_id: ${last_dm_id}`);

  T.get('direct_messages', { since_id: last_dm_id, count: 200 }, function(err, dms, response) {
    /* Next, let's DM's to our bot, starting after the last DM we responded to. */
    if (dms.length){
      dms.forEach(function(dm) {
        console.log(`Direct message: sender (${dm.sender_id}) id_str (${dm.id_str}) ${dm.text}`);

        /* Now we can respond to each tweet. */
        T.post('direct_messages/new', {
          user_id: dm.sender_id,
          text: "This is a test response."
        }, function(err, data, response) {
          if (err){
            /* TODO: Proper error handling? */
            handleError(err);
          }
          else{
            setLastDmId(dm.id_str);
          }
        });
      });
    } else {
      /* No new DMs since the last time we checked. */
      console.log('No new DMs...');      
    }
  });    
  
  /* TODO: Handle proper responses based on whether the tweets succeed, using Promises. For now, let's just return a success message no matter what. */
  response.sendStatus(200);
});

app.all("/test", function (request, response) {
  // Instantiate a Mocha instance.
  var mocha = new Mocha();

  var testDir = './test'

  // Add each .js file to the mocha instance
  fs.readdirSync(testDir).filter(function(file) {
      // Only keep the .js files
      return file.substr(-3) === '.js';

  }).forEach(function(file) {
      mocha.addFile(
          path.join(testDir, file)
      );
  });

  // Run the tests.
  mocha.run(function(failures) {
    //process.exitCode = failures ? 1 : 0;  // exit with non-zero status if there were failures
    // TODO: Handle proper responses based on whether the tweets succeed, using Promises. For now, let's just return a success message no matter what.
    response.sendStatus(200);
  });
});

app.all("/dumpfile", function (request, response) {
  var fileName = `${__dirname}/${lastMentionFilename}`;
  
  if (request.query.hasOwnProperty("filename")) {
    fileName = `${__dirname}/${request.query.filename}`;
  }
  console.log(`Sending file: ${fileName}.`)
  response.sendFile(fileName);
});

app.all("/errors", function (request, response) {
  var fileName = `${__dirname}/${errorFilename}`;

  console.log(`Sending file: ${fileName}.`)
  response.sendFile(fileName);
});

function chompTweet(tweet) {
  // Extended tweet objects include the screen name of the tweeting user within the full_text,  
  // as well as all replied-to screen names in the case of a reply.
  // Strip off those because if UserA tweets a license plate and references the bot and then
  // UserB replies to UserA's tweet without explicitly referencing the bot, we do not want to
  // process that tweet.
  var text = tweet.full_text;
  
  if (tweet.display_text_range != null && tweet.display_text_range.length >= 2) {
    text = tweet.full_text.substring(tweet.display_text_range[0]);
  }
  
  return text;
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
  }
  else {
    state = matches[1];
    plate = matches[2];

    if (statesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      handleError(new Error(`Invalid state: ${state}`));;
    }
  }
  
  return {
    state: state,
    plate: plate,
    verbose: verbose
  };
}


function GetReplies(plate, state, verbose) {
  var categorizedCitations = {};
  var chronologicalCitations = {};
  var violationsByYear = {};
  var violationsByStatus = {};
  verbose = true;

  return new Promise((resolve, reject) => {
    seattle.GetCitationsByPlate(plate, state).then(function(citations) {
      var tweets = [];

      if (!citations || Object.keys(citations).length == 0) {
        tweets.push(noCitations + license.formatPlate(plate, state));
      } else {
        Object.keys(citations).forEach(function (key) {
          var year = "Unknown";
          var violationDate = new Date(Date.now());
          
          console.log(printObject(citations[key]));
          
          try {
            violationDate = new Date(Date.parse(citations[key].ViolationDate));
          }
          catch ( e ) {
            handleError(new Error(`Error parsing date ${citations[key].ViolationDate}.`));
            handleError(e);
          }
          
          if (!(violationDate in chronologicalCitations)) {
            chronologicalCitations[violationDate] = new Array();
          }
          
          chronologicalCitations[violationDate].push(citations[key]);
          
          if (!(citations[key].Type in categorizedCitations)) {
            categorizedCitations[citations[key].Type] = 0;
          }
          categorizedCitations[citations[key].Type]++;
          
          if (!(citations[key].Status in violationsByStatus)) {
            violationsByStatus[citations[key].Status] = 0;
          }
          violationsByStatus[citations[key].Status]++;

          year = violationDate.getFullYear();

          if (!(year in violationsByYear)) {
            violationsByYear[year] = 0;
          }

          violationsByYear[year]++;
        });

        var tweetText = parkingAndCameraViolationsText + license.formatPlate(plate, state) + ": " + Object.keys(citations).length;

        Object.keys(categorizedCitations).forEach( function (key) {
          var tweetLine = key + ": " + categorizedCitations[key];

          // Max twitter username is 15 characters, plus the @
          if (tweetText.length + tweetLine.length + 1 + 16 > maxTweetLength) {
            tweets.push(tweetText);
            tweetLine = "";
            tweetText = tweetLine;
          }
          else {
            tweetText += "\n";
            tweetText += tweetLine;
          }
        });

        if (tweetText.length > 0) {
          tweetText += "\n";
          tweets.push(tweetText);
        }
        
        if (verbose) {
          tweetText = "";
          
          var sortedChronoCitationKeys = Object.keys(chronologicalCitations).sort( function(a, b) { return (new Date(a)).getTime() - (new Date(b)).getTime();} );
          
          sortedChronoCitationKeys.forEach( function (key) {
            chronologicalCitations[key].forEach( function ( citation ) {
              var tweetLine = citation.ViolationDate + ", " + citation.Type + ", " + citation.ViolationLocation + ", " + citation.Status;

              // Max twitter username is 15 characters, plus the @
              // HACK!!HACK!!HACK!!HACK!!HACK!! Remove 2nd +16
              if (tweetText.length + tweetLine.length + 1 + 16 + 16 > maxTweetLength) {
                tweets.push(tweetText);
                tweetText = tweetLine;
                tweetLine = "";
              }
              else {
                tweetText += "\n";
                tweetText += tweetLine;
              }
            });
          });

          if (tweetText.length > 0) {
            tweets.push(tweetText);
          }
        }

        tweetText = violationsByYearText + license.formatPlate(plate, state) + ":";
        Object.keys(violationsByYear).forEach( function (key) {
          var tweetLine = key + ": " + violationsByYear[key];

          // Max twitter username is 15 characters, plus the @
          if (tweetText.length + tweetLine.length + 1 + 16 > maxTweetLength) {
            tweets.push(tweetText);
            tweetLine = "";
            tweetText = tweetLine;
          }
          else {
            tweetText += "\n";
            tweetText += tweetLine
          }
        });

        if (tweetText.length > 0) {
          tweets.push(tweetText);
        }

        tweetText = violationsByStatusText + license.formatPlate(plate, state) + ":";
        Object.keys(violationsByStatus).forEach( function (key) {
          var tweetLine = key + ": " + violationsByStatus[key];

          // Max twitter username is 15 characters, plus the @
          if (tweetText.length + tweetLine.length + 1 + 16 > maxTweetLength) {
            tweets.push(tweetText);
            tweetLine = "";
            tweetText = tweetLine;
          }
          else {
            tweetText += "\n";
            tweetText += tweetLine
          }
        });

        if (tweetText.length > 0) {
          tweets.push(tweetText);
        }
      }
      
      resolve(tweets);
    });
  });
}

function SendResponses(origTweet, tweets, verbose) {
  var tweetText = tweets.shift();
  var replyToScreenName = origTweet.user.screen_name;
  var replyToTweetId = origTweet.id_str;
  
  try {
    /* Now we can respond to each tweet. */
    tweetText = "@" + replyToScreenName + " " + tweetText;
    (new Promise(function (resolve, reject) {

      //console.log(`Tweeting ${tweetText}`);
      console.log(`Replying to tweet: ${replyToTweetId}`)
      T.post('statuses/update', {
        status: tweetText,
        in_reply_to_status_id: replyToTweetId,
        auto_populate_reply_metadata: true
      }, function(err, data, response) {
        if (err){
            /* TODO: Proper error handling? */
          handleError(err);
          reject(err);
        }
        else{
          resolve(data);
        }
      });
    })).then ( function ( sentTweet ) {
      console.log(`Replied to tweet:  ${sentTweet.in_reply_to_status_id_str}`);
      console.log(`Created tweet:     ${sentTweet.id_str}`);
      
      // Wait a bit. It seems tweeting a whackload of tweets in quick succession
      // can cause Twitter to think you're a troll bot or something and then some
      // of the tweets will not display for users other than the bot account.
      // See: https://twittercommunity.com/t/inconsistent-display-of-replies/117318/11
      if (tweets.length > 0) {
        //sleep(500).then(() => {
          SendResponses(sentTweet, tweets, verbose);
        //});
      }
    });
  }
  catch ( e ) {
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
    var out = '';
    if (typeof indent === 'undefined') {
        indent = 0;
    }
    for (var p in o) {
        if (o.hasOwnProperty(p)) {
            var val = o[p];
            out += new Array(4 * indent + 1).join(' ') + p + ': ';
            if (typeof val === 'object') {
                if (val instanceof Date) {
                    out += 'Date "' + val.toISOString() + '"';
                } else {
                    out += '{\n' + printObject(val, indent + 1) + new Array(4 * indent + 1).join(' ') + '}';
                }
            } else if (typeof val === 'function') {

            } else {
                out += '"' + val + '"';
            }
            out += ',\n';
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
  const lineByLine = require('n-readlines', function (err) {
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

    while (line = liner.next()) {
      /* strip off the date if present, it's only used for debugging. */
      /* First, let's load the ID of the last tweet we responded to. */
      const matches = lastIdRegExp.exec(line);
      if (matches == null || matches.length < 1) {
        handleError(new Error(`Error: No last mention found: ${line}`));
      }
      else if (lastId == undefined) {
        lastId = matches[1];
        break;
      }
    }
  }
  catch ( err ) {
    handleError(err);
  }
    
  return lastId;
}
  
function setLastDmId(lastDmId) {
  console.log(`Writing last dm id ${lastDmId}.`)
  setLastIdInFile(lastDMFilename, lastDmId, maxIdFileLen)
}

function setLastMentionId(lastMentionId) {
  console.log(`Writing last mention id ${lastMentionId}.`)
  setLastIdInFile(lastMentionFilename, lastMentionId, maxIdFileLen)
}

function setLastIdInFile(filename, lastId, maxLines) {
  var filepath = `${__dirname}/${filename}`;
  var today = new Date();
  var newLine = lastId + ": " + today.toLocaleDateString() + " " + today.toLocaleTimeString();

  prependFile(filepath, newLine, maxLines);
}

function prependFile(filePath, newLines, maxLines) {
  // Read file into memory
  var textBuf = fs.readFileSync(filePath);
  var textByLines = textBuf.toString().split("\n");
  
  textByLines = newLines.split("\n").concat(textByLines);
  
  // truncate to specified number of lines
  textByLines = textByLines.slice(0, maxLines);
  
  fs.writeFileSync(filePath, textByLines.join("\n"), function (err) {
    if (err) {
      handleError(err);
    }
  });
}

// Print out subset of tweet object properties.
function printTweet(tweet) {
  return "Tweet: id: " + tweet.id + 
    ", id_str: " + tweet.id_str + 
    ", user: " + tweet.user.screen_name + 
    ", in_reply_to_screen_name: " + tweet.in_reply_to_screen_name + 
    ", in_reply_to_status_id: " + tweet.in_reply_to_status_id + 
    ", in_reply_to_status_id_str: " + tweet.in_reply_to_status_id_str + 
    ", " + tweet.full_text;
}

function handleError(error) {
  var filepath = `${__dirname}/${errorFilename}`;
  var today = new Date();
  var date = today.toLocaleDateString();
  
  //var time = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();
  var time = today.toLocaleTimeString();
  var dateTime = date + ' ' + time;
  
  console.log(`ERROR: ${error}`);

  // Truncate the callstack because only the first few lines are relevant to this code.
  var stacktrace = error.stack.split("\n").slice(0, 10).join("\n");
  var formattedError = `============================== ${dateTime} =========================================\n${error.message}\n${stacktrace}`;
  
  prependFile(filepath, formattedError, maxErrorFileLen);
}

function logTweetById(id) {
  // Quick check to fetch a specific tweet and dump it fully.
  // TODO: Move to app.all("/dumptweet" ...
  T.get(`statuses/show/${id}`, function(err, tweet, response) {
    if (err){
      handleError(err);
      return false;
    }

    console.log(`Retrieved tweet: ${printObject(tweet)}`);
  });

}

// Fake a sleep function. Call this thusly:
// sleep(500).then(() => {
//   do stuff
// })
// Or similar pattrs that use the Promise:
// async function myFunc() {
//   await sleep(1000);
// }
const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}