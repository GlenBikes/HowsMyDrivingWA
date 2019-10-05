/* Setting things up. */
var fs = require('fs'),
    path = require('path'),
    express = require('express'),
    app = express(),   
    Twit = require('twit'),
    convert = require('xml-js'),
    config = {
    /* Be sure to update the .env file with your API keys. See how to get them: https://botwiki.org/tutorials/make-an-image-posting-twitter-bot/#creating-a-twitter-app*/      
      twitter: {
        consumer_key: process.env.CONSUMER_KEY,
        consumer_secret: process.env.CONSUMER_SECRET,
        access_token: process.env.ACCESS_TOKEN,
        access_token_secret: process.env.ACCESS_TOKEN_SECRET,
        tweet_mode: 'extended'
      }
    },
    T = new Twit(config.twitter),
    stream = T.stream('statuses/sample');

console.log(`${process.env.TWITTER_HANDLE}: start`);

var soap = require('soap');
var url = 'https://web6.seattle.gov/Courts/ECFPortal/JSONServices/ECFControlsService.asmx?wsdl';

/*
    T.get('statuses/show/1179863388060618758', function(err, tweet, response) {
      if (err){
        console.log(`Error!: ${err}`);
        return false;
      }
      
      console.log(`Retrieved tweets: ${printObject(tweet)}`);
    });
*/
//var plate = 'ATT2936';
//var plate = 'BDS9037';
//var plate = 'R204WSU';
//var plate = 'ANB4866';
//var plate = '3FEET';
//var state = 'WA';
var maxTweetLength = 280;
var tweets = [];
var noCitations = "No citations found for plate # ";
var noValidPlate = "No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #";
var parkingAndCameraViolationsText = "Total parking and camera violations for #";
var violationsByYearText = "Violations by year for #";
var statesAndProvinces = [ 'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA', 'WA', 'WV', 'WI', 'WY', 'AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT' ];
var licenseRegExp = /\b([a-zA-Z]{2}):([a-zA-Z0-9]+)\b/;

app.use(express.static('public'));

/* uptimerobot.com is hitting this URL every 20 minutes. */

app.all("/tweet", function (request, response) {
  /* Respond to @ mentions */
  fs.readFile(__dirname + '/last_mention_id.txt', 'utf8', function (err, last_mention_id) {
    /* First, let's load the ID of the last tweet we responded to. */
    console.log(`last_mention_id: ${last_mention_id}`);

    /* Next, let's search for Tweets that mention our bot, starting after the last mention we responded to. */
    T.get('search/tweets', { q: '%40' + process.env.TWITTER_HANDLE, since_id: last_mention_id, tweet_mode: 'extended' }, function(err, data, response) {
      if (err){
        console.log(`Error!: ${err}`);
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

          /*
          Make sure this isn't a reply to one of the bot's tweets.
          */
          if (status.in_reply_to_screen_name === process.env.TWITTER_HANDLE) {
            // Do nothing
            console.log("Ignoring response to my tweet by (" + status.user.screen_name + "): " + status.full_text);
          }
          else {
            const {state, plate, verbose} = parseTweet(status.full_text);

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
        });
        
        if (maxTweetIdRead > last_mention_id) {
          console.log(`Writing last_mention_id: ${maxTweetIdRead}`);
          fs.writeFile(__dirname + '/last_mention_id.txt', maxTweetIdRead, function (err) {
            /* TODO: Error handling? */
            if(err){
              console.log(`Error!: ${err}`);
            }
          });
        }
      } else {
        /* No new mentions since the last time we checked. */
        console.log('No new mentions...');      
      }
    });    
  });

  /* Respond to DMs */

  /* Load the ID of the last DM we responded to. */
  fs.readFile(__dirname + '/last_dm_id.txt', 'utf8', function (err, last_dm_id) {
    if (err){
      console.log(`Error!: ${err}`);
      return false;
    }
    console.log(`last_dm_id: ${last_dm_id}`);

    T.get('direct_messages', { since_id: last_dm_id, count: 200 }, function(err, dms, response) {
      /* Next, let's DM's to our bot, starting after the last DM we responded to. */
      if (dms.length){
        dms.forEach(function(dm) {
          console.log(`Direct message: sender (${dm.sender_id}) ie_str (${dm.id_str}) ${dm.text}`);

          /* Now we can respond to each tweet. */
          T.post('direct_messages/new', {
            user_id: dm.sender_id,
            text: "This is a test response."
          }, function(err, data, response) {
            if (err){
              /* TODO: Proper error handling? */
              console.log(`Error!: ${err}`);
            }
            else{
              fs.writeFile(__dirname + '/last_dm_id.txt', dm.id_str, function (err) {
                /* TODO: Error handling? */
              });
            }
          });
        });
      } else {
        /* No new DMs since the last time we checked. */
        console.log('No new DMs...');      
      }
    });    
  });  
  
  /* TODO: Handle proper responses based on whether the tweets succeed, using Promises. For now, let's just return a success message no matter what. */
  response.sendStatus(200);
});

var listener = app.listen(process.env.PORT, function () {
  console.log(`Your bot is running on port ${listener.address().port}`);
});

function parseTweet(text) {
  var state;
  var plate;
  var verbose = false;
  
  if (/verbose/i.test(text)) {
    //console.log("Found verbose!");
    verbose = true;
  }
  
  const matches = licenseRegExp.exec(text);
  
  if (matches == null || matches.length < 2 || matches[1] == "XX") {
    console.log(`Error: No license found in tweet: ${text}`);
  }
  else {
    state = matches[1];
    plate = matches[2];

    //console.log("state: " + state + " plate: " + plate);
    if (statesAndProvinces.indexOf(state.toUpperCase()) < 0) {
      throw "Invalid state: " + state;
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
  verbose = true;

  return new Promise((resolve, reject) => {
    GetCitationsByPlate(plate, state).then(function(citations) {
      var tweets = [];

      if (!citations || Object.keys(citations).length == 0) {
        tweets.push(noCitations + formatPlate(plate, state));
      } else {
        Object.keys(citations).forEach(function (key) {
          var year = "Unknown";
          var violationDate = new Date(Date.now());
          try {
            violationDate = new Date(Date.parse(citations[key].ViolationDate));
          }
          catch ( e ) {
            console.log(`Error parsing date ${citations[key].ViolationDate} : ${e}`);
          }
          
          if (!(violationDate in chronologicalCitations)) {
            chronologicalCitations[violationDate] = new Array();
          }
          
          chronologicalCitations[violationDate].push(citations[key]);
          
          if (!(citations[key].Type in categorizedCitations)) {
            categorizedCitations[citations[key].Type] = 0;
          }
          categorizedCitations[citations[key].Type]++;

          year = violationDate.getFullYear();

          if (!(year in violationsByYear)) {
            violationsByYear[year] = 0;
          }

          violationsByYear[year]++;
        });

        var tweetText = parkingAndCameraViolationsText + formatPlate(plate, state) + ": " + Object.keys(citations).length;

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

        tweetText = violationsByYearText + formatPlate(plate, state) + ":";
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

      console.log(`Tweeting ${tweetText}`);
      T.post('statuses/update', {
        status: tweetText,
        in_reply_to_status_id: replyToTweetId,
        auto_populate_reply_metadata: true
      }, function(err, data, response) {
        if (err){
            /* TODO: Proper error handling? */
          console.log('Error!', err);
          reject(err);
        }
        else{
          fs.writeFile(__dirname + '/last_mention_id.txt', data.id_str, function (err) {
            /* TODO: Error handling? */
            if(err){
              console.log(`Error!: ${err}`);
            }
          });
          resolve(data);
        }
      });
    })).then ( function ( sentTweet ) {
      if (tweets.length > 0) {
        SendResponses(sentTweet, tweets, verbose);
      }
    });
  }
  catch ( e ) {
    console.log(`EXCEPTION!!!: ${e}`);
  }
}

async function GetCitationsByPlate(plate, state) {
  return new Promise((resolve, reject) => {
    var allCitations = {};

    GetVehicleIDs(plate, state).then(async function(vehicles) {

      for ( var i = 0; i < vehicles.length; i++)
      {
        var vehicle = vehicles[i];
        await new Promise(function(resolve, reject) {
          GetCitationsByVehicleNum(vehicle.VehicleNumber).then(function(citations) {
          
          var pre = Object.keys(allCitations).length;
          citations.forEach(function(item) {
            allCitations[item.Citation] = item;
          })
          var post = Object.keys(allCitations).length;
            if (pre != 0 && pre != post) {
              console.log(`Found vehicle for which violations for IDs different: ${formatPlate(plate, state)} Pre: ${pre} Post: ${post}`);
            }
          resolve();
          });
        });
      }
      
      resolve(allCitations);
      
    });
  });
}

async function GetVehicleIDs(plate, state) {
  var args = { 
    Plate: plate,
    State: state
  };

  return new Promise((resolve, reject) => {
                     
    soap.createClient(url, async function(err, client) {
          client.GetVehicleByPlate(args, function(err, result) {
            var vehicles = [];
            var jsonObj = JSON.parse(result.GetVehicleByPlateResult);
            var jsonResultSet = JSON.parse(jsonObj.Data);
            for ( var i = 0; i < jsonResultSet.length; i++)
            {
              var vehicle = jsonResultSet[i];
              vehicles.push(vehicle);
            }

            resolve(vehicles);
          });
      })
  });
}

async function GetCitationsByVehicleNum(vehicleID) {
  var args = { 
        VehicleNumber: vehicleID,
   };
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      client.GetCitationsByVehicleNumber(args, function (err, citations) {

        var jsonObj = JSON.parse(citations.GetCitationsByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        resolve(jsonResultSet);
      });
    });
  });
}

function getSortedHash(inputHash){
  var resultHash = {};

  var keys = Object.keys(inputHash);
  keys.sort(function(a, b) {
    return inputHash[a] - inputHash[b]
  }).reverse().forEach(function(k) {
    resultHash[k] = inputHash[k];
  });
  return resultHash;
}

function formatPlate(plate, state)
{
  return state.toUpperCase() + "-" + plate.toUpperCase();
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

function printTweet(tweet) {
  return "Tweet: id: " + tweet.id + 
    ", id_str: " + tweet.id_str + 
    ", user: " + tweet.user.screen_name + 
    ", in_reply_to_screen_name: " + tweet.in_reply_to_screen_name + 
    ", " + tweet.full_text;
}
