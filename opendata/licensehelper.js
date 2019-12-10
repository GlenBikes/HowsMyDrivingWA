/* Exported functions. */
module.exports = {
  formatPlate: formatPlate,
  printObject: printObject
};

module.exports.StatesAndProvinces = [
  "AL",
  "AK",
  "AS",
  "AZ",
  "AR",
  "CA",
  "CO",
  "CT",
  "DE",
  "DC",
  "FM",
  "FL",
  "GA",
  "GU",
  "HI",
  "ID",
  "IL",
  "IN",
  "IA",
  "KS",
  "KY",
  "LA",
  "ME",
  "MH",
  "MD",
  "MA",
  "MI",
  "MN",
  "MS",
  "MO",
  "MT",
  "NE",
  "NV",
  "NH",
  "NJ",
  "NM",
  "NY",
  "NC",
  "ND",
  "MP",
  "OH",
  "OK",
  "OR",
  "PW",
  "PA",
  "PR",
  "RI",
  "SC",
  "SD",
  "TN",
  "TX",
  "UT",
  "VT",
  "VI",
  "VA",
  "WA",
  "WV",
  "WI",
  "WY",
  "AB",
  "BC",
  "MB",
  "NB",
  "NL",
  "NT",
  "NS",
  "NU",
  "ON",
  "PE",
  "QC",
  "SK",
  "YT"
];

/**
 * When displaying a license we should not use XX:nnnnn especially since when
 * putting a # in front, it means all our tweets will show up in "yay state XX" hashtag queries.
 * Also it protects from someone copying/pasting or retweeting or something and having the bot
 * detect it as another request.
**/
  function formatPlate(license) {
    return license.toUpperCase().replace(':', '_');
  }

var _verbose = process.env.VERBOSE;

/**
 * When investigating a selenium test failure on a remote headless browser that couldn't be reproduced
 * locally, I wanted to add some javascript to the site under test that would dump some state to the
 * page (so it could be captured by Selenium as a screenshot when the test failed). JSON.stringify()
 * didn't work because the object declared a toJSON() method, and JSON.stringify() just calls that
 * method if it's present. This was a Moment object, so toJSON() returned a string but I wanted to see
 * the internal state of the object instead.
 *
 * So, this is a rough and ready function that recursively dumps any old javascript object.
**/
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
