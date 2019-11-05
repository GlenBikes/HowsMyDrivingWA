/* Setting things up. */
var license = require("./licensehelper");
const MINIMUM_CITATION_ID = 0,
  noPlateFoundCitationNumber = -1,
  noCitationsFoundCitationNumber = -2;


module.exports = {
  GetCitationsByPlate: GetCitationsByPlate,
  //ProcessCitations: ProcessCitations,
  ProcessCitationsForRequest: ProcessCitationsForRequest,
  CitationIDNoPlateFound: noPlateFoundCitationNumber,
  CitationIDNoCitationsFound: noCitationsFoundCitationNumber,
  GetVehicleIDs: GetVehicleIDs // TODO: Remove this from exports
};

/* Setting things up. */
var fs = require("fs"),
  path = require("path"),
  express = require("express"),
  app = express(),
  convert = require("xml-js"),
  soap = require("soap");

// Create a global client.
// TODO: Is this a good idea? Or should I create a client for each call?

const noCitationsFoundMessage = "No citations found for plate #",
  noValidPlate = "No valid license found. Please use XX:YYYYY where XX is two character state/province abbreviation and YYYYY is plate #",
  parkingAndCameraViolationsText = "Total parking and camera violations for #",
  violationsByYearText = "Violations by year for #",
  violationsByStatusText = "Violations by status for #";

var url =
  "https://web6.seattle.gov/Courts/ECFPortal/JSONServices/ECFControlsService.asmx?wsdl";

function GetCitationsByPlate(plate, state) {
  var vehicleNumPromises = [];

  return new Promise( (resolve, reject) => {
    var citations = [];
    console.log(`Getting vehicle IDs for ${license.formatPlate(plate, state)}.`);
    GetVehicleIDs(plate, state).then(async function(vehicles) {
      // Make the calls to GetCitationsByVehicleNum soap method synchronously
      // Or we could get throttled by the server.
      for (var i = 0; i < vehicles.length; i++) {
        var vehicle = vehicles[i];
        console.log(`About to call GetCitationsByVehicleNum soap method.`);
        
        citations.push( await GetCitationsByVehicleNum(vehicle.VehicleNumber) );
      }
      
      //Now wait for each of the vehicle num promises to resolve
      //Promise.all(vehicleNumPromises).then( function (citations) {
        console.log(`vehicleNumPromises resolved. Returned ${citations.length} citations.`);
        
        // citations is an array of an array of citations, one for each vehicle id
        // collapse them into a hash based on 
        var citationsByCitationID = {};
        citations.forEach( (innerArray) => {
          innerArray.forEach( (citation) => {
            console.log(`Citation: ${printCitation(citation)}.`);
            
            citationsByCitationID[citation.Citation] = citation;
          });
        });
        
        // Now put the unique citations back to an array
        var allCitations = Object.keys(citationsByCitationID).map(function(v) { return citationsByCitationID[v]; });

        resolve(allCitations);
    });
    
    console.log(`Returning promise from GetCitationsByPlate.`);
  });
}

function GetVehicleIDs(plate, state) {
  var args = {
    Plate: plate,
    State: state
  };
  
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      if (err) {
        console.log(`Error creating soap client: ${err}.`)
        throw err;
      }
      
      // GetVehicleByPlate returns all vehicles with plates that
      // start with the specified plate. So we have to filter the
      // results.
      client.GetVehicleByPlate(args, function(err, result) {
        var vehicle_records = [];
        var jsonObj = JSON.parse(result.GetVehicleByPlateResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        for (var i = 0; i < jsonResultSet.length; i++) {
          var vehicle = jsonResultSet[i];
          
          if (vehicle.Plate == plate) {
            vehicle_records.push(vehicle);
          }
          else {
            console.log(`Filtering out vehicle with plate ${state}:${vehicle.Plate}.`);
          }
        }
        resolve(vehicle_records);
      });
    });
  });
}

function GetCitationsByVehicleNum(vehicleID) {
  var args = {
    VehicleNumber: vehicleID
  };
  
  console.log(`Getting citations for vehicle ID: ${vehicleID}.`);
  
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      console.log(`Soap client created for vehicle ID: ${vehicleID}`);
      if (err) {
        console.log(`ERROR!!!!: ${err}`);
        throw err;
      }
      client.GetCitationsByVehicleNumber(args, function(err, citations) {
        console.log(`Promise for vehicleID ${vehicleID} completed.`);
        if (err) {
          console.log(`ERROR!!!!: ${err}`);
          throw err;
        }
        var jsonObj = JSON.parse(citations.GetCitationsByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        console.log(`Resolving promise in GetCitationsByVehicleNumber for vehicle ID: ${vehicleID}`);
        resolve(jsonResultSet);
      });
    });
    
    console.log(`Returning promise from GetCitationsByVehicleNumber for vehicle ID: ${vehicleID}.`);
  });
}

function GetCasesByVehicleNum(vehicleID) {
  var args = {
    VehicleNumber: vehicleID
  };
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      client.GetCasesByVehicleNumber(args, function(err, cases) {
        var jsonObj = JSON.parse(cases.GetCasesByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        resolve(jsonResultSet);
      });
    });
  });
}

// Process citations for one request
function ProcessCitationsForRequest( citations ) {
  var general_summary, detailed_list, temporal_summary;
  var categorizedCitations = {};
  var chronologicalCitations = {};
  var violationsByYear = {};
  var violationsByStatus = {};
  
  if (!citations || Object.keys(citations).length == 0) {
    // TODO: Write a dummy citation with the license?
    //general_summary = noCitations + license.formatPlate(plate, state);
  } else if (citations.length == 1 && citations[0].Citation < MINIMUM_CITATION_ID) {
    switch ( citations[0].Citation ) {
      case noPlateFoundCitationNumber:
        return [
          `${noCitationsFoundMessage}${citations[0].license}.`
        ];
        break;
        
      case noCitationsFoundCitationNumber:
        return [
          `${noCitationsFoundMessage}${citations[0].license}`
        ];
        break
        
      default:
        throw new Error(`ERROR: Unexpected citation ID: ${citations[0].Citation}.`);
        break;
    }
  } else {
    var license;

    for (var i = 0; i < citations.length; i++) {
      var citation = citations[i];
      var year = "Unknown";
      var violationDate = new Date(Date.now());

      // All citations are from the same license
      if (license == null) {
        license = citation.license;
      }

      try {
        violationDate = new Date(Date.parse(citation.ViolationDate));
      } catch (e) {
        // TODO: refactor error handling to a separate file
        console.log(`ERROR!!!!!!! ${e}.`);
      }

      if (!(violationDate in chronologicalCitations)) {
        chronologicalCitations[violationDate] = new Array();
      }

      chronologicalCitations[violationDate].push(citation);

      if (!(citation.Type in categorizedCitations)) {
        categorizedCitations[citation.Type] = 0;
      }
      categorizedCitations[citation.Type]++;

      if (!(citation.Status in violationsByStatus)) {
        violationsByStatus[citation.Status] = 0;
      }
      violationsByStatus[citation.Status]++;

      year = violationDate.getFullYear();

      if (!(year in violationsByYear)) {
        violationsByYear[year] = 0;
      }

      violationsByYear[year]++;
    }
    
    var general_summary =
      parkingAndCameraViolationsText +
      license +
      ": " +
      Object.keys(citations).length;

    Object.keys(categorizedCitations).forEach( key => {
      var line = key + ": " + categorizedCitations[key];

      // Max twitter username is 15 characters, plus the @
      general_summary += "\n";
      general_summary += line;
    });
    
    var detailed_list = "";

    var sortedChronoCitationKeys = Object.keys(chronologicalCitations).sort(
      function(a, b) {
        return new Date(a).getTime() - new Date(b).getTime();
      }
    );
    
    var first = true;

    for (var i = 0; i < sortedChronoCitationKeys.length; i++) {
      var key = sortedChronoCitationKeys[i];

      chronologicalCitations[key].forEach( citation => {
        if (first != true) {
          detailed_list += "\n";
        }
        first = false;
        detailed_list += `${citation.ViolationDate}, ${citation.Type}, ${citation.ViolationLocation}, ${citation.Status}`;
      });
    }

    var temporal_summary = violationsByYearText + license + ":";
    Object.keys(violationsByYear).forEach( key => {
      temporal_summary += "\n";
      temporal_summary += `${key}: ${violationsByYear[key]}`;
    });
    
    var type_summary = violationsByStatusText + license + ":";
    Object.keys(violationsByStatus).forEach( key => {
      type_summary += "\n";
      type_summary += `${key}: ${violationsByStatus[key]}`;
    });
  }

  // Return them in the order they should be rendered.
  var result = [
    general_summary,
    detailed_list,
    type_summary,
    temporal_summary
  ];
  
  console.log(`Returning:\n${result}.`);
  
  return result;
}

// Print out subset of citation object properties.
function printCitation( citation ) {
  return (
    `Citation: ${citation.Citation}, Type: ${citation.Type}, Status: ${citation.Status}, Date: ${citation.ViolationDate}, Location: ${citation.ViolationLocation}.`
  )
}

