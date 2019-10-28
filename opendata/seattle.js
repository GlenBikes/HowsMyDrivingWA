/* Setting things up. */
var license = require("./licensehelper");

module.exports = {
  GetCitationsByPlate: GetCitationsByPlate,
  ProcessCitations: ProcessCitations,
  GetVehicleIDs: GetVehicleIDs // TODO: Remove this from exports
};

/* Setting things up. */
var fs = require("fs"),
  path = require("path"),
  express = require("express"),
  app = express(),
  convert = require("xml-js"),
  soap = require("soap");

var noCitations = "No citations found for plate # ";
var parkingAndCameraViolationsText =
  "Total parking and camera violations for #";
var violationsByYearText = "Violations by year for #";
var violationsByStatusText = "Violations by status for #";

var url =
  "https://web6.seattle.gov/Courts/ECFPortal/JSONServices/ECFControlsService.asmx?wsdl";

function GetCitationsByPlate(plate, state) {
  var vehicleNumPromises = [];

  return new Promise( (resolve, reject) => {
    GetVehicleIDs(plate, state).then(function(vehicles) {

      for (var i = 0; i < vehicles.length; i++) {
        var vehicle = vehicles[i];

        var vehicleNumPromise = GetCitationsByVehicleNum(vehicle.VehicleNumber);

        vehicleNumPromises.push(vehicleNumPromise);
      }

      //Now wait for each of the vehicle num promises to resolve
      Promise.all(vehicleNumPromises).then( function (citations) {
        // citations is an array of an array of citations, one for each vehicle id
        // collapse them into a hash based on 
        var citationsByCitationID = {};
        citations.forEach( (innerArray) => {
          innerArray.forEach( (citation) => {
            citationsByCitationID[citation.Citation] = citation;
          })
        })
        
        // Now put the unique citations back to an array
        var allCitations = Object.keys(citationsByCitationID).map(function(v) { return citationsByCitationID[v]; });

        resolve(allCitations);
      }).catch( function ( err ) {
        // TODO: refactor error hanling out to a separete file.
        console.log(`ERROR!!! ${err}.`);
      });
    });
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
      
      client.GetVehicleByPlate(args, function(err, result) {
        var vehicle_records = [];
        var jsonObj = JSON.parse(result.GetVehicleByPlateResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        for (var i = 0; i < jsonResultSet.length; i++) {
          var vehicle = jsonResultSet[i];
          vehicle_records.push(vehicle);
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
  return new Promise((resolve, reject) => {
    soap.createClient(url, function(err, client) {
      client.GetCitationsByVehicleNumber(args, function(err, citations) {
        var jsonObj = JSON.parse(citations.GetCitationsByVehicleNumberResult);
        var jsonResultSet = JSON.parse(jsonObj.Data);

        resolve(jsonResultSet);
      });
    });
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

function ProcessCitations(citations) {
  var general_summary, detailed_list, temporal_summary;
  var categorizedCitations = {};
  var chronologicalCitations = {};
  var violationsByYear = {};
  var violationsByStatus = {};

  if (!citations || Object.keys(citations).length == 0) {
    // TODO: Write a dummy citation with the license?
    //general_summary = noCitations + license.formatPlate(plate, state);
  } else {
    var license;

    for (var citation in citations) {
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

    for (var key in Object.keys(categorizedCitations)) {
      var line = key + ": " + categorizedCitations[key];

      // Max twitter username is 15 characters, plus the @
      general_summary += "\n";
      general_summary += line;
    }

    var detailed_list = "";

    var sortedChronoCitationKeys = Object.keys(chronologicalCitations).sort(
      function(a, b) {
        return new Date(a).getTime() - new Date(b).getTime();
      }
    );
    var first = true;

    for (var key in sortedChronoCitationKeys) {
      for (var citation in chronologicalCitations[key]) {
        if (first != true) {
          detailed_list += "\n";
        }
        first = false;
        detailed_list += `${citation.ViolationDate}, ${citation.Type}, ${citation.ViolationLocation}, ${citation.Status}`;
      }
    }

    var temporal_summary = violationsByYearText + license + ":";
    for (var key in Object.keys(violationsByYear)) {
      temporal_summary += "\n";
      temporal_summary += `${key}: ${violationsByYear[key]}`;
    }

    var type_summary = violationsByStatusText + license + ":";
    for (var key in Object.keys(violationsByStatus)) {
      type_summary += "\n";
      type_summary += `${key}: ${violationsByStatus[key]}`;
    }
  }

  // return the groups of report items
  var report_items = [
    general_summary,
    detailed_list,
    type_summary,
    temporal_summary
  ];

  return report_items;
}
