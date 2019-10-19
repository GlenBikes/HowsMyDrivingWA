
/* Setting things up. */
var license = require('./licensehelper');

module.exports = {
  GetCitationsByPlate: GetCitationsByPlate
};

/* Setting things up. */
var fs = require("fs"),
  path = require("path"),
  express = require("express"),
  app = express(),
  convert = require("xml-js"),
  soap = require("soap");

var url =
  "https://web6.seattle.gov/Courts/ECFPortal/JSONServices/ECFControlsService.asmx?wsdl";

async function GetCitationsByPlate(plate, state) {
  return new Promise((resolve, reject) => {
    var allCitations = {};

    GetVehicleIDs(plate, state).then(async function(vehicles) {
      for (var i = 0; i < vehicles.length; i++) {
        var vehicle = vehicles[i];
        
        await new Promise(function(resolve, reject) {
          console.log(`Getting citations for vehicle #${vehicle.VehicleNumber}.`);
          GetCitationsByVehicleNum(vehicle.VehicleNumber).then(function(
            citations
          ) {
            citations.forEach(function(item) {
              allCitations[item.Citation] = item;
            });
            resolve();
          });
        });
        /*
        await new Promise(function(resolve, reject) {
          console.log(`Getting cases for vehicle #${vehicle.VehicleNumber}.`);
          GetCasesByVehicleNum(vehicle.VehicleNumber).then(function( cases ) {
            cases.forEach(function(item) {
              console.log(license.printObject(item));
              //allCases[item.Citation] = item;
            });
            resolve();
          });
        });
        */
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
        for (var i = 0; i < jsonResultSet.length; i++) {
          var vehicle = jsonResultSet[i];
          vehicles.push(vehicle);
        }

        resolve(vehicles);
      });
    });
  });
}

async function GetCitationsByVehicleNum(vehicleID) {
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

async function GetCasesByVehicleNum(vehicleID) {
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
