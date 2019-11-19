


var assert = require('assert'),
    log4js = require('log4js'),
    sinon = require('sinon'),
    strUtils = require('../util/stringutils.js');

// Log files
log4js.configure('./config/log4js.json');
var log = log4js.getLogger("test");

describe('Numeric string compare', function() {
  describe('Test strings with a < b', function() {
    it('return < 0', () => {
      assert(strUtils._compare_numeric_strings('1196720038423617536', '1196727525382049792') < 0);
    });
  });
  
  describe('Test strings with a < b, a shorter', function() {
    it('return < 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('99', '1196727525382049792') < 0);
    });
  });
  
  describe('Test strings with a > b, a shorter', function() {
    it('return > 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('1196727525382049792', '99') > 0);
    });
  });
  
  describe('Test strings with a > b, a shorter', function() {
    it('return > 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('1196727525382049792', '1196720038423617536') > 0);
    });
  });
  
  describe('Test strings with a == b', function() {
    it('return == 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('1196727525382049792', '1196727525382049792') == 0);
    });
  });
  
  describe('Test two empty strings', function() {
    it('return == 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('', '') == 0);
    });
  });
  
  describe('Test empty string < number', function() {
    it('return < 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('', '1') < 0);
    });
  });
  
  describe('Test number > empty string', function() {
    it('return > 0', () => {
      debugger;
      assert(strUtils._compare_numeric_strings('1', '') > 0);
    });
  });
});

