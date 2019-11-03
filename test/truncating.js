var assert = require('assert');
var server = require('../server');

describe('Line choppping', function() {
  describe('chop empty line', function() {
    it('should return an empty line', function() {
      var result = server._splitLines([''], 10);
      assert.equal(result.length, 1);
      assert.equal(result[0], '');
    });
  });
  
  describe('chop line into 6 pieces', function() {
    it('should return 6 strings chopped on word breaks', function() {
      var test_string = [
        "Here is a ",
        "line that ",
        "is a lot ",
        "more than ",
        "ten chars ",
        "long."
      ];

      var result = server._splitLines([test_string.join('')], 10);

      assert.equal(result.length, test_string.length);
      for (var i = 0; i < result.length; i++) {
        assert.equal(result[i], test_string[i]);
      }
    })
  });
  
  describe('chop line with word longer than limit', function() {
    it('should return strings chopped on a word break except for long work, chopped at limit', function() {
      var test_string = [
        'Here is a ',
        'line with ',
        'areallylon',
        'gwordthatc',
        'annotbecho',
        'pped to ',
        'something ',
        'smaller.'
      ];

      var result = server._splitLines([test_string.join('')], 10);
      
      assert.equal(result.length, test_string.length);
      for (var i = 0; i < result.length; i++) {
        assert.equal(result[i], test_string[i]);
      }
    })
  });
  
  describe('chop line on carriage returns', function() {
    it('should return a string per line, without the line break', function() {
      var test_string = [
        'Here is',
        'a string',
        'with three lines.' // this line will get split
      ];
      
      var result = server._splitLines([test_string.join('\n')], 10);
      
      assert.equal(result.length, test_string.length + 2);
      assert.equal(result[0], test_string[0]);
      assert.equal(result[1], test_string[1]);
      assert.equal(result[2] + result[3] + result[4], test_string[2]);
    })
  });
});
