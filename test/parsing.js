var assert = require('assert');
var server = require('../server');

const user = {
  userName: 'echo',
  avatar: 'echo.png'
};

const createTweet = ({
  id = -1,
  id_str = '',
  full_text = 'Default tweet.',
  display_text_range = []
}) => ({
  id,
  id_str,
  full_text,
  display_text_range
});

describe('Tweet chomping', function() {
  describe('chomp reply tweet', function() {
    it('should remove users at start of full_text if display_text_range specifies a display range', function() {
      assert.equal(server._chompTweet(createTweet({full_text: '@TestUser1 @TestUser2 This is the tweet', display_text_range: [22,17]})), 'This is the tweet');
    });
  });
  
  describe('chomp empty tweet', function() {
    it('should return empty string', function() {
      assert.equal(server._chompTweet(createTweet({full_text: ''})), '');
    })
  });
  
  describe('chomp non-reply tweet', function() {
    it('should return exactly full_text', function() {
      assert.equal(server._chompTweet(createTweet({full_text: 'This is just a tweet.'})), 'This is just a tweet.');
    })
  });
});
