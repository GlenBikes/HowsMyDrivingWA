import { chompTweet } from '../src/server';
// TODO: move this to utils.
import { createTweet } from './mocks/twitter';

var assert = require('assert');

const user = {
  userName: 'echo',
  avatar: 'echo.png'
};

describe('Tweet chomping', function() {
  describe('chomp reply tweet', function() {
    it('should remove users at start of full_text if display_text_range specifies a display range', function() {
      const { chomped, chomped_text } = chompTweet(
        createTweet({
          full_text: '@TestUser1 @TestUser2 This is the tweet',
          display_text_range: [22, 17]
        }) as any
      );
      assert.equal(chomped_text, 'This is the tweet');
    });
  });

  describe('chomp empty tweet', function() {
    it('should return empty string', function() {
      const { chomped, chomped_text } = chompTweet(
        createTweet({ full_text: '' }) as any
      );
      assert.equal(chomped_text, '');
    });
  });

  describe('chomp non-reply tweet', function() {
    it('should return exactly full_text', function() {
      const { chomped, chomped_text } = chompTweet(
        createTweet({ full_text: 'This is just a tweet.' }) as any
      );
      assert.equal(chomped_text, 'This is just a tweet.');
    });
  });
});
