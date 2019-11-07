// Exported functions that unit tests need.
module.exports = {
  _splitLines: SplitLongLines
};

// modules

/*
 * Split array of strings to ensure each string is <= maxLen
 *
 * Params:
 *   source_lines: array of strings (each one may be multi-line)
 *   maxLen:       maximum length for each element in source_lines
 * Returns:
 *   array of strings matching source_lines but with any elements longer
 *   than maxLen, broken up into multiple entries, breaking on in order:
 *   - newlines (trailing newlines on broken elements are removed)
 *   - word breaks
 *   - if neither above exist, then just split at maxLen characters
 *
 * Note: elements in source_lines are not joined if < maxLen, only broken
 *       up if > maxLen
**/
function SplitLongLines(source_lines, maxLen) {
  var truncated_lines = [];
  
  var index = 0;
  source_lines.forEach(source_line => {
    if (source_line.length > maxLen) {
      // break it up into lines to start with
      var chopped_lines = source_line.split("\n");
      var current_line = "";
      var first_line = true;

      chopped_lines.forEach(line => {
        if (line.length > maxLen) {
          // OK we have a single line that is too long for a tweet
          if (current_line.length > 0) {
            truncated_lines.push(current_line);
            current_line = '';
            first_line = true;
          }
          
          // word break it into multiple items
          var truncate_index = maxLen - 1;

          // Go back until we hit a whitespace characater
          while (truncate_index > 0 && !/\s/.test(line[truncate_index])) {
            truncate_index--;
          }

          if (truncate_index == 0) {
            // The line has no whitespace in it, just chop it in two
            truncate_index = maxLen - 1;
          }

          truncated_lines.push(line.substring(0, truncate_index + 1));

          // The rest of the string may still be too long.
          // Call ourselves recursively to split it up.
          var rest_truncated_lines = SplitLongLines(
            [line.substring(truncate_index + 1)],
            maxLen
          );
          truncated_lines = truncated_lines.concat(rest_truncated_lines);
        } else {
          if (current_line.length + line.length + 1 <= maxLen) {
            if (!first_line) {
              current_line += "\n";
            }
            current_line += line;
            first_line = false;
          } else {
            truncated_lines.push(current_line);
            
            // Start again
            current_line = line;
            first_line = true;
          }
        }
      });

      if (current_line.length > 0) {
        truncated_lines.push(current_line);
      }
    } else {
      truncated_lines.push(source_line);
    }
  });

  return truncated_lines;
}
