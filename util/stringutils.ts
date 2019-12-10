// modules
import '../src/string-ext';

import * as uuid from 'uuid';
export const uuidv1 = uuid.v1;

/**
 * Wrap this just so we can stub it in tests.
 *
 * Returns:
 *  String GUID of form uuid/v1 (see uuid npm package)
**/
export function GetHowsMyDrivingId(): string {
  return uuidv1();
}

/**
 * Recursively dumps any javascript object.
 *
 * Params:
 *   o:      Object to dump
 *   Indent: # characters to indent. This allows creating an intented 
 *           tree structure of an obect.
 *
 * Returns:
 *   String representing a dump of o and all it's values.
 */
export function DumpObject(o: any, indent: number = 0): string {
  var out: string = "";
  if (typeof indent === "undefined") {
    indent = 0;
  }
  for (var p in o) {
    if (o.hasOwnProperty(p)) {
      var val: any = o[p];
      out += new Array(4 * indent + 1).join(" ") + p + ": ";
      if (typeof val === "object") {
        if (val instanceof Date) {
          out += 'Date "' + val.toISOString() + '"';
        } else {
          out +=
            "{\n" +
            DumpObject(val, indent + 1) +
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


