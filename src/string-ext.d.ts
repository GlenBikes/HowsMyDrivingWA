/**
 * Add lpad(string, number) to String prototype
 *
 * Params:
 *   pad:    character to prepend to string to make it length.
 *   l: length of string returned
 * Returns:
 *   string of length >= l where pad character is prepended to string
 *   enough times to satisfy length requirement. If string.length >= l,
 *   then the string is returned.
 *
 * Exceptions:
 *   If pad is not a single character, an Error is thrown.
 *
 * See: string-ext.ts
**/
declare global {
    interface String {
        lpad(pad: string, l : number) : string;
        trunc(n: number): string;
      };
    }
}