import { DumpObject, PrintTweet } from 'howsmydriving-utils';

const provinces = require('provinces');

var states_and_provinces = provinces.reduce((acc: Array<string>, cur: any) => {
  if (!acc) {
    acc = new Array<string>();
  }

  if (!cur || !cur.country) {
    return acc;
  }

  if (cur.country === 'US' || cur.country === 'CA') {
    acc.push(cur.short);
  }

  return acc;
}, undefined);

/**
 * Array of all valid 2 character abbreviations for US states/occupied territories and
 * Canadian provinces/territories
 **/
export let StatesAndProvinces: Array<string> = states_and_provinces;

/**
 * When displaying a license we should not use XX:nnnnn especially since when
 * putting a # in front, it means all our tweets will show up in "yay state XX" hashtag queries.
 * Also it protects from someone copying/pasting or retweeting or something and having the bot
 * detect it as another request.
 **/
export function formatPlate(license: string) {
  return license.toUpperCase().replace(':', '_');
}
