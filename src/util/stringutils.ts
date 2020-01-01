// modules
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

export function FormatMilliseconds(millisec: number) {
  var seconds = parseInt((millisec / 1000).toFixed(1));
  var minutes = parseInt((millisec / (1000 * 60)).toFixed(1));
  var hours = parseInt((millisec / (1000 * 60 * 60)).toFixed(1));
  var days = parseInt((millisec / (1000 * 60 * 60 * 24)).toFixed(1));

  if (seconds < 60) {
    return seconds + ' seconds';
  } else if (minutes < 60) {
    return minutes + ' minutes';
  } else if (hours < 24) {
    return hours + ' hours';
  } else {
    return days + ' days';
  }
}
