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
