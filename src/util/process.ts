import * as findFreePort from 'find-port-free-sync';

export function getUnusedPort(): number {
  let port = findFreePort({
    start: 1025,
    end: 49151,
    ip: '127.0.0.1'
  });

  return port;
}
