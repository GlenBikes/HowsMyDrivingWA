import * as findFreePort from 'find-port-free-sync';
import * as fs from 'fs';
import * as path from 'path';

export function getUnusedPort(): number {
  let port = findFreePort({
    start: 1025,
    end: 49151,
    ip: '127.0.0.1'
  });

  return port;
}

export function getAppRootPath(): string {
  let root_path = path.resolve(__dirname + '/../../');

  if (!fs.existsSync(path.resolve(root_path, 'package.json'))) {
    root_path = path.resolve(__dirname + '/../../../');

    if (!fs.existsSync(path.resolve(root_path, 'package.json'))) {
      throw new Error(`Cannot find app root path containing package.json: ${__dirname}.`);
    }
  }

  return root_path;
}
