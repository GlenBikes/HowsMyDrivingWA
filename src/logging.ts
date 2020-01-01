import { sleep } from 'howsmydriving-utils';

import * as packpath from 'packpath';
import * as path from 'path';
import * as fs from 'fs';

let packpath_parent = packpath.parent() ? packpath.parent() : packpath.self();
let packpath_self = packpath.self();

let package_json_path = path.resolve(__dirname + '/../package.json');

if (!fs.existsSync(package_json_path)) {
  package_json_path = path.resolve(__dirname + '/../../package.json');

  if (!fs.existsSync(package_json_path)) {
    throw new Error(`Cannot find package.json: ${__dirname}.`);
  }
}

var pjson = require(package_json_path);

// Put this at very top so other modules can import it without taking
// dependencies on something else in the module being instantiated.
export const __MODULE_NAME__ = pjson.name;
export const __MODULE_VERSION__ = pjson.version;

const temp_log4js_config_path = path.resolve(
  '/app' + '/dist/config/log4js.json'
);

if (!fs.existsSync(temp_log4js_config_path)) {
  throw new Error(`Cannot find log4js.json: ${temp_log4js_config_path}.`);
}

export const log4js_config_path = temp_log4js_config_path;

const chokidar = require('chokidar');

let log4js = require('log4js');

// Load the config.
log4js.configure(log4js_config_path);

let temp_log = log4js.getLogger('result'),
  temp_lastdmLog = log4js.getLogger('_lastdm'),
  temp_lastmentionLog = log4js.getLogger('_lastdm');

temp_log.addContext('module', __MODULE_NAME__);
temp_lastdmLog.addContext('module', __MODULE_NAME__);
temp_lastmentionLog.addContext('module', __MODULE_NAME__);
temp_log.info(
  `howsmydrivingwa: Adding log4js (${log4js_config_path}) context: ${__MODULE_NAME__}.`
);

export const log = temp_log,
  lastdmLog = temp_lastdmLog,
  lastmentionLog = temp_lastmentionLog;

/**
 * Monitor the log4js config file and reloading log instances if the file changes.
 **/
var watcher = chokidar.watch(log4js_config_path, {
  ignored: /(^|[\/\\])\../, // ignore dotfiles
  persistent: true,
  awaitWriteFinish: true
});

/**
 * Reload log4js (when config changes).
 *
 * Params:
 *   reason: Reason why logs are being reloaded. This is logged before
 *           reloading log4js.
 *
 * TODO: Test to make sure this works. Do existing loggers still work? Do
 *       they update to the new log level?
 **/
function reloadlog(reason: string) {
  /*
  log.info(`Reloading log config due to config file ${reason}.`);

  // Leave the current log active until after we replace it.
  // This should allow any code with a stale instance to keep logging
  // until they are done.
  try {
    log4js.shutdown( (err: Error) => {
      if (err) {
        log.error(`Error occurred during log shutdown: ${err}.`);
      }

      sleep(10000).then( () => {
        log4js.configure(config_path);
        log = log4js.getLogger('reason');
        lastdmLog = log4js.getLogger('_lastdm');
        lastmentionLog = log4js.getLogger('_lastdm');
        log.addContext('module', __MODULE_NAME__);
        lastdmLog.addContext('module', __MODULE_NAME__);
        lastmentionLog.addContext('module', __MODULE_NAME__);
      });
    });
  } catch (err) {
      log.error(`Error occurred during log shutdown: ${err}.`);
  }
  */
}

// Handle the change/add events for the log4js config file.
watcher
  .on('add', (path: string) => {
    sleep(2000).then(() => {
      reloadlog(`add of ${path}`);
    });
  })
  .on('change', (path: string) => {
    sleep(2000).then(() => {
      reloadlog(`change of ${path}`);
    });
  });
