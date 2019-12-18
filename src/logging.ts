import { __MODULE_NAME__ } from './server';

const chokidar = require('chokidar'),
  log4js = require('log4js'),
  path = require('path');

const config_path = path.resolve(__dirname + '/../config/log4js.json');

// Load the config.
log4js.configure(config_path);

export let log = log4js.getLogger('result'),
  lastdmLog = log4js.getLogger('_lastdm'),
  lastmentionLog = log4js.getLogger('_lastdm');

log.addContext('module', __MODULE_NAME__);
lastdmLog.addContext('module', __MODULE_NAME__);
lastmentionLog.addContext('module', __MODULE_NAME__);

/**
 * Monitor the log4js config file and reloading log instances if the file changes.
 **/
var watcher = chokidar.watch(config_path, {
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
  log.info(`Reloading log config due to config file ${reason}.`);
  log4js.shutdown(() => {
    log4js.configure(config_path);
    log = log4js.getLogger('reason');
    lastdmLog = log4js.getLogger('_lastdm');
    lastmentionLog = log4js.getLogger('_lastdm');
    
    log.addContext('module', __MODULE_NAME__);
    lastdmLog.addContext('module', __MODULE_NAME__);
    lastmentionLog.addContext('module', __MODULE_NAME__);
  });
}

// Handle the change/add events for the log4js config file.
watcher
  .on('add', (path: string) => {
    reloadlog(`add of ${path}`);
  })
  .on('change', (path: string) => {
    reloadlog(`change of ${path}`);
  });
