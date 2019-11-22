const log4js = require('log4js'),
      chokidar = require('chokidar');

// Log files
log4js.configure('config/log4js.json');
var log = log4js.getLogger();

const LogType = {
  app: "app",
  err: "err",
  err: "test",
  last_dm: "last_dm",
  last_mention: "last_mention"
}

const watcher = chokidar.watch('./config/log4js.json', {
  ignored: /(^|[\/\\])\../, // ignore dotfiles
  persistent: true,
  awaitWriteFinish: true
});

function getLog(log_type) {
  switch(log_type) {
    case LogType.app:
      return log4js.getLogger("default");
      break;
      
    case LogType.err:
      return log4js.getLogger("_error");
      break;
      
    case LogType.test:
      return log4js.getLogger("_test");
      break;
      
    case LogType.last_dm:
      return log4js.getLogger("_lastdm");
      break;
      
    case LogType.last_mention:
      return log4js.getLogger("_lastmention");
      break;
      
    default:
      throw new Error(`Unknown log type '${log_type}'.''`);
  }
}

function reloadlog(reason) {
  log.info(`Reloading log config due to config file ${reason}.`);
  log4js.shutdown( () => {
    log4js.configure('config/log4js.json');
    log = log4js.getLogger();
  });
}

watcher
  .on('add', path => reloadlog('add'))
  .on('change', path => reloadlog('change'));

module.exports = {
  getLog: getLog,
  LogType: LogType
};