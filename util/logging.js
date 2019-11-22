const log4js = require('log4js'),
      chokidar = require('chokidar');

// Log files
log4js.configure('config/log4js.json');
var log = log4js.getLogger(),
    lastdmLog = log4js.getLogger("_lastdm"),
    lastmentionLog = log4js.getLogger("_lastmention");

module.exports._log = log;
module.exports._lastdmLog = lastdmLog;
module.exports._lastmentionLog = lastmentionLog;

const watcher = chokidar.watch('./config/log4js.json', {
  ignored: /(^|[\/\\])\../, // ignore dotfiles
  persistent: true,
  awaitWriteFinish: true
});

function reloadlog(reason) {
  log.info(`Reloading log config due to config file ${reason}.`);
  log4js.shutdown( () => {
    log4js.configure('config/log4js.json');
    log = log4js.getLogger(),
    lastdmLog = log4js.getLogger("_lastdm"),
    lastmentionLog = log4js.getLogger("_lastmention");
  });
}

watcher
  .on('add', path => reloadlog('add'))
  .on('change', path => reloadlog('change'));

module.exports._log = log;