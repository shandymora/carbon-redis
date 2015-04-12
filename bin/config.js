var fs 		= require('fs');
var log 	= require('winston');
var $		= require('jquery');
var os		= require('os');

var currentDir = __dirname;
var settings = {};
var my_hostname = os.hostname();

var health = {
	memory:		{
		leak:	[]
	},
	cluster: {
		is_master:	false,
		master:		'',
		nodes:		{}
	}
};

var app_data = {};
exports.app_data = app_data;

// Define logging settings
var logLevel = {
	debug:	false,
	info:	true,
	warn:	true,
	error:	true
};
exports.logLevel = logLevel;

log.add(log.transports.File, { filename: currentDir+'/../log/'+my_hostname+'-events.log'} ); 
log.remove(log.transports.Console);
log.info('Started Logging');

function readConfig(file, done) {
  fs.readFile(file, function (err, contents) {
  	
    // If error
    if (err) { 
      if (logLevel.error == true) { log.error('Error opening config file', { error: err } ); }
      console.log('error reading config file: '+JSON.stringify(err, undefined, 2));
      process.exit(1);
    }
	
    // Test if well formed JSON
    try {
		var config = $.parseJSON(contents.toString());
	} catch (error) {
		if (logLevel.error == true) { log.error('Invald JSON configuration file'); }
		console.log('error reading config file: '+JSON.stringify(error, undefined, 2));
		process.exit(1);
	}

	exports.settings = config;
	if (done) { done(config); }
	
  });
}

// Export variables/functions
exports.readConfig = readConfig;
exports.log = log;
exports.currentDir = currentDir;
exports.health = health;
exports.my_hostname = my_hostname;