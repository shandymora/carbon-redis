// Include modules

var config		= require('./config');
var memwatch 	= require('memwatch');
var app			= require('./app');
require('longjohn');

// Logging parameters
var logLevel = config.logLevel;
var log = config.log;
var currentDir = config.currentDir;


// Define command line options to script
var opt = require('node-getopt').create([
	['l', 'loglevel=ARG'	, 'set logging level'],
	['c', 'config=ARG'		, 'config file location'],
	['f', 'logfile=ARG'		, 'log file location'],
	['h', 'help'			, 'Display help.']
])
.bindHelp()
.parseSystem();



if ( opt.options.loglevel ) { 
	console.log(JSON.stringify(opt.options, undefined, 2));
	
	if ( opt.options.loglevel == "warn" ) {
		config.logLevel.info = false;
		log.warn('Log level warn'); 
	} else if ( opt.options.loglevel == "error" ) {
		config.logLevel.info = false;
		config.logLevel.warn = false;
		log.error('Log level error'); 
	} else if ( opt.options.loglevel == "debug" ) {
		config.logLevel.debug = true;
		log.log('info', 'Log level debug'); 
	}
	 
}

if ( opt.options.config ) {
	var configFile = opt.options.config;
} else {
	var configFile = currentDir+'/../conf/config.json';
}

// Read in config file
config.readConfig(configFile, function (settings) {

	// memory monitoring
	
	// Leak detection
	memwatch.on('leak', function(info) {  
		config.health.memory.leak.push(info);
	});
	// Heap Usage
	memwatch.on('stats', function(stats) { 
		config.health.memory.heap = stats;
	});
	
	// Force a GC on start up to get initial heap stats
	memwatch.gc();

	// Start app
	app.start(settings);
	
	// Refresh config
	setInterval( function() {
		if (logLevel.info == true) { log.info('Refreshing config'); }
		// Read in config file
		config.readConfig(configFile);
	}, 60000);
	
});