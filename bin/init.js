/*
 * project:	red-timestore
 * author: 	amora
 * 
 */
require('look').start(5960);
require('longjohn');

// Setup Logging
var config		= require('./config');
var utility		= require('./utility');
config.app_data = {
	logger:					{},
	tcp_servers:			{},
	http_servers:			{},
	redis_clients:			{},
	ircBots:				{},
	lineReceiverPattern:	{},
	whitelist:				[],
	blacklist:				[],
	lineRemainder:			'',
	slot_lookup:			{},
	schemas:				[]
};

var currentDir = config.currentDir;
config.app_data.logger = new utility.Logger({
	file_path	: currentDir+'/../log/'+config.my_hostname,
	log_for 	: 'app'
});
var logger = config.app_data.logger;

// Include modules
var memwatch 	= require('memwatch');
var app			= require('./app');

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
		logger.logLevel.info = false;
		logger.log.info('Log level warn'); 
	} else if ( opt.options.loglevel == "error" ) {
		logger.logLevel.info = false;
		logger.logLevel.warn = false;
		logger.log.info('Log level error'); 
	} else if ( opt.options.loglevel == "debug" ) {
		logger.logLevel.debug = true;
		logger.log.log('info', 'Log level debug'); 
	}
	 
}

if ( opt.options.config ) {
	var configFile = opt.options.config;
} else {
	var configFile = currentDir+'/../conf/config.json';
}

// Read in config file
utility.readConfig(configFile, function (settings) {

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
	
	if ( settings.app.auto_reload_config == true ) {
		// Refresh config
		setInterval( function() {
			if (logger.logLevel.info == true) { logger.log.info('Refreshing config'); }
			// Read in config file
			utility.readConfig(configFile);
			client.start_redis_clients(config.settings);
		}, 60000);
	}
		
});