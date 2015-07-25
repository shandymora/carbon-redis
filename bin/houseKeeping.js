/*
 * project:	red-timestore
 * author: 	amora
 * 
 */
require('longjohn');

// Setup Logging
// Include modules
var config		= require('./config');
var utility		= require('./utility');


config.app_data = {
	logger:					{},
	tcp_servers:			{},
	http_servers:			{},
	redis_clients:			{},
	slot_lookup:			{},
	schemas:				[]
};

var currentDir = config.currentDir;
config.app_data.logger = new utility.Logger({
	file_path	: currentDir+'/../log/'+config.my_hostname,
	log_for 	: 'houseKeeping'
});
var logger = config.app_data.logger;

// Include modules
var $			= require('jquery');
var client		= require('./client');
var server		= require('./server');
var memwatch 	= require('memwatch');
var app_utils	= require('./app_utils');

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
	start(settings);
	
		
	if ( settings.app.auto_reload_config == true ) {
		// Refresh config
		setInterval( function() {
			if (logger.logLevel.info == true) { logger.log.info('Refreshing config'); }
			// Read in config file
			utility.readConfig(configFile);
		}, 60000);
	}
});

function start(oSettings) {
	
	// initialize statsd client
	utility.statsd = new client.statsd(oSettings.client.statsd);
	// Setup timer to send health stats every 60s
	utility.send_health_stats('houseKeeping');
	
	// schema retentions
	init_schemas();
	
	// slot lookup table
	init_slot_lookup(oSettings.client.redis);
	
	// Connect to Redis
	client.start_redis_clients(oSettings, function() {
		// server.start_tcp_servers(oSettings);
		// server.start_http_servers(oSettings);
		
		// Aggregate metrics
		aggregate_metrics();
		setInterval( function() {
			console.log('aggregating metric');
			aggregate_metrics();
		}, 60000);
		
	});
}

function init_slot_lookup(redis_config) {
	

	if (config.app_data.logger.logLevel.info == true) { config.app_data.logger.log.info('Initializing Slots'); }
	
	
	for ( var server in redis_config ) {
		redis_config[server].slots.forEach( function(slots) {
			var items = slots.split("-");
			if ( items[1] ) {
				var startSlot = items[0];
				var endSlot = items[1];
			} else {
				var startSlot = items[0];
				var endSlot = items[0];
			}
			
			for ( var index = startSlot; index <= endSlot; index++ ) {
				if ( index.toString() in config.app_data.slot_lookup ) {
					config.app_data.slot_lookup[index.toString()].servers.push(server);
				} else {
					config.app_data.slot_lookup[index.toString()] = {
							servers: [ server ]
					};
				}
			}
			
		});
	}
	
		
	
	// Find all default servers
	var default_servers = [];
	for ( var server in redis_config ) {
		if ( 'slot_default' in redis_config[server] ) {
			if ( redis_config[server].slot_default ) { default_servers.push(server); }
		}
	}
	
	// Find all unset slots and assign default server
	for ( var slot = 0; slot <= parseInt(config.settings.app.slot_count); slot++ ) {
		if ( !(slot.toString() in config.app_data.slot_lookup) ) {
			config.app_data.slot_lookup[slot.toString()] = {
				servers: default_servers
			};
		}
	}

		
}

function init_schemas() {
	// custom schemas
	config.settings.app.schemas.forEach( function(schema) {
		var retentions_s = [];
		schema.retentions.forEach( function(retention){
			
			app_utils.period_string_to_seconds(retention, function(retention_s) {
				retentions_s.push(retention_s);
			});
				
		});
		
		// sort retentions_s ( retentions in seconds )
		retentions_s.sort( function(a, b) {
			return parseInt(a.split(":")[0]) - parseInt(b.split(":")[0]);
		});
		
		config.app_data.schemas.push(
			{
				pattern: 		new RegExp(schema.pattern),
				regex_string:	schema.pattern,
				retentions: 	schema.retentions,
				retentions_s:	retentions_s
			}
		);
	});
	// default schema
	var retentions_s = [];
	config.settings.app.schema_default.retentions.forEach( function(retention){
		app_utils.period_string_to_seconds(retention, function(retention_s) {
			retentions_s.push(retention_s);
		});
	});
	
	// sort retentions_s ( retentions in seconds )
	retentions_s.sort( function(a, b) {
		return parseInt(a.split(":")[0]) - parseInt(b.split(":")[0]);
	});
		
	config.app_data.schemas.push( 
		{
			pattern:		new RegExp(/.*/),
			regex_string:	'.*',
			retentions:		config.settings.app.schema_default.retentions,
			retentions_s:	retentions_s
		}
	);
}

function aggregate_metrics() {
	
	
	// each redis server
	for (var server_name in config.settings.client.redis) {
		
		scan_server(server_name);
	}
	
	function scan_server(server) {	
		var cursor = '0';
		
		send_scan_command(cursor, send_scan_command);
		
		function send_scan_command( cursor, done) {
			config.app_data.redis_clients[server].client.send_command( 'SCAN', [ cursor ], function(err, reply) {
				if (err) {
					if (logger.logLevel.error == true) { logger.log.error('Error reading from Redis server: '+server); }
		      		console.log('Error reading from server: '+server+'\n');
		      		console.log(reply);
				} else {
		      		var items = reply.toString().split(",");
		      		
		      		// process items
		      		items.forEach( function(item, item_index) {
		      			if ( item_index == 0 ) { return; }
		      			if ( item == "" ) {
		      				items.splice(item_index, 1);
		      			} else {
		      				var item_elements = item.split(":");
		      				
		      				/*
		      				 * 	Remove slot prefix and interval postfix from key
		      				 */
		      			//	var key = item_elements[0].split(":")[1];
		      				process_metric(item_elements);
		      			}
		      		});
		      		
		      		if (items[0] != "0") {
		      			done(items[0], done);
		      		} else {
		      //			metrics = metrics.concat(items.slice(1));
		      //			finished_getting_metricsnames();
		      		}
				}
			});
		}
		
		function process_metric(composite_key) {
			
			var slot = composite_key[0];
			var key = composite_key[1];
			var interval = composite_key[2];
			
			// Lookup schema retention for metric key
			lookup_schema( key, function(schema) {
				
			var sum_schema_retentions = 0;
			
				// each retention
				schema.retentions_s.forEach( function(retention, schema_index) {
					var schema_interval = parseInt(retention.split(":")[0]);
					var schema_retention = parseInt(retention.split(":")[1]);
					sum_schema_retentions += schema_retention;
					
					if ( schema_index < schema.retentions_s.length-1 ) {
						var action = 'agg';
						var next_schema_interval = parseInt(schema.retentions_s[schema_index+1].split(":")[0]);
						var next_schema_retention = parseInt(schema.retentions_s[schema_index+1].split(":")[1]);
						var num_agg_datapoints = next_schema_interval / schema_interval;
					} else {
						var action = 'del';
						var next_schema_interval = null;
						var next_schema_retention = null;
						var num_agg_datapoints = 0;
					}
						
					
					
					// match metric interval to schema interval
					if ( interval == schema_interval ) {
						var now = utility.timeInSecs();
						var cutoff_time = now - sum_schema_retentions;
						
						if ( action == 'agg' ) {
							// get, aggregate, delete datapoints
							config.app_data.redis_clients[server].client.zrangebyscore([slot+':'+key+':'+interval, 0, cutoff_time], function(err, reply) {
								if (err) { 
									if (logger.logLevel.error == true) { logger.log.error('Error reading from Redis server: '+server); }
								}
								else { 
																	
									var values = reply.toString().split(",");
									
									while (values.length > 0 ) {
										var agg_datapoint = 0;
										var elements = values.splice(0,num_agg_datapoints);
										var timestamp = 0;
										
										
										if ( elements.length == num_agg_datapoints ) {
											elements.forEach( function( element) {
												agg_datapoint += parseFloat(element.split(":")[0]);
											});
											timestamp = parseInt(elements[num_agg_datapoints-1].split(":")[1]);
											agg_datapoint = agg_datapoint / num_agg_datapoints;
											
											
											// write new aggregated data back to redis
											var payload = [];
											payload.push(key+':'+next_schema_interval, timestamp, agg_datapoint+':'+timestamp);
											app_utils.write_to_redis(key, payload);
											
											// delete old data from current retention period
											config.app_data.redis_clients[server].client.zremrangebyscore([slot+':'+key+':'+interval, 0, timestamp], function(err, reply) {
												if (err) { 
													if (logger.logLevel.error == true) { logger.log.error('Error reading from Redis server: '+server); }
												}
												else {
									//				console.log('deleted: '+reply+' datapoints from: '+key+':'+interval);
												}
											});
										}
											
								//		
										
								//		console.log('key: '+key+', datapoint: '+datapoint+', timestamp: '+timestamp);
									}
								}
							});
						}
						if ( action == 'del' ) {
							// delete datapoints
							config.app_data.redis_clients[server].client.zremrangebyscore([slot+':'+key+':'+interval, 0, cutoff_time], function(err, reply) {
								if (err) { 
									if (logger.logLevel.error == true) { logger.log.error('Error reading from Redis server: '+server); }
								}
								else {
									console.log('deleted: '+reply+' datapoints from: '+slot+':'+key+':'+interval+', cutoff_time: '+cutoff_time);
								}
							});
						}
					}
					
				});
				
			});
		}
	}
	
	function lookup_schema(key, done) {
		var breakLoop = false;
		config.app_data.schemas.forEach( function(schema) {
			if (!breakLoop) {
				utility.matchRegexp( key, schema.pattern, function(matched, matches) {
					if ( matched ) {
						breakLoop = true;
						done(schema);
					}
				});
			}
				
		});
			
	}
	
}
