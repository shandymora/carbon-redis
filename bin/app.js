/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

// Include modules
var config		= require('./config');
var $			= require('jquery');
var client		= require('./client');
var server		= require('./server');
var utility		= require('./utility');
var app_utils	= require('./app_utils');

// Logging parameters
var currentDir = config.currentDir;
var logger = config.app_data.logger;

function start(oSettings) {

	// Setup app cluster
	clusterApp(oSettings.app.cluster, function() {
		init_slot_lookup(oSettings.client.redis);
	});

	// initialize statsd client
	utility.statsd = new client.statsd(oSettings.client.statsd);
	// Setup timer to send health stats every 60s
	utility.send_health_stats('app');
	
	// Pre-compile Regexp
	// line receiver pattern <key> <value> <timestamp>
	config.app_data.lineReceiverPattern = new RegExp(/^(\S+)\s+(\d+(\.\d+)?)\s+(\d+)/);
	
	// schema retentions
	init_schemas();
	
	// Whitelist of allowed key patterns
	config.settings.app.whitelist.forEach( function(pattern) {
		config.app_data.whitelist.push( new RegExp(pattern) );
	});
	// Blacklist of disallowed patterns
	config.settings.app.blacklist.forEach( function(pattern) {
		config.app_data.blacklist.push( new RegExp(pattern) );
	});
	
	// Connect to Redis
	client.start_redis_clients(oSettings, function() {
		server.start_tcp_servers(oSettings, parse_data);
		server.start_http_servers(oSettings);
	});
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
/*
function period_string_to_seconds (sPeriod, done) {
	var pattern = new RegExp(/^([0-9]+)([s,m,h,d,y]):([0-9]+)([s,m,h,d,y])$/);
	
	utility.matchRegexp( sPeriod, pattern, function(matched, matches) {
		if ( matched ) {
			
			// Calculate interval in seconds
			switch(matches[2]) {
				case 's':
					var interval = parseInt(matches[1]) * 1;
					break;
				case 'm':
					var interval = parseInt(matches[1]) * 60;
					break;
				case 'h':
					var interval = parseInt(matches[1]) * 3600;
					break;
				case 'd':
					var interval = parseInt(matches[1]) * 86400;
					break;
				case 'y':
					var interval = parseInt(matches[1]) * 31536000;
					break;
				default:
					// No match return null
			}
			
			// Calculate retention in seconds
			switch(matches[4]) {
				case 's':
					var retention = parseInt(matches[3]) * 1;
					break;
				case 'm':
					var retention = parseInt(matches[3]) * 60;
					break;
				case 'h':
					var retention = parseInt(matches[3]) * 3600;
					break;
				case 'd':
					var retention = parseInt(matches[3]) * 86400;
					break;
				case 'y':
					var retention = parseInt(matches[3]) * 31536000;
					break;
				default:
					// No match return null
			}
			
			if ( interval && retention ) {
				done(interval+':'+retention);
			}
		} else {
			console.log('  no match.');
		}
	});
}
*/
function init_slot_lookup(redis_config) {
	
	if ( config.health.cluster.is_master ) {
		if (config.app_data.logger.logLevel.info == true) { config.app_data.logger.log.info('Initializing Slots'); }
		for ( var server in redis_config ) {
			redis_config[server].slots.forEach( function(slots) {
				
				var items = slots.split("-");
				if ( items[1] ) {
					var startSlot = parseInt(items[0]);
					var endSlot = parseInt(items[1]);
				} else {
					var startSlot = parseInt(items[0]);
					var endSlot = parseInt(items[0]);
				}
				
				for ( index = startSlot; index <= endSlot; index++ ) {
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
				if ( redis_config[server].slot_default == true ) { default_servers.push(server); }
			}
		}
		
		// Find all unset slots and assign default server
		for ( var slot = 0; slot < parseInt(config.settings.app.slot_count); slot++ ) {
			if ( !(slot.toString() in config.app_data.slot_lookup) ) {
				config.app_data.slot_lookup[slot.toString()] = {
					servers: default_servers
				};
			}
		}
	}
		
} 

function parse_data(data) {
	var lines = data.toString().split(/\r\n|\n|\r/);
	
	// Is last lines element a part metric
	if ( config.app_data.lineRemainder != '' ) {
		lines[0] = config.app_data.lineRemainder + lines[0];
		config.app_data.lineRemainder = '';
	}
	
	lines.forEach( function(line) {
		if ( line != '' ) {
			utility.matchRegexp( line, config.app_data.lineReceiverPattern, function(matched, matches) {
				if ( matched ) {
					// Increment statsD counter
					utility.statsd.client.increment(utility.statsd.prefix+'app.metricsReceived');
					
					var key = matches[1];
					var value = matches[2];
					var timestamp = matches[4];
					var payload = [];
					var match_whitelist = false;
					var match_blacklist = false;
					
					// Parse whitelist
					config.app_data.whitelist.forEach( function(pattern) {
						utility.matchRegexp( key, pattern, function(matched, matches) {
							if (matched) {
								match_whitelist = true;
							}
						});
					});
					
					
					if ( match_whitelist ) {
						// Parse blacklist
						config.app_data.blacklist.forEach( function(pattern) {
							utility.matchRegexp( key, pattern, function(matched, matches) {
								if (matched) {
									match_blacklist = true;
								}
							});
						});
						if ( match_blacklist ) {
							utility.statsd.client.increment(utility.statsd.prefix+'app.blacklist_matches');
						}
					} else {
						utility.statsd.client.increment(utility.statsd.prefix+'app.whitelist_rejects');
					}
						
					
					if ( match_whitelist && !match_blacklist ) {
						utility.statsd.client.increment(utility.statsd.prefix+'app.whitelist_matches');
						
						// lookup schema and write to redis
						lookup_schema(key, function(interval) {
							payload.push(key+':'+interval, timestamp, value+':'+timestamp);
							app_utils.write_to_redis(key, payload);
						});
							
					}
				} else {
					if ( line == lines[lines.length-1] ) {
						config.app_data.lineRemainder = line;
					} else {
						if (logger.logLevel.warn == true) { logger.log.warn('Bad metric line from data', {lineRemainder:config.app_data.lineRemainder,line:line}); }
						utility.statsd.client.increment(utility.statsd.prefix+'app.badMetricsReceived');
					}
				}
			});
		}
	});
		
	
}

/*
 * 	retentions_s is sorted by interval in seconds.
 * 	first element in teh array will be the smallest interval which we should use.
 */
function lookup_schema(key, done) {
	var breakLoop = false;
	config.app_data.schemas.forEach( function(schema) {
		if (!breakLoop) {
			utility.matchRegexp( key, schema.pattern, function(matched, matches) {
				if ( matched ) {
					breakLoop = true;
					done(schema.retentions_s[0].split(":").toString().split(",")[0]);
				}
			});
		}
			
	});
		
}

function write_to_redis(key, payload) {
	
	// Compute hash to determine slot number
	var slot_payload = utility.crc16(key) % parseInt(config.settings.app.slot_count);

	if ( slot_payload.toString() in config.app_data.slot_lookup ) {
		config.app_data.slot_lookup[slot_payload.toString()].servers.forEach( function(server) {
			if ( config.app_data.redis_clients[server].connected ) {
				config.app_data.redis_clients[server].client.zadd(payload, function(err, reply) {
					if (err) { console.log('error: '+err); }
				});
			} else {
				// The Redis server is down, we must do something here
			}
		});
			
	} else {
		if (logger.logLevel.error == true) { logger.log.error('Slot: '+slot_payload+' not found!'); }
		console.log('Slot: '+slot_payload+' not found!');
	}
		
}

function isMaster(node, done) {
	client.httpConn(
		{
			oSettings: {
				url:		'http://'+node+':'+config.settings.server.http.port+'/health?cluster=is_master',
				sMethod:	'GET'
			}
		}, 
		function(err, statusCode, response) {
			if (err) {
				if (logger.logLevel.error == true) { logger.log.error('isMaster error', {node: node, statuscode: statusCode}); }
				
				// Record whether node is up
				if ( node in config.health.cluster.nodes) {
					config.health.cluster.nodes[node].available = false;
				} else {
					config.health.cluster.nodes[node] = {
						available:	false
					};
				}
				
				done(false, node); 
			}
			else {
				
				// Record whether node is up
				if ( node in config.health.cluster.nodes) {
					config.health.cluster.nodes[node].available = true;
				} else {
					config.health.cluster.nodes[node] = {
						available:	true
					};
				}
				
				if ( response.is_master == true ) {
					done(true, node);
				} else {
					done(false, node);
				}
			
				
			}
		});

}

function clusterApp(oSettings, done) {
	// Schedule regular checks of all nodes
	setInterval( function() {
		node_index = 0;
		found_master = false;
		oSettings.nodes.forEach( function(node) {
			if ( node != config.my_hostname   ) { 
				// Get node status
				isMaster(node, function(masterFound, checked_node) {
					if (masterFound) {
						
						if ( checked_node != config.health.cluster.master ) {
							// New master found
							if (logger.logLevel.debug == true) { logger.log.debug('New master: '+checked_node); }
							if ( 'ircBot' in config.settings.client ) {
								for (var bot in config.settings.client.ircBot) {
									app_clients[bot].sendMessage('New master: '+checked_node);
								}
							}
						}
					
						config.health.cluster.master = checked_node;
						config.health.cluster.is_master = false;
						found_master = true;
//						if (logger.logLevel.debug == true) { logger.log.debug('Set master: '+checked_node); }
					}
					node_index += 1;
					
					if ( node_index == oSettings.nodes.length ) {
						if ( ! found_master ) {
							// I will be master as I didn't find any
							config.health.cluster.master = config.my_hostname;
							config.health.cluster.is_master = true;
						}
					}
				});
			} else {
				node_index += 1;
			}
			
		});
	}, oSettings.heartbeat);
	
	// Determine master	
	// Are any nodes already up, what are their state?
	var node_index = 0;
	var found_master = false;
	oSettings.nodes.forEach( function(node) {
		if ( node != config.my_hostname   ) { 
			// Get node status
			isMaster(node, function(masterFound, checked_node) {
				if (masterFound) {
					
					if ( checked_node != config.health.cluster.master ) {
						// New master found
						if (logger.logLevel.debug == true) { logger.log.debug('New master: '+checked_node); }
						if ( 'ircBot' in config.settings.client ) {
							for (var bot in config.settings.client.ircBot) {
								config.app_data.ircBots[bot].sendMessage('New master: '+checked_node);
							}
						}
							
					}
					
					config.health.cluster.master = checked_node;
					config.health.cluster.is_master = false;
					found_master = true;
/*
					if (logger.logLevel.debug == true) { logger.log.debug('Set master: '+checked_node); }
					for (var bot in oSettings.client.ircBot) {
						app_clients[bot].sendMessage('Set master: '+checked_node);
					}
*/
				} 
				node_index += 1;
				
				if ( node_index == oSettings.nodes.length ) {
					if ( ! found_master ) {
						// I will be master as I didn't find any
						config.health.cluster.master = config.my_hostname;
						config.health.cluster.is_master = true;
						if ( 'ircBot' in config.settings.client ) {
							for (var bot in config.settings.client.ircBot) {
								config.app_data.ircBots[bot][bot].sendMessage('I am master');
							}
						}
					}
					if ( done ) { done(); }
				}
			});
			
		} else {
			node_index += 1;
			if ( node_index == oSettings.nodes.length ) {
				if ( ! found_master ) {
					// I will be master as I didn't find any
					config.health.cluster.master = config.my_hostname;
					config.health.cluster.is_master = true;
					if ( 'ircBot' in config.settings.client ) {
						for (var bot in config.settings.client.ircBot) {
							config.app_data.ircBots[bot][bot].sendMessage('I am master');
						}
					}
				}
				if ( done ) { done(); }
			}
		}
		
	});
	
	
}

function exceptionHandler() {
	process.on('uncaughtException', function(err) {
	    // handle and log the error safely
	    console.log('BIG ASS UNHANDLED EXCEPTION: '+JSON.stringify(err,undefined,2));
	});
}

// Module exports
exports.start = start;
