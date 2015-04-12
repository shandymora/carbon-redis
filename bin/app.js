// Include modules
var config		= require('./config');
var $			= require('jquery');
var client		= require('./client');
var server		= require('./server');
var utility		= require('./utility');

// Logging parameters
var logLevel = config.logLevel;
var log = config.log;
var currentDir = config.currentDir;

 
config.app_data = {
	tcp_servers:			{},
	http_servers:			{},
	redis_conn:				{},
	lineReceiverPattern:	{},
	whitelist:				[],
	blacklist:				[]
};

function start(oSettings) {

	
	// initialize statsd client
	utility.statsd = new client.statsd(oSettings.client.statsd);
	
	// start http servers
	if ( 'http' in oSettings.server ) {
		if ( utility.isArray(oSettings.server.http.port) ) {
			oSettings.server.http.port.forEach( function(port) {
				config.app_data.http_servers[port.toString()] = new server.http_server(port);
			});
		} else {
			config.app_data.http_servers[oSettings.server.http.port.toString()] = new server.http_server(oSettings.server.http.port);
		}
	}
		
	
	// start tcp servers
	if ( 'tcp' in oSettings.server ) {
		if ( utility.isArray(oSettings.server.tcp.port) ) {
			oSettings.server.tcp.port.forEach( function(port) {
				config.app_data.tcp_servers[port.toString()] = new server.tcp_server(port, parse_data);
			});
		} else {
			config.app_data.tcp_servers[oSettings.server.tcp.port.toString()] = new server.tcp_server(oSettings.server.tcp.port, parse_data);
		}
	}
		

	// Connect to Redis
	if ( 'redis' in oSettings.client ) {
		config.app_data.redis_conn = new client.redisConn(oSettings.client.redis, function() {
			redis_set_options();
		});
	}

	// Pre-compile Regexp
	// line receiver pattern <key> <value> <timestamp>
	config.app_data.lineReceiverPattern = new RegExp(/^(\S+)\s+(\d+(\.\d+)?)\s+(\d+)/);
	// Whitelist of allowed key patterns
	config.settings.app.whitelist.forEach( function(pattern) {
		config.app_data.whitelist.push( new RegExp(pattern) );
	});
	// Blacklist of disallowed patterns
	config.settings.app.blacklist.forEach( function(pattern) {
		config.app_data.blacklist.push( new RegExp(pattern) );
	});
	
	
}

function redis_set_options() {
	
	// Set DB Option zset-max-ziplist-entries
	config.app_data.redis_conn.client.send_command('CONFIG', ['GET', 'zset-max-ziplist-entries'], function(err, reply) {
		var options = reply.toString().split(',');
		if ( options[1] != '3000' ) {
			config.app_data.redis_conn.client.send_command('CONFIG', ['SET', 'zset-max-ziplist-entries', '3000'], function(err, reply) {
				if (logLevel.info == true) { log.info('SET: '+reply); }
			});
		}
	});

	// Set DB Option zset-max-ziplist-value
	config.app_data.redis_conn.client.send_command('CONFIG', ['GET', 'zset-max-ziplist-value'], function(err, reply) {
		var options = reply.toString().split(',');
		if ( options[1] != '256' ) {
			config.app_data.redis_conn.client.send_command('CONFIG', ['SET', 'zset-max-ziplist-value', '256'], function(err, reply) {
				if (logLevel.info == true) { log.info('SET: '+reply); }
			});
		}
	});
	
	
}

function parse_data(data) {
	var lines = data.toString().split(/\n|\r|\r\n/);
	
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
						
						// write to redis
						payload.push(key+':60s', timestamp, value+':'+timestamp);
						config.app_data.redis_conn.client.zadd(payload, function(err, reply) {
							if (err) { console.log('error: '+err); }
						});
					}
				} else {
					if (logLevel.warn == true) { log.warn('Bad metric line: '+line); }
					utility.statsd.client.increment(utility.statsd.prefix+'app.badMetricsReceived');
				}
			});
		}
	});
		
	
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
				if (logLevel.error == true) { log.error('isMaster error', {node: node, response: response, statuscode: statusCode}); }
				
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

function clusterApp(oSettings) {
	
	// Determine master
	
	// Are any nodes already up, what are their state?
	var node_index = 0;
	var found_master = false;
	config.settings.app.nodes.forEach( function(node) {
		if ( node != config.my_hostname   ) { 
			// Get node status
			isMaster(node, function(masterFound, checked_node) {
				if (masterFound) {
					
					if ( checked_node != config.health.cluster.master ) {
						// New master found
						if (logLevel.debug == true) { log.debug('New master: '+checked_node); }
						for (var bot in config.settings.client.ircBot) {
							app_clients[bot].sendMessage('New master: '+checked_node);
						}
					}
					
					config.health.cluster.master = checked_node;
					config.health.cluster.is_master = false;
					found_master = true;
/*
					if (logLevel.debug == true) { log.debug('Set master: '+checked_node); }
					for (var bot in oSettings.client.ircBot) {
						app_clients[bot].sendMessage('Set master: '+checked_node);
					}
*/
				} 
				node_index += 1;
				
				if ( node_index == config.settings.app.nodes.length ) {
					if ( ! found_master ) {
						// I will be master as I didn't find any
						config.health.cluster.master = config.my_hostname;
						config.health.cluster.is_master = true;
						for (var bot in config.settings.client.ircBot) {
							app_clients[bot].sendMessage('I am master');
						}
					}
				}
			});
			
		} else {
			node_index += 1;
		}
		
	});
	
	// Schedule regular checks of all nodes
	setInterval( function() {
		node_index = 0;
		found_master = false;
		config.settings.app.nodes.forEach( function(node) {
			if ( node != config.my_hostname   ) { 
				// Get node status
				isMaster(node, function(masterFound, checked_node) {
					if (masterFound) {
						
						if ( checked_node != config.health.cluster.master ) {
							// New master found
							if (logLevel.debug == true) { log.debug('New master: '+checked_node); }
							for (var bot in config.settings.client.ircBot) {
								app_clients[bot].sendMessage('New master: '+checked_node);
							}
						}
					
						config.health.cluster.master = checked_node;
						config.health.cluster.is_master = false;
						found_master = true;
//						if (logLevel.debug == true) { log.debug('Set master: '+checked_node); }
					}
					node_index += 1;
					
					if ( node_index == config.settings.app.nodes.length ) {
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
}

function exceptionHandler() {
	process.on('uncaughtException', function(err) {
	    // handle and log the error safely
	    console.log('BIG ASS UNHANDLED EXCEPTION: '+JSON.stringify(err,undefined,2));
	});
}

// Module exports
exports.start = start;
