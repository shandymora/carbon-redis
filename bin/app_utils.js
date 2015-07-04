/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

// Include modules
var utility			= require('./utility');
var config			= require('./config');

// Logging parameters
var currentDir = config.currentDir;
var logger = config.app_data.logger;

/*
 * 		datapoints: 	array [ [value, timestamp], [value, timestamp] ]. Datapoints to aggregate
 * 		method:			'avg', 'sum', 'min', 'max'.  Aggregation method
 * 		agg_factor:		integer.  Number of datapoints to aggregate together
 * 
 */
function aggregate_datapoints(datapoints, method, agg_factor) {
	if ( method == null ) {
		method = 'avg';
	}
	if ( !(utility.isArray(datapoints)) ) {
		console.log('Datapoints not array');
		return null;
	}
	if ( agg_factor == null ) {
		agg_factor = datapoints.length;
	} 
	
	var agg_datapoint = 0;
	switch (method) {
		case 'avg':
			if ( !(utility.isInteger(agg_factor))) {
				console.log('Aggregation factor should be an integer');
			}
			// Track number of datapoints
			var point_count = 0;
			datapoints.forEach( function(point) {
				if ( point[0] != null ) { 			// If the datapoint has a value
					agg_datapoint += point[0]; 
					point_count += 1;
				}
			});
			// If number of points is the same as aggreagtion factor, divide by agg_factor
			if ( point_count == agg_factor ) { agg_datapoint = agg_datapoint / agg_factor; }
			// If there was at least one non-null datapoint, but less than agg_factor
			else if ( point_count > 0 ) { agg_datapoint = agg_datapoint / point_count; } 
			// Otherwise set agg_datapoint top null
			else { agg_datapoint = null; }	
													
			return [ agg_datapoint, datapoints[datapoints.length-1][1] ];
			break;
			
		case 'sum': 
			// Track number of datapoints
			var point_count = 0;
			datapoints.forEach( function(point) {
				if ( point[0] != null ) { 			// If the datapoint has a value
					agg_datapoint += point[0];
					point_count += 1;
				}
			});
			
			// If there was at least one non-null datapoint
			if ( point_count > 0 ) {
				return [ agg_datapoint, datapoints[datapoints.length-1][1] ];
			// Otherwise return null for datapoint value
			} else {
				return [ null, datapoints[datapoints.length-1][1] ];
			}
			
			break;
		
		case 'min':
			// Track number of datapoints
			var point_count = 0;
			var min_datapoint = datapoints[0];
			datapoints.forEach( function(point) {
				if ( min_datapoint[0] == null && point[0] != null ) { min_datapoint = point; }
				if ( point[0] != null ) {
					if ( point[0] <= min_datapoint[0] ) {
						min_datapoint = point;
						point_count += 1;
					}
				}
			});
			
			// If there was at least one non-null datapoint
			if ( point_count > 0 ) { return min_datapoint; }
			else { return null; }
			
			break;
			
		case 'max':
			// Track number of datapoints
			var point_count = 0;
			var max_datapoint = datapoints[0];
			datapoints.forEach( function(point) {
				if ( max_datapoint[0] < point[0] ) {
					max_datapoint = point;
					point_count += 1;
				}
			});
			
			if ( point_count > 0 ) { return max_datapoint; }
			else { return null; }
			
			break;
		default:
			console.log('Method not supported');
	}
	
	
}

/*
 * 		Read datapoints and timestamps from redis server for a given key, interval, from and until timestamps
 * 		server:		string:		name of Redis server ( config.settings.client.redis.<server> )
 * 		key:		string:		key to retrieve metric data for.
 * 		interval:	integer:	time in seconds for each datapoint
 * 		from:		integer:	unix timestamp (in seconds) 
 * 		until:		integer:	unix timestamp (in seconds)
 * 		done:		function:	callback
 */
function read_from_redis(server, key, interval, from, until, done) {
	
	// Are we connected to the Redis Server
	if ( config.app_data.redis_clients[server].connected ) {
		
		config.app_data.redis_clients[server].client.zrangebyscore([key+':'+interval, from, until], function(err, reply) {
			var datapoints = [];
			
			if (err) { 
				if (logger.logLevel.error == true) { logger.log.error('Error reading from Redis server: '+server); }
				done(datapoints);
			}
			else {
				var values = reply.toString().split(",");
				var datapoint = null;
				var timestamp = from;
				
				while (values.length > 0 ) {
					
					var datapoint = values.splice(0,1).toString();
					var point 		= parseFloat(datapoint.split(":")[0]);
					var timestamp 	= parseInt(datapoint.split(":")[1]);
			
					datapoints.push([point, timestamp]);
				}
				
			
				/* 	Return data
				 * 	
				 */
				done(datapoints);
			}
		});
	} else {
		// Not connected to Redis, do something useful
		if (logger.logLevel.error == true) { logger.log.error('Connection to Redis server: '+server+' down'); }
	}
}
function write_to_redis(key, payload) {
	
	// Compute hash to determine slot number
	var slot_payload = utility.crc16(key) % parseInt(config.settings.app.slot_count);

	if ( slot_payload.toString() in config.app_data.slot_lookup ) {
		config.app_data.slot_lookup[slot_payload.toString()].servers.forEach( function(server) {
			write_to_server(server);
		});
			
	} else {
		if (logger.logLevel.error == true) { logger.log.error('Slot: '+slot_payload+' not found!'); }
	}
	
	function write_to_server(server) {
		if ( config.app_data.redis_clients[server].connected ) {
			config.app_data.redis_clients[server].client.zadd(payload, function(err, reply) {
				if (err) { if (logger.logLevel.error == true) { logger.log.error('Error writing to server: '+server, { error:err }); } }
				else {
					utility.statsd.client.increment(utility.statsd.prefix+'app.redis.'+server+'.write');
				}
			});
		} else {
			// The Redis server is down, we must do something here
			if (logger.logLevel.error == true) { logger.log.error('Connection to Redis server: '+server+' down'); }
		}
	}
	
}


exports.aggregate_datapoints = aggregate_datapoints;
exports.read_from_redis = read_from_redis;
exports.write_to_redis = write_to_redis;