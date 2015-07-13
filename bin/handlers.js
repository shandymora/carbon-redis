/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

// Include modules
var	fs 				= require("fs");
var $				= require("jquery");
var url				= require("url");
var querystring		= require("querystring");
var mime			= require("mime");
var config			= require("./config");
var client			= require("./client");
var utility			= require('./utility');
var app_utils		= require('./app_utils');

// Logging parameters
var currentDir = config.currentDir;
var logger = config.app_data.logger;

var metric_interval_regex = new RegExp(/^(\d+)s$/);

function appHealth(response, request) {

	if ( request.url ) {
	    var query = {};
	
	    try {
	      query = url.parse(request.url, true).query;
	    } catch (err) {
	      response.writeHead(500);
	      response.end('Error processing url parameteres\n'+JSON.stringify(err));
	      return;
	    }
	}
	
	request.on('data', function(data) {
      requestData += data;
    });
    
    request.on('end', function() {
    	
		if ( query.show ) {
			
			if ( query.show == 'leak') {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({leak: config.health.memory.leak,issued:utility.timeInSecs()} ));
			} else if ( query.show == 'heap') {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({heap:config.health.memory.heap,issued:utility.timeInSecs()}));
			} else if ( query.show == 'slot_lookup') {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({slot_lookup:config.app_data.slot_lookup,issued:utility.timeInSecs()}));
			} else if ( query.show == 'schemas') {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({schemas:config.app_data.schemas,issued:utility.timeInSecs()}));
			}
			else {
		    	response.writeHeader("Content-Type", "application/json");
				response.writeHead(500);
		    	response.end(JSON.stringify({status:"failure",message:"unknown query",issued:utility.timeInSecs()}));
		    }
		    
		} else if ( query.cluster ) {
			
			// Respond with whether this node is the cluster master
			if ( query.cluster == 'is_master' ) {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
				response.end(JSON.stringify({is_master: config.health.cluster.is_master,issued:utility.timeInSecs()} ));
				
				// Set this node to master and issue a not master to all other nodes
			} else if ( query.cluster == 'set_master' ) {
				
				// Parse text if its JSON formatted
			    try {
					oPayload = $.parseJSON(requestData.toString());
				} catch (error) {
					if (logger.logLevel.error == true) { logger.log.error('RequestData Not JSON format'); }
					response.writeHeader("Content-Type", "application/json");
					response.writeHead(500);
		    		response.end(JSON.stringify({status:"failure",message:"RequestData not JSON format",issued:utility.timeInSecs()}));
		    		return;
				}
				
				// Set other nodes to not is_master
				oSettings.app.nodes.forEach( function(node) {
					if ( node != config.my_hostname   ) { 
						client.httpConn(
							{
								oSettings: {
									url:		'http://'+node+':'+config.settings.server.http.port+'/health?cluster=set_master',
									sMethod:	'POST',
									sPayload:	JSON.stringify(oPayload)
								}
							}, 
							function(err, statusCode, response) {
								if (err) {
									if (logger.logLevel.error == true) { logger.log.error('isMaster error', {node: node, response: response, statuscode: statusCode}); }

								}
								
							}
						);
					}
				});
				
				config.health.cluster.is_master = true;
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({is_master: config.health.cluster.is_master,issued:utility.timeInSecs()} ));
			} else if ( query.cluster == 'status' ) {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
		      	response.end(JSON.stringify({cluster: config.health.cluster,issued:utility.timeInSecs()} ));
			} else {
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(500);
		    	response.end(JSON.stringify({status:"failure",message:"unknown query",issued:utility.timeInSecs()}));
			}
	   } else {
	   		response.writeHeader("Content-Type", "application/json");
	   		response.writeHead(500);
			response.end(JSON.stringify({result:"error",message:"No query specified",issued:utility.timeInSecs()}));
	   }
	   
	});	  
}

function sendfile(response, filename) {
	fs.readFile(config.currentDir+'/../html'+filename, "binary", function (err, file) {
	    if (err) {
	      response.writeHead(500, {"Content-Type": "text/plain"});
	      response.end(err+"\n");
	      return;
	    }
	    response.setHeader("Content-Type", mime.lookup("html"+filename));
	    response.writeHead(200);
	    response.end(file, "binary");
	});
}

function metrics( response, request ) {
	
	
	var requestData = '';
	  
	request.on('data', function(data) {
      requestData += data;
    });
    
    request.on('end', function() {

    	// Is request body JSON
		var isJSON = false;
		try {
			var requestDataQuery = $.parseJSON(requestData.toString());
			console.log("request body isJSON");
			isJSON = true;

		} catch (error) {
			isJSON = false;
		}
		
		// Is requestBody a querystring
		if ( isJSON == false ) {
	    	try {
	    		var requestDataQuery = querystring.parse(requestData);
	    		console.log('request body is queryString');
	    	} catch (err) {
	    		if (logger.logLevel.error == true) { logger.log.error('Error parsing requestData query params'); }
				response.writeHead(500);
				response.end(JSON.stringify({status:"error",message:"Error parsing requestData query params",issued:utility.timeInSecs()}));
	    	}
		}
		
    	if ('url' in request) {
    		
    		try {
			  var requestURL = url.parse(request.url, true);
			} catch (err) {
			  if (logger.logLevel.error == true) { logger.log.error('Error parsing URL query params'); }
			  response.writeHead(400);
			  response.end(JSON.stringify(consolidated_metrics));
			}
			
			
			if (requestURL.query) {
				// Merge requestDataQuery with requestURL.query
				$.extend(true, requestURL.query, requestDataQuery);
				
				if ( ('target' in requestURL.query) && ('from' in requestURL.query) && ('until' in requestURL.query) ) {
					get_metrics(requestURL.query.target, requestURL.query.from, requestURL.query.until);
				
				} else {
					response.writeHeader("Content-Type", "application/json");
		      		response.writeHead(400);
		      		response.end(JSON.stringify({status:"error",message:"No target, from and until parameters",issued:utility.timeInSecs()}));
				}
			} else {
				console.log('metrics, query in requestURL');
				response.writeHeader("Content-Type", "application/json");
	      		response.writeHead(400);
	      		response.end(JSON.stringify({status:"error",message:"No Query parameters",issued:utility.timeInSecs()}));
			}
    	} else {
    		response.writeHeader("Content-Type", "application/json");
      		response.writeHead(400);
      		response.end(JSON.stringify({status:"error",message:"No URL in request",issued:utility.timeInSecs()}));
    	}
    });
    
	function get_metrics(key, from, until) {
		console.log("requested - key: "+key+", from: "+from+", until: "+until);
		// Compute slot
		var slot_payload = utility.crc16(key) % parseInt(config.settings.app.slot_count);
	
		var consolidated_datapoints = [];
		var consolidated_interval = null;
		
		var now = utility.timeInSecs() - 1;
		var sum_retention = 0;
		
		
		// Lookup schema retention interval, just takes first one for now.
		lookup_schema_retention( function(retentions) {
	
			/*
			 * 		For each retention, work out some timestamps and intervals
			 */
			var retention_count = 0;
			var e_retentions = [];			// enhanced retention information
			
			retentions.forEach( function(retention) {
				var e_retention = {};
				
				e_retention.interval = 	parseInt(retention.split(":").toString().split(",")[0]);
				e_retention.range = 	parseInt(retention.split(":").toString().split(",")[1]);
				
				e_retention.newest_timestamp = now - sum_retention;
				e_retention.oldest_timestamp = e_retention.newest_timestamp - e_retention.range;
				sum_retention += e_retention.range;
				
				e_retentions.push(e_retention);
							
				// If theres only one retention config, 
				if ( retentions.length == 1 ) {
					
					// Set consolidated_interval to this retention interval
					consolidated_interval = e_retention.interval;
					
					// Prefill consolidated_datapoints with null values and timestamps
					var consolidated_datapoints_length = Math.ceil( (until - from)/consolidated_interval);
					for ( var count = 0; count < consolidated_datapoints_length; count++) {
						consolidated_datapoints[count] = [null, from + (count * consolidated_interval)];
					}
				}
				
				// Find retention period to aggregate datapoints by
				if ( from >= e_retention.oldest_timestamp && from < e_retention.newest_timestamp && consolidated_interval == null ) {
					
					// Set consolidated_interval to this retention interval
					consolidated_interval = e_retention.interval;
					
					// Prefill consolidated_datapoints with null values and timestamps
					var consolidated_datapoints_length = Math.ceil( (until - from)/consolidated_interval);
					for ( var count = 0; count < consolidated_datapoints_length; count++) {
						consolidated_datapoints[count] = [null, from + (count * consolidated_interval)];
					}
				}
			});
			
			/*
			 * 	Did we setup consolidated data and intervals, if not we should use the longest (last one)
			 * 
			 */
			if ( consolidated_interval == null ) {
				// Set consolidated_interval to this retention interval
				consolidated_interval = parseInt(retentions[retentions.length-1].split(":").toString().split(",")[0]);
				
				// Prefill consolidated_datapoints with null values and timestamps
				var consolidated_datapoints_length = Math.ceil( (until - from)/consolidated_interval);
				for ( var count = 0; count < consolidated_datapoints_length; count++) {
					consolidated_datapoints[count] = [null, from + (count * consolidated_interval)];
				}
			}
			e_retentions.forEach( function(e_retention) {
					
				// Are we requesting data from this retention period?
				if ( from < e_retention.newest_timestamp ) {
					make_request({
						interval:			parseInt(e_retention.interval),
						range:				parseInt(e_retention.range),
						newest_timestamp:	parseInt(e_retention.newest_timestamp),
						oldest_timestamp:	parseInt(e_retention.oldest_timestamp)
					}, function() {
						
						retention_count += 1;
						
						if ( retention_count == e_retentions.length ) {
							// Processed all retentions
							
				      		// Build payload from consolidated_datapoints
				      		var payload_datapoints = [];
				      		consolidated_datapoints.forEach( function(datapoint) {
				      			payload_datapoints.push(datapoint[0]);
				      		});
				      		
				      		response.writeHeader("Content-Type", "application/json");
				      		response.writeHead(200);
				      		response.end(JSON.stringify({target:key,datapoints:payload_datapoints,interval:consolidated_interval}));
				      		
				      		// Increment statsD counter
							utility.statsd.client.increment(utility.statsd.prefix+'app.metricQuery');
						}
					});
				} else {
					
					retention_count += 1;
						
					if ( retention_count == retentions.length ) {
						// Processed all retentions
						
			      		// Build payload from consolidated_datapoints
			      		var payload_datapoints = [];
			      		consolidated_datapoints.forEach( function(datapoint) {
			      			payload_datapoints.push(datapoint[0]);
			      		});
			      		
			      		response.writeHeader("Content-Type", "application/json");
			      		response.writeHead(200);
			      		response.end(JSON.stringify({target:key,datapoints:payload_datapoints,interval:consolidated_interval}));
			      		
			      		// Increment statsD counter
						utility.statsd.client.increment(utility.statsd.prefix+'app.handlers.metricQuery');
					}
				}
	
			});			
		});
		
		
		
		function make_request(retention_config, done) {
			
			// Make request
			if ( slot_payload.toString() in config.app_data.slot_lookup ) {
				
				config.app_data.slot_lookup[slot_payload.toString()].servers.forEach( function(server, server_count) {
					
					// Increment statsD counter
					utility.statsd.client.increment(utility.statsd.prefix+'app.redis.'+server+'.read');
						
					app_utils.read_from_redis(server, key, retention_config.interval, from, until, function(server_datapoints) {
						
						// Merge server_datapoints with all other returned data arrays
						merge_datapoints(server_datapoints, retention_config);
						
				      	if ( server_count == config.app_data.slot_lookup[slot_payload.toString()].servers.length-1 ) {
				      		// finished querying all Redis servers
				      		done();
				      	}
					});
				});
					
			} else {
				// slot_payload not found in slot_lookup, something went very wrong here
				if (logger.logLevel.warn == true) { logger.log.warn('Slot not found for key: '+key); }
			}
		}
		
		/*
		 * 	merge passed datapoints array with consolidated_datapoints array. Useful for pulling data
		 * 	from multiple Redis servers and filling in any blanks ( nulls ).
		 */
		function merge_datapoints(datapoints, retention_config) {
			
			
			// Aggregate datapoints
			if ( retention_config.interval != consolidated_interval ) {
				
				// Aggregate datapoints to consolidated_interval
				var num_agg_datapoints = consolidated_interval / retention_config.interval;
				var new_datapoints = [];
				
				
				while ( datapoints.length > 0 ) {
					var elements = datapoints.splice(0,num_agg_datapoints);
					
					if ( elements.length == num_agg_datapoints ) {
						new_datapoints.push( app_utils.aggregate_datapoints(elements) );				
					}
				}
		
				datapoints = new_datapoints;
			}
			
			// Fit datapoints into consolidated_datapoints array, based upon timestamp
			datapoints.forEach( function(datapoint) {
				var loop_continue = true;
				for ( var con_datapoint_count = 0; con_datapoint_count < consolidated_datapoints.length-1; con_datapoint_count++ ) {
					if (loop_continue) {			
						if ( datapoint[1] >= consolidated_datapoints[con_datapoint_count][1] && datapoint[1] < consolidated_datapoints[con_datapoint_count+1][1]) {
							consolidated_datapoints[con_datapoint_count][0] = datapoint[0];
							loop_continue = false;
						}
					}
				}	
			});
			
			// Check last array element of datapoints and consolidated_datapoints
			if ( datapoints.length > 0 ) {
				if ( datapoints[datapoints.length-1][1] >= consolidated_datapoints[consolidated_datapoints.length-1][1]) {
					consolidated_datapoints[consolidated_datapoints.length-1][0] = datapoints[datapoints.length-1][0];
				}
			}
			
			
			return;				
		}
		
		/*
		 * 	Looks up the schema, matching key to regex_string pattern.
		 * 	Returns schema retentions in seconds array ( schema.rententions_s )
		 */
		function lookup_schema_retention(done) {
			var breakLoop = false;
			config.app_data.schemas.forEach( function(schema) {
				console.log('Checking schema pattern: '+schema.pattern);
				if (!breakLoop) {
					utility.matchRegexp( key, schema.pattern, function(matched, matches) {
						if ( matched ) {
							console.log("    MATCHED: "+schema.retentions_s);
							breakLoop = true;
							done(schema.retentions_s);
						}
					});
				}
					
			});
		}

	}
}

function metricnames( response, request ) {
	
	var metrics = [];
	
	// Get number of redis servers
	var redisdb_count = Object.keys(config.app_data.redis_clients).length;
	
	var redisdb_client_count = 0;
	for ( var server in config.app_data.redis_clients ) {
		
		get_metricnames(server, function() {
			redisdb_client_count += 1;
			if ( redisdb_client_count == redisdb_count ) {
				
				// Ensure metrics array contains only unique elements
				metrics = utility.uniqueArray(metrics);
				
				response.writeHeader("Content-Type", "application/json");
				response.writeHead(200);
				response.end(JSON.stringify(metrics));
			}
		});
	}
	
	
	
	function get_metricnames(server, finished_getting_metricsnames) {
		
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
		      				metrics.push(item_elements[0]);
		      			}
		      		});
		      		
		      		if (items[0] != "0") {
		      //			metrics = metrics.concat(items.slice(1));
		      			done(items[0], done);
		      		} else {
		      //			metrics = metrics.concat(items.slice(1));
		      			finished_getting_metricsnames();
		      		}
				}
			});
		};			
	}
}

// Export variables/functions
exports.appHealth = appHealth;
exports.sendfile = sendfile;
exports.metrics = metrics;
exports.metricnames = metricnames;