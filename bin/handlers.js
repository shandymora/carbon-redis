// Include modules
var	fs 				= require("fs");
var $				= require("jquery");
var url				= require("url");
var mime			= require("mime");
var config			= require("./config");
var client			= require("./client");
var utility			= require('./utility');

// Logging parameters
var logLevel = config.logLevel;
var log = config.log;
var currentDir = config.currentDir;

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
			}
			else {
		    	response.writeHeader("Content-Type", "application/json");
				response.writeHead(500);
		    	response.end(JSON.stringify({status:"failure",message:"unknown query",issued:utility.timeInSecs()}));
		    }
		    
		} else if ( query.cluster ) {
			
			// Respond with whether this node is teh cluster master
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
					if (logLevel.error == true) { log.error('RequestData Not JSON format'); }
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
									if (logLevel.error == true) { log.error('isMaster error', {node: node, response: response, statuscode: statusCode}); }

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


// Export variables/functions
exports.appHealth = appHealth;
exports.sendfile = sendfile;