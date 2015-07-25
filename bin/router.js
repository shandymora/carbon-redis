/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

// Include modules
var url 		= require('url');
var handlers 	= require('./handlers');

var handle = {
	"/" : 				handlers.start,
  	"/health":  		handlers.appHealth,
  	"/metrics":			handlers.metrics,
  	"/metricnames": 	handlers.metricnames,
  	"/findmetricnames": handlers.find_metricnames
};

function route(pathname, response, request) {
  if (typeof handle[pathname] === 'function') {
    handle[pathname](response, request);
  } else {
  	handlers.sendfile(response, pathname);
  }
}

// Export variables/functions
exports.route = route;