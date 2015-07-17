/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

var os			= require('os');
var currentDir 	= __dirname;

// Export variables/functions
exports.health = {
	memory:		{
		leak:	[]
	},
	cluster: {
		is_master:	false,
		master:		'',
		nodes:		{}
	}
};

exports.currentDir = currentDir;
exports.configFile = currentDir+'/../conf/config.json';
exports.my_hostname = os.hostname();
exports.settings = {};
exports.app_data = {};