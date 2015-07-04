/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

var os		= require('os');

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

exports.currentDir = __dirname;
exports.my_hostname = os.hostname();
exports.settings = {};
exports.app_data = {};