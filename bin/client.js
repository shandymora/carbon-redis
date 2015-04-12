// Include modules
var config		= require('./config');
var net			= require('net');
var exec 		= require('child_process').exec;
var $			= require('jquery');
var StatsD		= require('node-statsd').StatsD;
var irc 		= require("irc");
var utility		= require('./utility');
var redis		= require('redis');

// Logging parameters
var logLevel = config.logLevel;
var log = config.log;
var currentDir = config.currentDir;

// StatsD 

function statsd(oSettings) {
	this.client = {};
	
	this.start = function() {
		self.client = new StatsD(oSettings.server);
		
		self.client.socket.on('error', function(error) {
			if (logLevel.error == true) { log.error('Error from statsd_client', { error: error } ); }
		});


	};
	
	this.stop = function() {
		
	};
	
	this.get_config = function() {
		return ({
			server:	oSettings.server,
			prefix: self.statsd_prefix
		});
	};
	
	this.prefix = oSettings.prefix+'.'+config.my_hostname+'.';
	
	var self = this;
	self.start();
	
}

// Elasticsearch
function elasticsearch(options) {
	
	this.conn_options = {
		sMethod:	'POST',
		oSettings:	{},
		sPayload:	'',
		dataType:	'json'
	};
	
	$.extend(true, this.conn_options, options);
	
	this.send = function(send_options, done) {
		
		if (send_options) {
			$.extend(true, self.conn_options, send_options);
		}
		
		$.ajax({
		    type: 	self.conn_options.sMethod,
		    url: 	self.conn_options.oSettings.url,
		    data: 	self.conn_options.sPayload, 				// or JSON.stringify ({name: 'jonas'}),
		    success: function(data, status, xhr) {
		    	var responseStatus = xhr.statusCode();
				if (logLevel.debug == true) { log.debug('xhr.statusCode: '+JSON.stringify(responseStatus.status,undefined,2)); }
				
				var responseHeader = xhr.getResponseHeader("Content-Type");		
				if (logLevel.debug == true) { log.debug('xhr.Content-Type: '+JSON.stringify(responseHeader,undefined,2)); }
		 
		 		if (logLevel.info == true) { log.info('Successfully indexed message', {data:data}); }
		 		
		    	done(false, responseStatus.status, data);
		    },
		    error: function(xhr, status, err) {
		    	if (logLevel.warn == true) { log.warn('Unable to post message', { error: err } ); }
		    	
		    	var responseStatus = xhr.statusCode();
				if (logLevel.error == true) { log.error('ERROR - xhr.statusCode: '+JSON.stringify(responseStatus.status,undefined,2)); }
				
				var responseHeader = xhr.getResponseHeader("Content-Type");		
				if (logLevel.error == true) { log.error('ERROR: xhr.Content-Type: '+JSON.stringify(responseHeader,undefined,2)); }
		    	done(true, responseStatus.status, err);
		    },
		    contentType: "application/json",
		    dataType: 'json'
		});
	};
	
	this.close = function(done) {
		
	};
	
	var self = this;
	if (logLevel.info == true) { log.info('Created Elasticsearch client', { url: this.conn_options.oSettings.url } ); }
	
}

// IRC
function ircBot(oSettings) {

	this.bot =		{};
	
	this.start = function() {
		
		// Create the bot
		self.bot = new irc.Client(oSettings.server, oSettings.botName, {
			channels : oSettings.channels });
		
		// Listen for bot errors
		self.bot.addListener("error", function(message) {
			if (logLevel.error == true) { log.error('IRC BOT error: ', message); }
		});
		if (logLevel.info == true) { log.info(oSettings.botName+' joined channel '+oSettings.channels.join(", ")); }
	};
	
	this.sendMessage = function(message) {
		
		self.bot.say(oSettings.channels, message );
		utility.statsd.client.increment(utility.statsd.prefix+'app.client.ircBot.say_count');
	};
	
	this.stop = function() {
		
	};
	
	var self = this;
	self.start();
	
};

// TCP
function tcpConn (oSettings) {
	
	this.connected 	= false;
	this.error		= false;
	var conn = {};
	var retryInterval = 3000;
	var retryCount = 1;
	var maxRetries = 30;
	
	var server = '';
	
	var self = this;
	
	connect();
	
	function connect() {
		conn = net.createConnection(oSettings.port, oSettings.host);
		conn.setKeepAlive(true);
		
		conn.on('connect', function(socket) { 
			retryCount = 0; 
			if (logLevel.info == true) { log.info('connected to '+oSettings.host+' on port '+oSettings.port); }
			server = oSettings.host;
			self.connected = true;
		});
		
		conn.on('error', function(err) { 
			if (logLevel.info == true) { log.info('Error in connecting to '+oSettings.host+' on port '+oSettings.port, {error: err}); }
			self.connected = false;
			self.error = true;
		});
		conn.on('close', function() { 
			if (logLevel.info == true) { log.info('connection to '+oSettings.host+' on port '+oSettings.port+' closed'); }
			self.connected = false;
			reconnect(); 
		});
		
		function reconnect() {
			if (retryCount >= maxRetries) { 
				if (logLevel.info == true) { log.info('Max retries to '+oSettings.host+' on port '+oSettings.port+' have been exceeded, giving up.'); }
				self.connected = false;
				conn.end();
			} else {
				retryCount += 1; 
				setTimeout(connect, retryInterval);
			}
			
		}
	};
	
	this.write = function(line, done) {
		conn.write(line);
		done();
	};
	
	this.close = function(done) {
		conn.end();
		done();
	};
}

function httpConn (options, done) {
	
	var conn_options = {
		sMethod:	'POST',
		oSettings:	{},
		sPayload:	'',
		dataType:	'json'
	};
	
	$.extend(true, conn_options, options);
	
	try {
		$.ajax({
		    type: 	conn_options.sMethod,
		    url: 	conn_options.oSettings.url,
		    data: 	conn_options.sPayload, 
		    success: function(data, status, xhr) { 
	
				var responseStatus = xhr.statusCode();
				if (logLevel.debug == true) { log.debug('xhr.statusCode: '+JSON.stringify(responseStatus.status,undefined,2)); }
				
				var responseHeader = xhr.getResponseHeader("Content-Type");		
				if (logLevel.debug == true) { log.debug('xhr.Content-Type: '+JSON.stringify(responseHeader,undefined,2)); }
		 
		    	done(false, responseStatus.status, data);
	
		    },
		    error: function(xhr, status, err) {
		    	if (logLevel.warn == true) { log.warn('Unable to post message', { error: err } ); }
	    	
		    	var responseStatus = xhr.statusCode();
				if (logLevel.error == true) { log.error('ERROR - xhr.statusCode: '+JSON.stringify(responseStatus.status,undefined,2)); }
				
				var responseHeader = xhr.getResponseHeader("Content-Type");		
				if (logLevel.error == true) { log.error('ERROR: xhr.Content-Type: '+JSON.stringify(responseHeader,undefined,2)); }
		    	done(true, responseStatus.status, err);
		    },
		    contentType: "application/json",
		    dataType: conn_options.dataType
		});
	} catch(error) {
		console.log('BIG ASS HTTP ERROR: '+JSON.stringify(error,undefined,2));
	}
}

function redisConn (oSettings, ready_callback) {
	

	var oOptions = {
		server:		'localhost',
		port:		6379,
		db:			0
	};
	
	$.extend(true, oOptions, oSettings);
	
	var connect_options = {
		retry_max_delay:	10000
	};
	
	this.connected 	= false;
	this.error		= false;

	this.client = redis.createClient(oOptions.port, oOptions.server, connect_options);
	
	this.client.select(oOptions.db, function() {});
	
	var self = this;
	
	this.client.on("error", function (err) {
		if (logLevel.error == true) { log.error('Redis Error: '+err, {error: err}); }
		self.connected = false;
		self.error = true;
		console.log('Redis Error: '+err);
  	});
  	
	this.client.on('ready', function() {
		if (logLevel.info == true) { log.info('Connected to Redis server: '+oSettings.server); }
		self.connected = true;
		if ( ready_callback ) { ready_callback(); }
	});
	
	this.client.on('drain', function() {
		console.log('Drain detected.');
	});
	
	this.client.on('end', function() {
		if (logLevel.warn == true) { log.warn('Connection to Redis server ended'); }
		console.log('Connection ended');
		self.connected = false;
	});
	
}

exports.tcpConn = tcpConn;
exports.httpConn = httpConn;
exports.statsd = statsd;
exports.elasticsearch = elasticsearch;
exports.ircBot = ircBot;
exports.redisConn = redisConn;