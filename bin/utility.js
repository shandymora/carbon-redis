/*
 * project:	red-timestore
 * author: 	amora
 * 
 */

// Include modules
var config			= require("./config");
var utility			= require("./utility");
var $				= require('jquery');
var winston			= require('winston');
var fs 				= require('fs');

// Setup variables for CRC16 function
var Buffer, TABLE;

Buffer = require('buffer').Buffer;

TABLE = [0x0000, 0xc0c1, 0xc181, 0x0140, 0xc301, 0x03c0, 0x0280, 0xc241, 0xc601, 0x06c0, 0x0780, 0xc741, 0x0500, 0xc5c1, 0xc481, 0x0440, 0xcc01, 0x0cc0, 0x0d80, 0xcd41, 0x0f00, 0xcfc1, 0xce81, 0x0e40, 0x0a00, 0xcac1, 0xcb81, 0x0b40, 0xc901, 0x09c0, 0x0880, 0xc841, 0xd801, 0x18c0, 0x1980, 0xd941, 0x1b00, 0xdbc1, 0xda81, 0x1a40, 0x1e00, 0xdec1, 0xdf81, 0x1f40, 0xdd01, 0x1dc0, 0x1c80, 0xdc41, 0x1400, 0xd4c1, 0xd581, 0x1540, 0xd701, 0x17c0, 0x1680, 0xd641, 0xd201, 0x12c0, 0x1380, 0xd341, 0x1100, 0xd1c1, 0xd081, 0x1040, 0xf001, 0x30c0, 0x3180, 0xf141, 0x3300, 0xf3c1, 0xf281, 0x3240, 0x3600, 0xf6c1, 0xf781, 0x3740, 0xf501, 0x35c0, 0x3480, 0xf441, 0x3c00, 0xfcc1, 0xfd81, 0x3d40, 0xff01, 0x3fc0, 0x3e80, 0xfe41, 0xfa01, 0x3ac0, 0x3b80, 0xfb41, 0x3900, 0xf9c1, 0xf881, 0x3840, 0x2800, 0xe8c1, 0xe981, 0x2940, 0xeb01, 0x2bc0, 0x2a80, 0xea41, 0xee01, 0x2ec0, 0x2f80, 0xef41, 0x2d00, 0xedc1, 0xec81, 0x2c40, 0xe401, 0x24c0, 0x2580, 0xe541, 0x2700, 0xe7c1, 0xe681, 0x2640, 0x2200, 0xe2c1, 0xe381, 0x2340, 0xe101, 0x21c0, 0x2080, 0xe041, 0xa001, 0x60c0, 0x6180, 0xa141, 0x6300, 0xa3c1, 0xa281, 0x6240, 0x6600, 0xa6c1, 0xa781, 0x6740, 0xa501, 0x65c0, 0x6480, 0xa441, 0x6c00, 0xacc1, 0xad81, 0x6d40, 0xaf01, 0x6fc0, 0x6e80, 0xae41, 0xaa01, 0x6ac0, 0x6b80, 0xab41, 0x6900, 0xa9c1, 0xa881, 0x6840, 0x7800, 0xb8c1, 0xb981, 0x7940, 0xbb01, 0x7bc0, 0x7a80, 0xba41, 0xbe01, 0x7ec0, 0x7f80, 0xbf41, 0x7d00, 0xbdc1, 0xbc81, 0x7c40, 0xb401, 0x74c0, 0x7580, 0xb541, 0x7700, 0xb7c1, 0xb681, 0x7640, 0x7200, 0xb2c1, 0xb381, 0x7340, 0xb101, 0x71c0, 0x7080, 0xb041, 0x5000, 0x90c1, 0x9181, 0x5140, 0x9301, 0x53c0, 0x5280, 0x9241, 0x9601, 0x56c0, 0x5780, 0x9741, 0x5500, 0x95c1, 0x9481, 0x5440, 0x9c01, 0x5cc0, 0x5d80, 0x9d41, 0x5f00, 0x9fc1, 0x9e81, 0x5e40, 0x5a00, 0x9ac1, 0x9b81, 0x5b40, 0x9901, 0x59c0, 0x5880, 0x9841, 0x8801, 0x48c0, 0x4980, 0x8941, 0x4b00, 0x8bc1, 0x8a81, 0x4a40, 0x4e00, 0x8ec1, 0x8f81, 0x4f40, 0x8d01, 0x4dc0, 0x4c80, 0x8c41, 0x4400, 0x84c1, 0x8581, 0x4540, 0x8701, 0x47c0, 0x4680, 0x8641, 0x8201, 0x42c0, 0x4380, 0x8341, 0x4100, 0x81c1, 0x8081, 0x4040];

if (typeof Int32Array !== 'undefined') {
  TABLE = new Int32Array(TABLE);
}
	
function readConfig(file, done) {
  fs.readFile(file, function (err, contents) {
  	config.app_data.logger.log.info('Loaded config file');
  	
    // If error
    if (err) { 
      if (config.app_data.logger.logLevel.error == true) { config.app_data.logger.log.error('Error opening config file', { error: err } ); }
      console.log('error reading config file: '+JSON.stringify(err, undefined, 2));
      process.exit(1);
    }
	
    // Test if well formed JSON
    try {
		var app_config = $.parseJSON(contents.toString());
	} catch (error) {
		if (config.app_data.logger.logLevel.error == true) { config.app_data.logger.log.error('Invald JSON configuration file'); }
		console.log('error reading config file: '+JSON.stringify(error, undefined, 2));
		process.exit(1);
	}

	config.settings = app_config;
	if (done) { done(app_config); }
	
  });
}

function matchRegexp( o, regexp, done) {
  if ( !( regexp.test( o ) ) ) {
        done (false, null);
  } else {
        var patt = regexp.exec(o);
        done (true, patt);
  }
}

function find_key(key, value, obj) {
	var matched = false;
	for (var property in obj) {
        if (obj.hasOwnProperty(property)) {
            if (typeof obj[property] == "object") {
                matched = find_key(key, value, obj[property]);
            } else {
            	if ( property == key ) { 
            		// keys match
            		if ( obj[property] == value ) {
            			// Values match too
            			return true;
            		}
            	}
           }
        }
    }
    return matched;
}

function isArray(o) {
  return Object.prototype.toString.call(o) === '[object Array]';
}

function arrayMin(arr) {
  var len = arr.length, min = Infinity;
  while (len--) {
    if (arr[len] < min) {
      min = arr[len];
    }
  }
  return min;
}

function arrayMax(arr) {
  var len = arr.length, max = -Infinity;
  while (len--) {
    if (arr[len] > max) {
      max = arr[len];
    }
  }
  return max;
}

function isFloat(n) {
    return n === +n && n !== (n|0);
}

function isInteger(n) {
    return n === +n && n === (n|0);
}

function uniqueArray(a) {
	return a.reduce(function(p, c) {
        if (p.indexOf(c) < 0) p.push(c);
        return p;
    }, []);
}

function timeInSecs (sDateString) {
	if (sDateString) {
		var d = new Date(sDateString);
	} else {
		var d = new Date();
	}
    
    var now = d.getTime();

    now = now / 1000;
    now = parseInt(now.toFixed(0));
    return now;
}

function timeAsString (iDate) {
	if (iDate) {
		var d = new Date(iDate*1000);
	} else {
		var d = new Date();
	}
	
	return d.toLocaleString();
}

function Timer() {
	
	this.start_time = {};
	this.stop_time = {};
	this.elasped_time = {};
	
	this.start = function() {
		self.start_time = process.hrtime();
		return;
	};
	
	this.stop = function() {
		self.elasped_time = process.hrtime(self.start_time);
		return;
	};
	
	this.elasped_in_ms = function() {
		return self.elasped_time[1] / 1000000;
	};
	this.elasped_in_s = function() {
		return self.elasped_time[0];
	};
	this.elasped_in_ns = function() {
		return self.elasped_time[1];
	};
	var self = this;
	self.start();
}

function Logger(options, done_start) {
	
	this.logger_options = {
		file_path	: config.currentDir+'/../log/',
		log_for 	: 'events'
	};
	
	$.extend(true, this.logger_options, options);
	
	// Private
	this.log = new (winston.Logger);
	
	// Properties
	this.logLevel = {
		debug:	false,
		info:	true,
		warn:	true,
		error:	true
	};

	
	// Methods
	this.start = function() {
		self.log.add(winston.transports.File, { filename: self.logger_options.file_path + '-' + self.logger_options.log_for + '.log' } ); 
		self.log.info('Started Logging');
		if (done_start) { done_start(); } else { return; }
	};
	
	this.stop = function() {
		
	};
	
	var self = this;
	self.start();
	
}

function crc16(buf, previous) {
	
  var byte, crc, _i, _len;
  if (!Buffer.isBuffer(buf)) {
    buf = Buffer(buf);
  }
  crc = ~~previous;
  for (_i = 0, _len = buf.length; _i < _len; _i++) {
    byte = buf[_i];
    crc = (TABLE[(crc ^ byte) & 0xff] ^ (crc >> 8)) & 0xffff;
  }
  return crc;
}

function send_health_stats(process) {
	// Periodically send health status to StatsD, hard coded to 60s
	var last = {
		num_full_gc:	0,
		num_inc_gc:		0
	};
	
	var healthTimer = setInterval( function() {
		
		// Calculate change in counters over interval period.  Hardcoded to 60secs currently.
		var count_full_gc = config.health.memory.heap.num_full_gc - last.num_full_gc;
		var count_inc_gc = config.health.memory.heap.num_inc_gc - last.num_inc_gc;
		
		utility.statsd.client.gauge(utility.statsd.prefix+process+'.health.memory.heap.current_base', config.health.memory.heap.current_base);
		utility.statsd.client.gauge(utility.statsd.prefix+process+'.health.memory.heap.estimated_base', config.health.memory.heap.estimated_base);
		utility.statsd.client.gauge(utility.statsd.prefix+process+'.health.memory.heap.usage_trend', config.health.memory.heap.usage_trend);
		utility.statsd.client.gauge(utility.statsd.prefix+process+'.health.memory.heap.full_gc_count', config.health.memory.heap.num_full_gc);
		utility.statsd.client.gauge(utility.statsd.prefix+process+'.health.memory.heap.inc_gc_count', config.health.memory.heap.num_inc_gc);
		
		utility.statsd.client.increment(utility.statsd.prefix+process+'.health.memory.heap.full_gc', count_full_gc);
		utility.statsd.client.increment(utility.statsd.prefix+process+'.health.memory.heap.inc_gc', count_inc_gc);
/*		
		var process_memory_usage = process.memoryUsage();
		utility.statsd.client.increment(utility.statsd.prefix+process+'.health.memory.process.rss', process_memory_usage.rss);
		utility.statsd.client.increment(utility.statsd.prefix+process+'.health.memory.process.heapTotal', process_memory_usage.heapTotal);
		utility.statsd.client.increment(utility.statsd.prefix+process+'.health.memory.process.heapUsed', process_memory_usage.heapUsed);
*/		
		last.num_full_gc = config.health.memory.heap.num_full_gc;
		last.num_inc_gc = config.health.memory.heap.num_inc_gc;
		
	}, 60000);
}

// start statsd client
var statsd = {};

exports.readConfig = readConfig;
exports.matchRegexp = matchRegexp;
exports.find_key = find_key;
exports.isArray = isArray;
exports.arrayMin = arrayMin;
exports.arrayMax = arrayMax;
exports.isFloat = isFloat;
exports.isInteger = isInteger;
exports.uniqueArray = uniqueArray;
exports.timeInSecs = timeInSecs;
exports.timeAsString = timeAsString;
exports.statsd = statsd;
exports.Timer = Timer;
exports.Logger = Logger;
exports.crc16 = crc16;
exports.send_health_stats = send_health_stats;
