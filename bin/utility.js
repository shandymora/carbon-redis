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

// start statsd client
var statsd = {};

exports.matchRegexp = matchRegexp;
exports.find_key = find_key;
exports.isArray = isArray;
exports.uniqueArray = uniqueArray;
exports.timeInSecs = timeInSecs;
exports.statsd = statsd;
