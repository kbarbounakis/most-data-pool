/**
 * MOST Web Framework
 * A JavaScript Web Framework
 * http://themost.io
 *
 * Copyright (c) 2014, Kyriakos Barbounakis k.barbounakis@gmail.com, Anthi Oikonomou anthioikonomou@gmail.com
 *
 * Released under the BSD3-Clause license
 * Date: 2015-08-16
 */
var util = require('util'),
    async = require('async');

function randomInt(min, max) {
    return Math.floor(Math.random()*max) + min;
}

function randomString(length) {

    length = length || 16;
    var chars = "abcdefghkmnopqursuvwxz2456789ABCDEFHJKLMNPQURSTUVWXYZ";
    var str = "";
    for(var i = 0; i < length; i++) {
        str += chars.substr(randomInt(0, chars.length-1),1);
    }
    return str;
}

function randomHex(length) {

    var buffer = new Buffer(randomString(length));
    return buffer.toString('hex');
}

/**
 * @class DataAdapterPool
 * @constructor
 * @param {{size:number,timeout:number,lifetime:number}=} options
 * @param {Function} ctor
 * @property {{size:number,timeout:number,lifetime:number}} options
 */
function DataAdapterPool(options, ctor) {
    this.options = util._extend({ maxConnections:30, timeout:30000, lifetime:1200000 }, options);
    /**
     * A collection of objects which represents the available pooled data adapters.
     */
    this.available = { };
    /**
     * A collection of objects which represents the pooled data adapters that are currently in use.
     */
    this.inUse = { };
    /**
     * An array of listeners that are currently waiting for a pooled data adapter.
     * @type {Function[]}
     */
    this.listeners = [ ];
    //set default state to active
    self.state = 'active';
    /**
     * @type {Function}
     * @returns DataAdapter
     */
    this.createObject = function() {
        return new ctor(options);
    };
}

DataAdapterPool.prototype.cleanup = function(callback) {
    try {
        var self = this;
        self.state = 'paused';
        var keys = Object.keys(self.available);
        async.eachSeries(keys, function(key,cb) {
            var item = self.available[key];
           if (typeof item === 'undefined' || item == null) { return cb(); }
            if (typeof item.close === 'function') {
                item.close(function() {
                    cb();
                });
            }
        }, function(err) {
            callback(err);
            //clear available collection
            keys.forEach(function(key) {
                delete self.available[key];
            });
            self.state = 'active';
        });
    }
    catch(e) {
        callback(e);
    }
};

/**
 *
 * @param {function(Error=,DataAdapter|*=)} callback
 */
DataAdapterPool.prototype.getObject = function(callback) {
    var self = this, newObj;
    callback = callback || function() {};

    if (self.state !== 'active') {
        var er = new Error('Connection refused due to pool state.');
        er.code = 'EPSTATE';
        return callback(er);
    }
    var inUseKeys = Object.keys(self.inUse);
    if ((inUseKeys.length < self.options.size) || (self.options.size == 0)) {
        //create new object
        newObj = self.createObject();
        //add createdAt property
        newObj.createdAt = new Date();
        //add object hash code
        newObj.hashCode = randomHex(6);
        //push object in inUse collection
        self.inUse[newObj.hashCode] = newObj;
        //return new object
        return callback(null, newObj);
    }
    else {
        var timeout;
        //register a connection pool timeout
        timeout = setTimeout(function() {
            //throw timeout exception
            var er = new Error('Connection pool timeout.');
            er.code = 'EPTIMEOUT';
            callback(er);
        }, self.options.timeout);
        self.listeners.push(function() {
            //clear timeout
            if (timeout) {
                clearTimeout(timeout);
            }
            var keys = Object.keys(self.available);
            if (keys.length>0) {
                var key = keys[0];
                //get connection from available connections
                var pooledObj = self.available[key];
                delete self.available[key];
                //push object in inUse collection
                self.inUse[pooledObj.hashCode] = pooledObj;
                //return pooled object
                callback(null, pooledObj);
            }
            else {
                //create new object
                newObj = self.createObject();
                //add createdAt property
                newObj.createdAt = new Date();
                //add object hash code
                newObj.hashCode = randomHex(6);
                //push object in inUse collection
                self.inUse[newObj.hashCode] = newObj;
                //return new object
                callback(null, newObj);
            }
        });
    }
};
/**
 *
 * @param {{hashCode:string}|*} obj
 * @param {function(Error=)} callback
 */
DataAdapterPool.prototype.releaseObject = function(obj, callback) {
    var self = this;
    callback = callback || function() {};
    if (typeof obj === 'undefined' || obj == null) {
        return callback();
    }
    try {
        //get the first listener
        var listener = self.listeners.unshift();
        //if listener exists
        if (typeof listener === 'function') {
            //execute listener
            listener.call(self);
        }
        else {
            //search inUse collection
            var ix = -1;
            for (var i = 0; i < this.inUse.length; i++) {
                if (this.inUse[i].hashCode === obj.hashCode) {
                    var used = this.inUse.splice(i,1);
                    //push connection to available connections
                    self.available.push(used);
                    break;
                }
            }
        }
        //finally exit
        callback();
    }
    catch(e) {
        callback(e);
    }
};
/**
 * @class DataAdapterPoolConnector
 * @constructor
 * @property {DataAdapterPool} pool - The underlying data adapter pool.
 * @property {*} rawConnection - The underlying data connection.
 */
function DataAdapterPoolConnector() {

}

/**
 * @param {function(Error=)} callback
 */
DataAdapterPoolConnector.prototype.connect = function(callback) {

};

/**
 * @param {function(Error=)} callback
 */
DataAdapterPoolConnector.prototype.disconnect = function(callback) {

};

var adpP = {
    /**
     * @constructs {DataAdapterPool}
     */
    DataAdapterPool:DataAdapterPool,
    /**
     * @constructs {DataAdapterPoolConnector}
     */
    DataAdapterPoolConnector:DataAdapterPoolConnector
};

if (typeof exports !== 'undefined') {
    module.exports = adpP;
}