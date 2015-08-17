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
    events = require('events');
/**
 * @class DataAdapterPool
 * @constructor
 * @param {{maxConnections:number,timeout:number}=} options
 * @property {number} connections
 * @property {{maxConnections:number,timeout:number}} options
 * @augments {EventEmitter}
 */
function DataAdapterPool(options) {
    this.options = util._extend({ maxConnections:0, timeout:30000 }, options);
    this.connections = 0;
}
util.inherits(DataAdapterPool, events.EventEmitter);
/**
 *
 * @param {DataAdapter|*} adp
 * @param {function(Error=)} callback
 */
DataAdapterPool.prototype.connect = function(adp, callback) {
    var self = this;
    callback = callback || function() {};
    if (typeof adp === 'undefined' || adp == null) {
        return callback(new Error('Missing argument.Data adapter cannot be empty at this context.'));
    }
    if (typeof adp.connect !== 'function') {
        return callback(new Error('Invalid argument type. Expected DataAdapter.'));
    }
    if (self.connections < self.options.maxConnections || self.options.maxConnections == 0) {
        //call DataAdapter.open()
        adp.connect(function(err) {
            if (err) { return callback(err); }
            self.connections += 1;
        });
    }
    else {
        //register a connection pool timeout
        var timeout = setTimeout(function() {
            //throw timeout exception
            var er = new Error('Data adapter pooling timeout.');
            er.code = 'ETIMEOUT';
            callback(er);
        }, self.options.timeout);
        self.on('disconnect', function() {
            //clear timeout
            if (timeout) {
                clearTimeout(timeout);
            }
            //call DataAdapter.open()
            adp.connect(function(err) {
                if (err) { return callback(err); }
                self.connections += 1;
            });
        });
    }
};
/**
 *
 * @param {DataAdapter|*} adp
 * @param {function(Error=)} callback
 */
DataAdapterPool.prototype.disconnect = function(adp, callback) {
    var self = this;
    callback = callback || function() {};
    if (typeof adp === 'undefined' || adp == null) {
        return callback();
    }
    if (typeof adp.disconnect !== 'function') {
        return callback(new Error('Invalid argument type. Expected DataAdapter.'));
    }
    try {
        //call DataAdapter.close()
        adp.disconnect(function() {
            var listeners = self.listeners('disconnect');
            if (listeners.length>0) {
                //exit emitter
                var listener = listeners[0];
                self.removeListener('disconnect', listener);
                self.connections -= 1;
                if (typeof listener === 'function') {
                    listener.call();
                }
            }
            else {
                self.connections -= 1;
            }
            callback();
        });
    }
    catch(e) {
        self.connections -= 1;
        callback(e);
    }
};
/**
 *
 * @type {{DataAdapterPool: DataAdapterPool, pool: DataAdapterPool}}
 */
var adpP = {
    /**
     * @constructs {DataAdapterPool}
     */
    DataAdapterPool:DataAdapterPool
};
//set default adapter pool
adpP.pool = new DataAdapterPool();

if (typeof exports !== 'undefined') {
    module.exports = adpP;
}