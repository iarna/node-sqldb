"use strict";
var drivers = [];
var defer = process.nextTick;
var Promisable = require('promisable');

var sqldb = exports;
sqldb.register = function (driver) {
    drivers[driver.ident] = driver;
    return sqldb;
}

sqldb.connect = function (driver,config,callback) {
    var P = Promisable(function(R) {
        var drh = drivers[driver];
        if (! drh) {
            try {
                drh = require('sqldb-driver-'+driver);
            }
            catch (e1) {
                try {
                    drh = require('./sqldb-driver-'+driver);
                }
                catch (e2) {
                    return R.reject("Unknown driver "+driver+"\n"+e1+"\n"+e2);
                }
            }
            sqldb.register(drh);
        }
        drh.connect(config).then(function (drc) {
            R.fulfill(new DBH(config,drh,drc));
        });
    });
    if (callback) P(callback);
    return P;
}

var DBH = function (config,drh,drc) {
    this.config = config;
    this.drh = drh;
    this.drc = drc;
}
DBH.prototype = {
    disconnect: function (callback) {
        return Promisable.withCB(callback,function(R) {
            if (! this.drc) { R(); return; }
            this.drc.disconnect()(function(e) {
                this.drc = null;
                R(e);
            }.bind(this));
        }.bind(this));
    },
    reconnect: function (callback) {
        return Promisable.withCB(callback,function(R) {
            this.disconnect()
                .then(function() {
                    return this.drh.connect(this.config)
                }.bind(this))
                .then(function(drc) {
                    this.drc = drc;
                    R.fulfill(this);
                }.bind(this));
        }.bind(this));
    },
    prepare: function (sql,callback) {
        return this.drc.prepare(new STH(this),sql,callback);
    }
};
var STH = function (dbh) {
    this.dbh = dbh;
    this.driver = null;
}
STH.prototype = {
    param: function(key,value,type) {
        if (!type) { type = 'ANY' }
        return this.driver.param(key,value,type);
    },
    execute: function(callback) {
        return this.driver.execute(this,callback);
    }
};
