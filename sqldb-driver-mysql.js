"use strict";
var mysql = require('mysql');
var util = require('util');
var stream = require('stream');
var Promisable = require('promisable');

function Driver(con) {
    this.con = con;
}
Driver.prototype = {
    disconnect: function (callback) {
        return Promisable.withCB(callback,function(R){
            this.con.end(R);
        }.bind(this));
    },
    prepare: function(sth,sql,callback) {
        return Promisable.withCB(callback,function(R) {
            sth.driver = new STH(this.con,sql);
            R.fulfill(sth);
        }.bind(this));
    }
}
function STH(con,sql) {
    this.con = con;
    this.sql = sql;
    this.params = {};
}
function typeFilter() {}

STH.prototype = {
    execute: function(sth,callback) {
        var sql = this.format(this.sql, this.params, this.escape);
        console.log(sql);
        return Promisable.withCB(callback,function(streamstart) {
            var resultset;
            var streampromise = Promisable(function(streamend) {
                resultset =
                    this.con.query(sql)
                        .stream()
                        .on('error',function(e){ streampromise.emit('error',e); streamend(e); streampromise.emit('end') })
                        .pipe(new typeFilter())
                        .on('end',function(){ try { streamend() } catch (e){} })
            }.bind(this));
            // Clone the stream on to our promise so it can be used both ways.  This actually works.
            for (var k in resultset) {
                streampromise[k] = resultset[k];
            }
            streamstart.fulfill(streampromise,resultset);
        }.bind(this));
    },
    param: function (key,value,type) {
        if (type == 'ANY') {
            if (value instanceof Buffer) {
                type = 'BINARY';
            }
        }
        this.params[key] = {type: type, value: value};
    },
    format: function (sql,params) {
        for (var k in params) {
            sql = sql.replace(':'+k,this.escape(params[k]),'g');
        }
        return sql;
    },
    escape: function (param) {
        if (typeof(param.value) == 'number' && 
                 (param.type=='DECIMAL'  || param.type=='SIGNED' ||
                  param.type=='UNSIGNED')) {
            return 'CAST('+param.value+' AS '+param.type+'  )';
        }
        else if (param.value instanceof Date && (param.type=='DATETIME' || param.type=='DATE' || param.type=='TIME')) {
            return "CAST('"+param.value.toISOString()+"' AS "+param.type+")";
        }
        var escaped = "'"+param.value.toString().replace("'","''","g")+"'";
        if (param.type == 'ANY') {
            return escaped;
        }
        else if (param.type == 'BINARY') {
            return 'BINARY '+escaped;
        }
        else {
            return 'CAST(  '+escaped+' AS '+param.type+')';
        }
    },
};

function typeFilter(options) {
    if (!options) options = {};
    options.objectMode = true;
    stream.Transform.call(this, options);
}
util.inherits(typeFilter,stream.Transform);
typeFilter.prototype._transform = function(row,encoding,done) {
    for (var name in row) {
        if (row[name] instanceof Function) { continue; }
        row[name] = this.typeMap(row[name]);
    }
    this.push(row);
    done();
}
typeFilter.prototype.typeMap = function(value) {
  if (value === null) return value;
  switch (value.type) {
    case mysql.Types.TIMESTAMP:
        value.type = 'TIMESTAMP';
        return value;
    case mysql.Types.DATETIME:
        value.type = 'DATETIME';
        return value;
    case mysql.Types.DATE:
    case mysql.Types.NEWDATE:
        value.type = 'DATE';
        return value;
    case mysql.Types.STRING:
        value.type = value instanceof Buffer ? 'BINARY' : 'CHAR';
        return value;
    case mysql.Types.VARCHAR:
    case mysql.Types.VAR_STRING:
        value.type = value instanceof Buffer ? 'VARBINARY' : 'VARCHAR';
        return value;
    case mysql.Types.TINY_BLOB:
        value.type = value instanceof Buffer ? 'TINYTEXT' : 'TINYBLOB';
        return value;
    case mysql.Types.MEDIUM_BLOB:
        value.type = value instanceof Buffer ? 'MEDIUMTEXT' : 'MEDIUMBLOB';
        return value;
    case mysql.Types.LONG_BLOB:
        value.type = value instanceof Buffer ? 'LONGTEXT' : 'LONGBLOB';
        return value;
    case mysql.Types.BLOB:
        value.type = value instanceof Buffer ? 'TEXT' : 'BLOB';
        return value;
    case mysql.Types.TINY:
        value.type = 'TINYINT';
        return value;
    case mysql.Types.SHORT:
        value.type = 'SMALLINT';
        return value;
    case mysql.Types.LONG:
        value.type = 'INT';
        return value;
    case mysql.Types.INT24:
        value.type = 'MEDIUMINT';
        return value;
    case mysql.Types.YEAR:
        value.type = 'YEAR';
        return value;
    case mysql.Types.FLOAT:
        value.type = 'FLOAT';
        return value;
    case mysql.Types.DOUBLE:
        value.type = 'DOUBLE';
        return value;
    case mysql.Types.LONGLONG:
        value.type = 'BIGINT';
        return value;
    case mysql.Types.DECIMAL:
    case mysql.Types.NEWDECIMAL:
        value.type = 'DECIMAL';
        return value;
    case mysql.Types.TIME:
        value.type = 'TIME';
        return value;
    case mysql.Types.GEOMETRY:
        value.type = 'GEOMETRY';
        return value;
    case mysql.Types.BIT:
         value.type = 'BIT';
         return value;
    case mysql.Types.ENUM:
         value.type = 'ENUM';
         return value;
    case mysql.Types.SET:
         value.type = 'SET';
         return value;
    default:
        value.type = 'UNKNOWN ('+value.type+')';
        return value;
  }
}


module.exports = {
    ident: 'mysql',
    connect: function (config, callback) {
        var con = mysql.createConnection(config);
        var result = Promisable(function(R){ con.connect(R) })
            .thenPromise(function(R) { con.query("SET NAMES 'utf8'",R) })
            .thenPromise(function(R){ R.fulfill(new Driver(con)) });
        if (callback) result(callback);
        return result;
    }
};
