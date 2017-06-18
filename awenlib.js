/*
 * @Description: 
 *   Mix up all useful stuff here ..., come to my own lib !
 * @Author: 
 *   wenghanyi
 */

'use strict';

// ----------
// require
// ----------
var CP = require('child_process');
var FS = require('fs');
var PH = require('path');
var URL = require('url');
var HTTP = require('http');
var QS = require('querystring');

var A = require('async');
var IL = require('iconv-lite');
var REQ = require('request');
var XML = require('xml2js');
var MYSQL = require('mysql');
var CHOKIDAR = require('chokidar');
var MKDIRP = require('mkdirp');


// ----------------------
// SIGINT, SIGTERM
// ----------------------
var cleanUpList = [];
process.on('SIGINT', function () {
    logger.warn('SIGINT captured');
    A.parallel(cleanUpList, function (err) {
        process.exit(1);
    });
});
process.on('SIGTERM', function () {
    logger.warn('SIGTERM captured');
    A.parallel(cleanUpList, function (err) {
        process.exit(1);
    });
});


// ----------
// log config
// ----------
// TODO: what if app need to require awenlib, but won't want logs out ?
// TODO: process.cwd()/logs is too urgly
//       the better way should be:
//       by default no logger out
//       exports.consoleOn = function () {};    // logger out only on stdout
//       exports.setLogFile = function () {};  // logger out to a file, need to support log split, if no path in, use process.cwd()/run.log by default
/*
 * var logPath = PH.join(process.cwd(), 'logs');
 * // TODO: existsSync is deprecated, use accessSync with try-catch instead !
 * if (!FS.existsSync(logPath)) {
 *     FS.mkdirSync(logPath);
 * }
 */

// TODO: how to split log ?
var log4js = require('log4js');
log4js.configure({});
log4js.loadAppender('file');
log4js.loadAppender('dateFile');

/*
 * log4js.configure({
 *     appenders: [
 *         { type: 'file', filename: 'logs/run.log'}
 *   ] 
 * }); 
 */
var logger = log4js.getLogger('awen');
logger.setLevel('INFO');

exports.consoleOn = function () {
    log4js.addAppender(log4js.appenders.console());
};

var logFiles = {};
exports.logFileOn = function (logName, logPath, modName, pattern) {
    logName = logName || 'run.log';
    logPath = logPath || PH.join(process.cwd(), 'logs');
    var logFullPath = PH.join(logPath, logName);
    if (logFiles.hasOwnProperty(logFullPath)) {
        return;
    }

    // TODO: mkdir can only create one level dir, mulit-level one such as 'a/b/c/d/e' will throw exception, oh my fuck !
    /*
     * try {
     *     FS.accessSync(logPath, FS.R_OK & FS.W_OK);
     * }
     * catch (e) {
     *     FS.mkdirSync(logPath);
     * }
     */
    MKDIRP.sync(logPath);

    logFiles[logFullPath] = true;
    pattern = pattern || ".yyyy-MM-dd";
    if (modName) {
        // log4js.addAppender(log4js.appenders.file(logFullPath), modName);
        log4js.addAppender(log4js.appenders.dateFile(logFullPath, pattern, false), modName);
    }
    else {
        // log4js.addAppender(log4js.appenders.file(logFullPath));
        log4js.addAppender(log4js.appenders.dateFile(logFullPath, pattern, false));
    }
};

exports.logger = function () {
    var args = Array.prototype.slice.call(arguments, 0);
    
    // log name
    var name = args.shift();
    var logger = log4js.getLogger(name);
    logger.setLevel('INFO');
    if (args.length === 0) {
        return logger;
    }

    // level
    var level = args.shift();
    if (/info/i.test(level) 
            || /debug/i.test(level) 
            || /warn/i.test(level) 
            || /error/i.test(level) 
            || /fatal/i.test(level)
            || /off/i.test(level)) {
        logger.setLevel(level);
    }
    else {
        // it is not level, put it back
        args.unshift(level);
    }

    // self log file
    if (args.length > 0) {
        // default logPath
        var logPath = PH.join(process.cwd(), 'logs');
        if (args[0] !== true) {
            logPath = PH.join.apply(PH, args);
        }
        exports.logFileOn(name + '.log', logPath, name);
    }

    return logger;
};

exports.setInternalLogLevel = function (level) {
    logger.setLevel(level || 'INFO');
};


// ----------
// Flow control part
// ----------
var INTERNAL_STOP = '__stop__';
var INTERNAL_STOP_LOOP_CONTINUE = '__stop__.loopContinue';
function isInternalStop(err) {
    if (err && typeof err.indexOf === 'function' && err.indexOf('__stop__') === 0) {
        return true;
    }
    return false;
}

var mainFlow = function () {
    var steps = [];
    var options = {};
    var retry = 0;
    Array.prototype.forEach.call(arguments, function(item) {
        if (typeof item === 'function') {
            steps.push(item);
        }
        else if (Object.prototype.toString.call(item) === '[object Object]') {
            exObj.copyProps(item, options, true);
            /*
             * // this is for web part
             * if (item.hasOwnProperty('success') && typeof item['success'] === 'function' &&
             *     item.hasOwnProperty('fail') && typeof item['fail'] === 'function') {
             *     options = {
             *         success: item['success'],
             *         fail: item['fail']
             *     };
             * }
             * // TODO: retry part not finish yet
             * // this is for retry
             * if (item.hasOwnProperty('retry') && Object.prototype.toString.call(item['retry']) === '[object Number]') {
             *     retry = item['retry'];
             * }
             */
        }
    });
    if (steps.length) {
        var start;
        steps.unshift(function(cb) {
            start = new Date();
            cb(null);
        });
        A.waterfall(steps, function() {
            var rets = Array.prototype.slice.call(arguments, 0);
            var err = rets.shift();
            var end = new Date();
            var consume = end.getTime() - start.getTime();
            logger.info('>> Totally consumed: ' + consume + '(ms)');
            if (err) {
                if (isInternalStop(err)) {
                    logger.info('>> conditinal stop in mainFlow !');
                    if (options && typeof options.success === 'function') {
                        options.success(rets);
                    }
                }
                else {
                    logger.info('>> Ooops, something wrong: ', err);
                    if (options && typeof options.fail === 'function') {
                        options.fail(err);
                    }
                }
            }
            else {
                logger.info('>> Done');
                if (options && typeof options.success === 'function') {
                    options.success(rets);
                }
            }
        });
    }
};

var newStep = function () {
    var cb = arguments[arguments.length - 1];
    cb(null);
};

var createConditionalStop = function (signal) {
    return function (fn) {
        return function () {
            var args = Array.prototype.slice.call(arguments, 0);
            var done = args.pop();
            try {
                var ret = fn.apply(null, args);
                if (ret) {
                    args.unshift(signal);
                }
                else {
                    args.unshift(null);
                }
                done.apply(null, args);
            }
            catch (e) {
                done(e);
            }
        };
    };
};

var conditionalBreak = createConditionalStop(INTERNAL_STOP);
var conditionalContinue = createConditionalStop(INTERNAL_STOP_LOOP_CONTINUE);

// [awen] Although A.asyncifyg will deal with promise object, 
//        but it will still pass through undefined/null to the next
//        i can't bear of this, 
//        besides it can't pass many results to the next
// Note:  fn must be sync func ! 
// Update: 
//        sync fn returning null probably is what author want to do, so let null pass through
//        while returning undefined is always those cases: no return inside func, therefore args in could pass through
//        Risk:  
//          I am not sure how many cases 'return null means to pass args-in out' have already in use ...
var dataPrepare = function(fn, keepArrayRet) {
    return function() {
        var args = Array.prototype.slice.call(arguments, 0);
        var cb = args.pop();
        var ret;
        try {
            ret = fn.apply(this, args);
        }
        catch(e) {
            cb(e);
            return;
        }
        // if (typeof ret === 'undefined' || ret === null) {
        if (typeof ret === 'undefined') {
            cb.apply(null, [null].concat(args));
        }
        else if (!keepArrayRet && Object.prototype.toString.call(ret) === '[object Array]') {
            cb.apply(null, [null].concat(ret));
        }
        else {
            cb(null, ret);
        }
    };
};

/*
 * var dataProcess = function(fn) {
 *     return A.asyncify(fn);
 * };
 */

var dataProcess = dataPrepare;

// TODO: UNCOMPLETE
// heavyDataProcess is different from dataPrepare/dataProcess, it focus on 'processing data' much harder
// Thus, it is better to use another process to handle this
var heavyDataProcess = function (fn) {
    // TODO: create worker.js here ?
    //       Q: how to handle require inside fn ? (specailly the require path)
    //       Q: how to handle global var inside fn ?
    var workerJs = 'var fn = ' + fn.toString() + ';'
                 + "process.on('message', function (args) {"
                 + "    try {"
                 + "         logger.info('fuck, worker start to work @' + exDate.logOutNow('yyyy-MM-dd hh:mm:ss'));"
                 + "         var ret = trans.apply(null, args);"
                 + "         logger.info('fuck, worker work done, start to send ret back @' + exDate.logOutNow('yyyy-MM-dd hh:mm:ss'));"
                 + "         process.send({ret: ret});"
                 + "    }"
                 + "    catch (e) {"
                 + "        logger.info(e);"
                 + "        process.send({err: e.toString()});"
                 + "    }"
                 + "    process.disconnect();"
                 + "    process.exit();"
                 + "});"
    return function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var next = args.pop();
        
        // TODO:
        var worker = CP.fork('a.js');
        // get the result
        worker.on('message', function (message) {
        
        });
        // worker done, call next here
        worker.on('exit', function (code) {
        
        });
        worker.on('error', function (err) {
            next(err);
        })
        // let worker run, send paramerter
        worker.send(args);
        // TODO: I met another bottleneck ...
        // sending 'args' from father process to child process spend much more time than computing in the same process when args is very large !
        // so do does sending 'ret' back !
        // Example:
        //   1. fn will consume 20s to compute xxx, (this will also block any other interrupt/callback/event)
        //   2. if put fn computing in another process by 'fork', this '20s' could be parallel running 
        //      however, the fn input & output are both very large object, sending them between father & child process will totaly consume 40s indeed !
        //      thus, the whole consumation will be 60s !
        //
        // To avoid sending args from father to child, I could create 'child' js file without input args,
        //    which means, just print all args as string into 'child' js, (also need to wrap fn)
        //    so, workerJs should be inside here !
        // Now, how to avoid sending ret back from child to father ? ...
        // Q: Is there another way to let father and child communicate ?
    };
};

var proc = function () {
    var groups = Array.prototype.slice.call(arguments, 0);
    return function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var next = args.pop();
        var fnArr = [];
        groups.forEach(function (g) {
            if (Object.prototype.toString.call(g) === '[object Array]') {
                fnArr.push(function (done) {
                    var waterFns = [];
                    g.forEach(function (f) {
                        if (typeof f === 'function') {
                            if (args.length === 1) {
                                waterFns.push(f.bind(args[0]));
                            }
                            else {
                                waterFns.push(f.bind(args));
                            }
                        }
                    });
                    if (waterFns.length > 0) {
                        waterFns.unshift(function (cb) {
                            cb.apply(null, [null].concat(args)); 
                        });
                        A.waterfall(waterFns, function () {
                            var args = Array.prototype.slice.call(arguments, 0);
                            var err = args.shift();
                            if (err) {
                                if (isInternalStop(err)) {
                                    logger.info('>> conditinal stop in proc!');
                                }
                                else {
                                    done(err);
                                    return;
                                }
                            }
                            done.apply(null, [null].concat(args));
                        });
                    }
                    else {
                        done.apply(null, [null].concat(args));
                    }
                }); 
            } 
        });

        if (fnArr.length > 0) {
            A.parallel(fnArr, function (err, result) {
                if (err) {
                    next(err);
                }
                else {
                    // result is an array !
                    next.apply(null, [null].concat(result));
                }
            });
        }
        else {
            next.apply(null, [null].concat(args));
        }
    };
};

var getObjFromOuterLevel = function (level) {
    if (!this) {
        return null;
    }
    if (!level) {
        return this;
    }
    var deepth = 0;
    var me = this;
    var sup = me.sup;
    while (sup) {
        me = sup;
        sup = me.sup;
        deepth++;
        if (deepth === level) {
            return me;
        }
    }
    return null;
};


// TODO: there is a fault: one branch of the each fail won't stop the others !
var eachProcess = function () {
    var fns = Array.prototype.slice.call(arguments, 0);

    // collection could be dynamic setted by upflow
    var collection = fns.shift();
    if (typeof collection === 'function') {
        fns.unshift(collection);
        collection = null;
    }
    
    // limit 
    var limit = fns.pop();
    if (typeof limit === 'function') {
        fns.push(limit);
        limit = 0;  
    }

    return function() {
        var args = Array.prototype.slice.call(arguments, 0);
        var next = args.pop();
        var result;
        var localCollection;

        var sup = this;

        var dynamic = false;
        if (!collection) {
            dynamic = true;
            if (args.length === 1 && Object.prototype.toString.call(args[0]) === '[object Object]') {
                localCollection = args[0];
            }
            else {
                localCollection = args;
            }
        }
        else {
            localCollection = collection;
        }

        if (Object.prototype.toString.call(localCollection) === '[object Array]') {
            result = [];
        }
        else {
            result = {};
        }
        var fnEach = function (item, key, done) {
            /*
             * var fnEachArgs = Array.prototype.slice.call(arguments, 0);
             * var done = fnEachArgs.pop();
             * var item = null;
             * var key = null;
             * if (fnEachArgs.length > 1) {
             *     item = fnEachArgs.shift();
             * }
             * if (fnEachArgs.length > 1) {
             *     key = fnEachArgs.shift();
             * }
             */
            var fnArr = [];
            fns.forEach(function (f) {
                if (typeof f === 'function') {
                    fnArr.push(f.bind({
                        key: key, 
                        item: item, 
                        sup: sup,
                        getObjFromOuterLevel: getObjFromOuterLevel
                    }));
                }
            });
            if (fnArr.length > 0) {
                fnArr.unshift(function (cb) {
                    if (dynamic) {
                        cb.apply(null, [null, item, key]);
                    }
                    else {
                        cb.apply(null, [null, item, key].concat(args));
                    }
                })
                A.waterfall(fnArr, function () {
                    var rets = Array.prototype.slice.call(arguments, 0);
                    var err = rets.shift();
                    if (err) {
                        if (isInternalStop(err)) {
                            logger.info('>> conditinal stop in each!');
                        }
                        else {
                            done(err);
                            return;
                        }
                    }
                    // since async.each can't pass result, i have to do it myself
                    result[key] = rets;
                    done(null);
                });
            }
            else {
                done(null);
            }
        };

        var nextWrap = function (err) {
            if (err) {
                next(err);   
            }
            else if (exObj.isEmpty(result)) {
                next.apply(null, [null].concat(args));
            }
            else {
                next(null, result);
            }
        }

        if (limit) {
            A.forEachOfLimit(localCollection, limit, fnEach, nextWrap);
        }
        else {
            A.forEachOf(localCollection, fnEach, nextWrap);
        }
        
    };
};

var loop = function () {
    var fns = Array.prototype.slice.call(arguments, 0);
    return function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var next = args.pop();
        var waterFns = [];
        var ignoreFail = false;
        var sleepAfterFail = 1000;
        fns.forEach(function (f) {
            if (typeof f === 'function') {
                waterFns.push(f);
            }
            else if (Object.prototype.toString.call(f) === '[object Object]') {
                if (f.ignoreFail) {
                    ignoreFail = true;
                }
                if (f.sleepAfterFail && Object.prototype.toString.call(f.sleepAfterFail) === '[object Number]') {
                    sleepAfterFail = f.sleepAfterFail;
                }
            }
        });
        if (waterFns.length > 0) {
            waterFns.unshift(function (cb) {
                cb.apply(null, [null].concat(args)); 
            });
            var results;
            A.forever(function (cb) {
                A.waterfall(waterFns, function () {
                    results = Array.prototype.slice.call(arguments, 0);
                    var err = results.shift();
                    
                    // To meet such situcation:
                    // a step fail in the loop, don't let it propagate to main, absorb internal, let the new round start !
                    if (err) {
                        if (err === INTERNAL_STOP_LOOP_CONTINUE) {
                            logger.info('>> conditinal continue in loop, new round will start after 100ms ...');
                            setTimeout(function() {
                                cb(null);
                            }, 100);
                        }
                        else if (isInternalStop(err)) {
                            logger.info('>> conditinal stop in loop!');
                            cb(err);
                        }
                        else if (ignoreFail) {
                            logger.warn('loop fail, but ignore, new round will start after ' + sleepAfterFail + 'ms ..., error: ', err);
                            setTimeout(function() {
                                cb(null);
                            }, sleepAfterFail);
                        }
                        else {
                            cb(err);
                        }
                    }
                    else {
                        cb(null);
                    }
                });
            }, function (err) {
                if (err && !isInternalStop(err)) {
                    next(err);
                }
                else {
                    next.apply(null, [null].concat(results));
                }
            })
        }
        else {
            next.apply(null, [null].concat(args));
        }
    };
};

var sleep = function (ms) {
    return function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var next = args.pop();
        if (Object.prototype.toString.call(ms) === '[object Number]') {
            logger.info('>> sleep ' + ms + 'ms ...');
            setTimeout(function () {
                logger.info('>> awake !');
                next.apply(null, [null].concat(args));
            }, ms);
        }
        else {
            next.apply(null, [null].concat(args));
        }
    };
}

exports.mainFlow = mainFlow;
exports.loop = loop;
exports.sleep = sleep;
exports.proc = proc;
exports.newStep = newStep;
exports.dataPrepare = dataPrepare;
exports.dataProcess = dataProcess;
exports.eachProcess = eachProcess;
exports.conditionalBreak = conditionalBreak;
exports.conditionalContinue = conditionalContinue;


// ----------
// Wrap async func
// ----------
var wrapInfo = function(name, fn) {
    return function() {
        logger.info('[' + name + ']'+ ': ' + JSON.stringify(Array.prototype.slice.call(arguments, 0, arguments.length - 1)));
        return fn.apply(this, arguments);
    };
};

var exe = wrapInfo('exe', function () {
    var args = Array.prototype.slice.call(arguments, 0);
    var cb = args.pop();
    var cmd = args.shift();
    var isStream = args.shift();

    var wrapCb = function (err, stdout, stderr) {
        if (err) {
            cb(err);
        }
        else if (stderr) {
            cb(stderr);
        }
        else if (stdout) {
            stdout += '';
            var cbArgs = stdout.split(/[\r\n]+/);
            if (!cbArgs[0]) {
                cbArgs.shift();
            }
            if (!cbArgs[cbArgs.length-1]) {
                cbArgs.pop();
            }
            cbArgs.unshift(null);
            cb.apply(null, cbArgs);
        }
        else {
            cb();
        }
    };

    // TODO:
    //   There is a known issue when using spawn to execute cmd:
    //      exe('cmd op1 op2 "lots of ops regarded as one op" ops4', true, function (err, ret) { ... });
    //      Since internally run like: 
    //          var cmdArr = cmd.split(/\s+/); CP.spawn(cmdArr.shift(), cmdArr);
    //      Just to split whole command by 'space' while this is fucking too simple !
    //      Using grep like: [grep "xxx yyy zzz" a.log] is very usual
    //      So does: [ssh xxx@yyy "do lots of thing here"]
    //
    //   There is a known limit usage:
    //      in spawn mode, data won't be buffed and pass to callback, only log out instead !

    var cpsn = null;
    var cmdStr = '';
    var t = Object.prototype.toString.call(cmd);
    if (t === '[object String]') {
        cmdStr = cmd;
        if (isStream) {
            var cmdArr = cmdStr.split(/\s+/);
            cpsn = CP.spawn(cmdArr.shift(), cmdArr);
        }
        else {
            CP.exec(cmdStr, wrapCb);
        }
    }
    else if (t === '[object Object]') {
        if (cmd.hasOwnProperty('cmd')) {
            cmdStr  = cmd['cmd'];
            delete cmd['cmd'];
            if (isStream) {
                var cmdArr = cmdStr.split(/\s+/);
                cpsn = CP.spawn(cmdArr.shift(), cmdArr, cmd);
            }
            else {
                CP.exec(cmdStr, cmd, wrapCb);
            }
        }
        else {
            cb('cmd is obj mode, but can not find command!');
        }
    }
    else {
        cb('cmd type wrong, not string or obj !');
    }

    // Notice: This is for spawn mode, as described above, 2 known issue in it !
    if (isStream && cpsn) {
        cpsn.on('error', function (err) {
            logger.error(cmdStr + ', error >> ', err);
            wrapCb(err);
        });
        
        var hasError = false;
        cpsn.stderr.on('data', function (data) {
            logger.error(cmdStr + ', stderr >> ' + data);
            hasError = true;
        });
        
        // Notice: in stream mode, data won't be passed to cb, just log out
        cpsn.stdout.on('data', function (data) {
            logger.info(cmdStr + ', stdout >> ' + data);
        });
    
        cpsn.on('close', function (code) {
            if (hasError) {
                wrapCb('stderr got output, regarded as error', code);
            }
            else {
                wrapCb(null, code);
            }
        });
    }
});

var req = wrapInfo('req', function (op, cb) {
    var logHead = '[req-' + tools.genRandomPattern(6) + ']: ';
    var start = new Date();
    logger.info(logHead + 'req go');
    return REQ(op, function (err, resp, body) {
        if (err) {
            cb(err);
        }
        else {
            if (resp.statusCode !== 200) {
                cb('response code is not 200, act[' + resp.statusCode + ']');
            }
            else {
                var end = new Date();
                logger.info(logHead + 'resp back, ' + (end.getTime() - start.getTime()) + 'ms');
                cb(null, body);
            }
        }
    });
});

exports.exe = exe;
exports.req = req;


// ----------------------------------------
// Simple flow controller:
//   not use aysnc
//   won't pass result from step to step
// 
// Demo:
//   
//   var flowController = createSimpleFlowController();
//   var asyncStepA = flowController.createStep(asyncFnA, args1, args2, cbA);
//   var asyncStepB = flowController.createStep(asyncFnB, args1, args2, args3, cbB);
//   var asyncStepC = flowController.createStep(asyncFnC, null);
//   var asyncStepD = flowController.createStep(asyncFnD, args1, cbD);
//   var asyncStepE = flowController.createStep(asyncFnE, cbE);
//   var asyncStepF = flowController.createStep(asyncFnF, args1, args2, args3, args4, null);
//   
//   asyncStepA.next(asyncStepB);
//   asyncStepA.next(asyncStepC);
//   asyncStepB.next(asyncStepF);
//   asyncStepC.next(asyncStepE);
//   asyncStepD.next(asyncStepC);
//
//            start                A.n: B, C
//             / \                 A.b: 
//            A   D                B.n: F
//           / \ /                 B.b: A
//          B   C                  C.n: E
//          |   |                  C.b: A, D
//          F   E                  D.n: C
//          \   /                  D.b:
//           end                   E.n:
//                                 E.b: C
//                                 F.n:
//                                 F.b: B
//
//   flowController.run(function (err) {
//
//   });
//
//   Note:
//      1. all cb func shouldn't contain another async func -- that is simpleFlowController's job
//      2. simpleFlowController take check for valid DAG
//      3. waterfall way is an ususal situcation, e.g. A output a -> B input a, output b
//          however, for SimpleFlowController, args from A to B, must be obj, otherwise can't pass through,
//
//              var outA = {};
//              var stepA = flowController.createStep(asyncA, function (err, out) {
//                  // outA = out;  --> this is wrong !
//                  exObj.copyProps(out, outA, true);
//              });
//              var outB = {};
//              var stepB = flowController.createStep(asyncB, outA, function (err, out) {
//                  // outB = out;  --> this is wrong !
//                  exObj.copyProps(out, outB, true);
//              });
//              stepA.next(stepB);
//              flowController.run(function (err) {
//                  logger.info(outB);
//              });
//          here, outA, outB should be Object, otherwise like (number, string, boolean), simpleFlowController won't work !
// ----------------------------------------
exports.createSimpleFlowController = (function () {
    function SimpleFlowController() {
        this.flow = {};
        this.err = null;
        this.stop = false;
        this.startPointSign = null;
        this.endPointSign = null;
        this.cnt = 0;
    }

    SimpleFlowController.prototype.createStep = function () {
        var me = this;
        var sign = me.cnt + '' + tools.genRandomPattern(10);
        me.cnt++;
        
        var args = Array.prototype.slice.call(arguments, 0);
        var asyncFn = args.shift();
        // wrap cb:
        var cb = args.pop();
        args.push(function (err) {
            if (err) {
                me.err = err;
            }
            var goOn = true;
            if (typeof cb === 'function') {
                goOn = cb.apply(null, arguments);
            }
            if (me.flow.hasOwnProperty(sign)) {
                me.flow[sign].done = true;
                if (goOn || (typeof goOn === 'undefined')) {
                    // if original cb return undefined or anything regarded as true, go on
                    me.flow[sign].after.forEach(function (s) {
                        me.flow[s].fn();
                    });
                }
                else {
                    // else, stop the whole run !
                    me.stop = true;
                    me.flow[me.endPointSign].fn();
                }
            }
        });

        me.flow[sign] = {
            fn: function () {
                var allBeforeDone = true;
                me.flow[sign].before.forEach(function (s) {
                    allBeforeDone &= me.flow[s].done;
                });

                if ((!me.flow[sign].done) && 
                        (((!me.stop) && allBeforeDone) || 
                        (me.stop && sign === me.endPointSign))) {
                    asyncFn.apply(null, args);
                }
            },
            before: [],
            after: [],
            done: false
        };
        
        return {
            next: function (nextStep) {
                var obj = this;
                if (Object.prototype.toString.call(nextStep) === '[object Array]') {
                    nextStep.forEach(function (n) {
                        obj.next(n);
                    });
                }
                else if (nextStep && nextStep.sign && me.flow.hasOwnProperty(nextStep.sign)) {
                    if (!exArray.contain(me.flow[sign].after, nextStep.sign)) {
                        me.flow[sign].after.push(nextStep.sign);
                    }
                    if (!exArray.contain(me.flow[nextStep.sign].before, sign)) {
                        me.flow[nextStep.sign].before.push(sign);
                    }
                }
            },
            sign: sign
        };
    };

    SimpleFlowController.prototype.removeStep = function (step) {
        var me = this;
        if (step && step.sign && me.flow.hasOwnProperty(step.sign)) {
            delete me.flow[step.sign];
            exObj.each(me.flow, function (obj) {
                exArray.remove(obj.before, step.sign, 2);
                exArray.remove(obj.after, step.sign, 2);
            });
        }
    };

    SimpleFlowController.prototype.run = function (done, timeout) {
        var me = this;
        
        // TODO:
        // check loop inside DAG !
        
        // to support multi-times run
        me.err = null;
        me.stop = false;
        if (me.startPointSign) {
            delete me.flow[me.startPointSign];
        }
        if (me.endPointSign) {
            delete me.flow[me.endPointSign];
        }
        exObj.each(me.flow, function (obj) {
            if (me.startPointSign) {
                exArray.remove(obj.before, me.startPointSign, 2);
                exArray.remove(obj.after, me.startPointSign, 2);
            }
            if (me.endPointSign) {
                exArray.remove(obj.before, me.endPointSign, 2);
                exArray.remove(obj.after, me.endPointSign, 2);
            }
            obj.done = false;
        });
        me.startPointSign = null;
        me.endPointSign = null;

        var dummyFn = function (cb) {
            cb();
        };

        var t = null;
        var timeout = timeout || 600000;
        if (typeof done === 'function') {
            t = setTimeout(function () {
                done('timetout after ' + timeout + 'ms, before all steps done ! err = ' + me.err);
            }, timeout);
        }
        var doneWrap = function () {
            if (t) {
                clearTimeout(t);
                t = null;
                done(me.err);
            }
        };

        var startPoint = this.createStep(dummyFn, null);
        me.startPointSign = startPoint.sign;
        // logger.debug('startPointSign = ' + startPoint.sign);

        var endPoint = this.createStep(dummyFn, doneWrap);
        me.endPointSign = endPoint.sign;
        // logger.debug('endPointSign = ' + endPoint.sign);

        exObj.each(me.flow, function (obj, sign) {
            if (obj.before.length === 0 && sign !== startPoint.sign) {
                startPoint.next({sign: sign});
            }
            if (obj.after.length === 0 && sign != endPoint.sign) {
                obj.after.push(endPoint.sign);
                me.flow[endPoint.sign].before.push(sign);
            }
        });

        // logger.debug('flow = ' + JSON.stringify(me.flow, null, 2));
        me.flow[startPoint.sign].fn();
    };

    return function () {
        return new SimpleFlowController();
    };
}) ();

// awenlib always call func like this:  func.apply(null, arguments), this won't work well for obj's method, thus use this func to trans
exports.objMethodToGlobalFun = function (obj, methodName) {
    return function () {
        if (Object.prototype.toString.call(obj) === '[object Object]' && typeof obj[methodName] === 'function') {
            obj[methodName].apply(obj, arguments);
        }
    };
}

// ----------
// Xml
// ----------
var parseXml = wrapInfo('parseXml', function () {
    var xmlString = '';
    var args = Array.prototype.slice.call(arguments, 0);
    var cb = args.pop();
    args.forEach(function (a) {
        if (Object.prototype.toString.call(a) === '[object String]') {
            xmlString += a;
        }
    });
    XML.parseString(xmlString, {
        trim: true,
        explicitArray: false
    }, cb);
});

var createXmlTextProcessStream = function(textProcess) {
    var textProcess = textProcess || {};
    if (Object.prototype.toString.call(textProcess) !== '[object Object]') {
        textProcess = {};
    }

    var innerTextProcess = [];
    for (var prop in textProcess) {
        if (textProcess.hasOwnProperty(prop)) {
            if (typeof textProcess[prop] !== 'function') {
                return;
            }

            innerTextProcess.push({
                'searchPath': prop.split('.'),
                'process': textProcess[prop],
                'found': []
            });
        }
    }

    var indent = {
        tabs: 4,
        index: 0,
        inc: function() { this.index++; },
        dec: function() { 
            if (this.index > 0) {
                this.index--;
            }
            else {
                this.index = 0;
            }
        },
        print: function() {
            var space = '';
            for (var i=0; i<this.index; i++) {
                for (var j=0; j<this.tabs; j++) {
                    space += ' ';
                }
            }
            return space;
        }
    };

    var hasText = false;
    var saxStream = require('sax').createStream(true, {trim: true, normalize: true});
    saxStream.on('opentag', function (tag) {
        innerTextProcess.forEach(function(item) {
            if (indent.index === item.found.length && indent.index < item.searchPath.length) {
                if ( ((indent.index >= 1 && item.found[indent.index - 1]) || indent.index == 0)
                 && (tag.name === item.searchPath[indent.index])) {
                    item.found.push(true);
                }
                else {
                    item.found.push(false);
                }
            }
        });
    
        var data = '\n' + indent.print() + '<' + tag.name;
        for (var prop in tag.attributes) {
            if (tag.attributes.hasOwnProperty(prop)) {
                data += ' ' + prop + '="' + tag.attributes[prop] + '"';
            }
        }
        data += '>';
        indent.inc();
        hasText = false;
        this.emit('data', data);
    });
    
    saxStream.on('closetag', function (tag) {
        indent.dec();
        innerTextProcess.forEach(function(item) {
            if (indent.index === item.found.length -  1 && indent.index < item.searchPath.length) {
                item.found.pop();
            }
        });

        var data = '</' + tag + '>';
        if (!hasText) {
            data = '\n' + indent.print() + data;
        }
        hasText = false;
        this.emit('data', data);
    });
    
    saxStream.on('text', function(text) {
        innerTextProcess.forEach(function(item) {
            var l1 = item.searchPath.length;
            var l2 = item.found.length;
            if ( l1 === l2 && item.found[l2 - 1]) {
                text = item.process.call(null, text) || text;
            }
        });

        hasText = true;
        this.emit('data', text);
    });
    saxStream.on('cdata', function (cdata) {
        indent.inc();
        var data = '\n' + indent.print() + '<![CDATA[' + cdata + ']]>';
        this.emit('data', data);
    });
    saxStream.on('comment', function (comment) {
        var data = '\n' + indent.print() + '<!-- ' + comment + ' -->';
        this.emit('data', data);
    });
    saxStream.on("error", function (error) {
        throw error;
    });
    
    // override write func, do not emit data at once !
    saxStream.write = function (data) {
        if (typeof Buffer === 'function' 
            && typeof Buffer.isBuffer === 'function' 
            && Buffer.isBuffer(data)) {
            if (!this._decoder) {
                var SD = require('string_decoder').StringDecoder
                this._decoder = new SD('utf8')
            }
            data = this._decoder.write(data);
        }
    
        this._parser.write(data.toString());
        return true;
    };

    return saxStream;
};

var processXmlByStream = function (xmlIn, xmlOut, seds, cb) {
    var xmlProcess = createXmlTextProcessStream(seds);
    xmlIn.pipe(xmlProcess).pipe(xmlOut);

    xmlIn.on('error',      function(e) { cb(e); });
    xmlOut.on('error',     function(e) { cb(e); });
    xmlProcess.on('error', function(e) { cb(e); });
    xmlOut.on('finish',    function()  { cb(null, xmlOut, xmlIn); });
};

var processRemoteXml = wrapInfo('processRemoteXml', function (reqOp, xmlOut, seds, cb) {
    var xmlIn  = REQ(reqOp);
    var xmlOut = FS.createWriteStream(xmlOut);
    processXmlByStream(xmlIn, xmlOut, seds, cb);
});

var processXmlFile = wrapInfo('processXmlFile', function (xmlIn, xmlOut, seds, cb) {
    var xmlIn  = FS.createReadStream(xmlIn, {encoding: 'utf8'});
    var xmlOut = FS.createWriteStream(xmlOut);
    processXmlByStream(xmlIn, xmlOut, seds, cb);
});

exports.parseXml = parseXml;
exports.processRemoteXml = processRemoteXml;
exports.processXmlFile = processXmlFile;


// ---------------
// read file
// ---------------
var readFileByLine = wrapInfo('readFileByLine', function(file, encoding, fn, cb) {
    if (typeof encoding !== 'string') {
        cb('readFileByLine need encoding as the 2nd param');
        return;
    }
    if (typeof fn !== 'function') {
        cb('readFileByLine need function as the 3rd param');
        return;
    }
    //var fin = FS.createReadStream(file, {encoding: 'utf8'});
    var fin = FS.createReadStream(file);
    var decodeStream = IL.decodeStream(encoding);
    fin.pipe(decodeStream);

    var uncompleteLine = '';
    var lineNum = 0;
    var ret = {};
    
    function handleData(data) {
        decodeStream.pause();
        var lines = data.split('\n');
        if (lines.length > 0) {
            if (uncompleteLine) {
                lines[0] = uncompleteLine + lines[0];
            }
            uncompleteLine = lines.pop();
            lines.forEach(function(line) {
                fn.call(null, line, lineNum, ret);
                lineNum++;
            });
        }
        decodeStream.resume();
    }

    function handleEnd() {
        if (uncompleteLine) {
            fn.call(null, uncompleteLine, lineNum, ret);
        }
        cb(null, ret);
    }

    function handleErr(err) {
        cb(err);
    }

    decodeStream.on('data', handleData);
    decodeStream.on('end', handleEnd);
    decodeStream.on('error', handleErr);
    
    //fin.on('data', handleData);
    //fin.on('end', handleEnd);
    fin.on('error', handleErr);
});
exports.readFileByLine = readFileByLine;


// ----------
// extend js
// ----------
var exDate = {
    format: function(date, fmt) {
        var o = {
            'M+': date.getMonth() + 1,
            'd+': date.getDate(),
            'h+': date.getHours(),
            'm+': date.getMinutes(),
            's+': date.getSeconds(),
            'q+': Math.floor((date.getMonth()+3)/3),
            'S': date.getMilliseconds()
        };
        if(/(y+)/.test(fmt))
            fmt = fmt.replace(RegExp.$1, (date.getFullYear()+'').substr(4 - RegExp.$1.length));
        for (var k in o) {
            if (new RegExp('('+ k +')').test(fmt)) {
                fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (('00' + o[k]).substr(('' + o[k]).length)));
            }
        }
        return fmt;
    },
    logOutNow: function(fmt) {
        return this.format(new Date(), fmt);
    }
}

var exMath = {
    // output: [min, max]
    randInt: function(min, max) {
        var range = max - min;   
        var rand = Math.random();   
        return (min + Math.round(rand * range)); 
    }
};

var exArray = {
    unique: function(arr, equalFn, initFn, mergeFn, replaceEvenUnique) {
        if (Object.prototype.toString.call(arr) === '[object Array]') {
            var len = arr.length;
            for (var i = 0; i < len; i++) {
                (function (i) {
                    var merge;
                    if (initFn && typeof initFn === 'function') {
                        merge = initFn(arr[i]);
                    }
                    else {
                        merge = arr[i];
                    }
                    var find = false;
                    for (var j = i + 1; j < len; j++) {
                        var equal = false;
                        if (equalFn && typeof equalFn === 'function') {
                            if (equalFn(arr[i], arr[j])) {
                                equal = true;
                            }
                        }
                        else if (arr[i] === arr[j]) {
                            equal = true;
                        }
                        if (equal) {
                            if (mergeFn && typeof mergeFn === 'function') {
                                if (typeof merge === 'object') {
                                    mergeFn(merge, arr[j]);
                                }
                                else {
                                    merge = mergeFn(merge, arr[j]);
                                }
                            }
                            arr.splice(j, 1);
                            j--;
                            len--;
                            find = true;
                        }
                    }
                    if (find || replaceEvenUnique) {
                        arr[i] = merge;
                    }
                }) (i);
            }
        }
    },
    findObjItemByKey: function(arr, key, exp) {
        if (Object.prototype.toString.call(arr) !== '[object Array]') {
            return null;
        }
        for (var i = 0, l = arr.length; i < l; i++) {
            var obj = arr[i];
            if (typeof obj === 'object' && obj.hasOwnProperty(key)) {
                if (obj[key] === exp) {
                    return [obj, i];
                }
            }
        }
        return null;
    },
    compare: function (a, b, fn) {
        if (Object.prototype.toString.call(a) !== '[object Array]') {
            return false;
        }
        if (Object.prototype.toString.call(b) !== '[object Array]') {
            return false;
        }

        var al = a.length;
        var bl = b.length;
        if (al !== bl) {
            return false;
        }

        var i = 0;
        var j = 0;
        var found = false;

        for (i = 0; i < al; i++) {
            found = false;
            for (j = 0; j < bl; j++) {
                if (typeof fn === 'function') {
                    if (fn.call(null, a[i], b[j])) {
                        found = true;
                        break;
                    }
                }
                else if (a[i] === b[j]) {
                    found  = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        for (j = 0; j < bl; j++) {
            found = false;
            for (i = 0; i < al; i++) {
                if (typeof fn === 'function') {
                    if (fn.call(null, a[i], b[j])) {
                        found = true;
                        break;
                    }
                }
                else if (a[i] === b[j]) {
                    found  = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    },
    contain: function (arr, ele, fn) {
        if (Object.prototype.toString.call(arr) !== '[object Array]') {
            return false;
        }
    
        return arr.some(function (x) {
            if (typeof fn === 'function') {
                return fn.call(null, x, ele);
            }
            else {
                return x === ele;
            }
        });
    },
    indexOf: function (arr, ele, fn) {
        var index = -1;
        if (Object.prototype.toString.call(arr) !== '[object Array]') {
            return index;
        }

        arr.some(function (x, i) {
            var find = false;
            if (typeof fn === 'function') {
                find = fn.call(null, x, ele);
            }
            else {
                find = (x === ele);
            }

            if (find) {
                index = i;
                return true; 
            }
            return false;
        });

        return index;
    },
    intersect: function (a, b, fn) {
        var me = this;
        if (Object.prototype.toString.call(a) !== '[object Array]') {
            return null;
        }
        if (Object.prototype.toString.call(b) !== '[object Array]') {
            return null;
        }
        
        var ret = [];
        a.forEach(function (x) {
            if (me.contain(b, x, fn)) {
                ret.push(x);
            }
        });
        return ret;
    },
    /*
     * removeType:
     *   0 -- remove first met
     *   1 -- remove last met
     *   2 -- remove all
     *   other -- same as 0
     */
    remove: function (arr, ele, removeType, fn) {
        if (Object.prototype.toString.call(arr) !== '[object Array]') {
            return;
        }
        if (typeof removeType === 'function') {
            fn = removeType;
            removeType = 0;
        }
        removeType = removeType || 0;
        var indexs = [];
        arr.forEach(function (x, i) {
            var find = false;
            if (typeof fn === 'function') {
                find = fn.call(null, x, ele);
            }
            else {
                find = (x === ele);
            }
            if (find) {
                switch (removeType) {
                    case 0:
                        if (indexs.length === 0) {
                            indexs[0] = i;
                        }
                        break;
                    case 1:
                        indexs[0] = i;
                        break;
                    case 2:
                    default:
                        indexs.push(i);
                }
            }
        });
        var offset = 0;
        indexs.forEach(function (index) {
            arr.splice(index - offset, 1);
            offset++;
        });
    }
};

function traceDifferPath(arr, val) {
    if (arr && Object.prototype.toString.call(arr) === '[object Array]') {
        arr.unshift(val);
    }
}

function flatten(obj) {
    var flattens = [];
    var type = Object.prototype.toString.call(obj);
    if (type === '[object Object]' || type === '[object Array]') {
        exObj.each(obj, function (v, k) {
            var arr = flatten(v);
            arr.forEach(function (item) {
                if (item.path !== null) {
                    item.path = k + '.' + item.path;
                }
                else {
                    item.path = k;
                }
            });
            flattens = flattens.concat(arr);
        });
    }
    else {
        flattens.push({
            path: null,
            value: obj
        });
    }
    return flattens;
}

var exObj = {
    isEmpty: function (obj) {
        if (!obj) {
            return true;
        }
        if (typeof obj.isEmpty === 'function') {
            return obj.isEmpty();
        }
        if (Object.prototype.toString.call(obj) === '[object Array]') {
            return obj.length === 0;
        }
        if (Object.prototype.toString.call(obj) === '[object Object]') {
            var empty = true;
            for (var prop in obj) {
                if (obj.hasOwnProperty(prop)) {
                    empty = false;
                    break;
                }
            }
            return empty;
        }
        return true;      
    },
    each: function (obj, fn) {
        if (!obj) {
            return;
        }
        if (Object.prototype.toString.call(obj) === '[object Array]') {
            obj.forEach(fn);
        }
        if (Object.prototype.toString.call(obj) === '[object Object]') {
            for (var prop in obj) {
                if (obj.hasOwnProperty(prop)) {
                    if (fn(obj[prop], prop) === false) {
                        break;
                    }
                }
            }
        }
    },
    isEqual: function (a, b, crumbs) {
        if (a === b) {
            return true;
        }
        var typeA = Object.prototype.toString.call(a);
        var typeB = Object.prototype.toString.call(b);
        if (typeA === typeB) {
            switch (typeA) {
                case '[object Object]':
                    for (var p in a) {
                        if (a.hasOwnProperty(p)) {
                            if (b.hasOwnProperty(p)) {
                                if (!this.isEqual(a[p], b[p], crumbs)) {
                                    traceDifferPath(crumbs, p);
                                    return false;
                                }
                            }
                            else {
                                traceDifferPath(crumbs, p);
                                return false;
                            }
                        }
                    }
                    for (var p in b) {
                        if (b.hasOwnProperty(p)) {
                            if (!a.hasOwnProperty(p)) {
                                traceDifferPath(crumbs, p);
                                return false;
                            }
                        }
                    }
                    break;
                case '[object Array]':
                    var lengthA = a.length;
                    var lengthB = b.length;
                    if (lengthA === lengthB) {
                        for (var i = 0; i < lengthA; i++) {
                            if (!this.isEqual(a[i], b[i], crumbs)) {
                                traceDifferPath(crumbs, i);
                                return false;
                            }
                        }   
                    }
                    else {
                        return false;
                    }
                    break;
                case '[object String]':
                case '[object Number]':
                case '[object Boolean]':
                    return a === b;
                default: 
                    // i don't compare others !
                    return true;
            }
        }
        else {
            return false;
        }
        return true;
    },
    copyProps: function (a, b, canOverride) {
        if (Object.prototype.toString.call(a) !== '[object Object]') {
            return;
        }
        if (Object.prototype.toString.call(b) !== '[object Object]') {
            return;
        }
        for (var prop in a) { 
            if (a.hasOwnProperty(prop)) {
                if (!b.hasOwnProperty(prop) || canOverride) {
                    b[prop] = a[prop];
                }
            }
        }
    },
    flatten: flatten
};

function isTypeFactory(type) {
    return function (target) {
        return Object.prototype.toString.call(target) === '[object ' + type + ']';
    };
}

// use json stringify & parse to do deep copy
function deepCopy(a) {
    if (typeof a === 'undefined') {
        return;
    }
    return JSON.parse(JSON.stringify(a));
}

var ex = {
    isObject: isTypeFactory('Object'),
    isArray: isTypeFactory('Array'),
    isNumber: isTypeFactory('Number'),
    isString: isTypeFactory('String'),
    isFunction: isTypeFactory('Function'),

    isValid: function (v) {
        return typeof v !== 'undefined' && v !== null;
    },
    
    deepCopy: deepCopy
}

exports.exArray = exArray;
exports.exMath = exMath;
exports.exDate = exDate;
exports.exObj = exObj;
exports.ex = ex;

// ----------------
// misc tools
// ----------------
var tools = {
    genRandomPattern: (function () {
        var candidates = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        var canlen = candidates.length;
        return function (length) {
            var pattern = '';
            for (var i = 0; i < length; i++) {
                pattern += candidates[exMath.randInt(0, canlen - 1)];
            }
            return pattern;
        };
    }) (),

    iconv: function (src, f, t) {
        return IL.decode(IL.encode(src, f), t);
    }
};
exports.tools = tools;


// ----------
// Mysql pool
// ----------
var mysqlTimeout = 60000;
exports.createDbPool = function (host, port, database, user, password, poolSize) {
    if (!host || !port || !user || !password || !database) {
        return null;
    }
    poolSize = poolSize || 10;
    var pool = MYSQL.createPool({
        connectionLimit: poolSize,
        connectTimeout: mysqlTimeout,
        host: host,
        port: port,
        user: user,
        password: password,
        database: database,
        dateStrings: true,
        supportBigNumbers: true,
        typeCast: function (field, next) {
            if (field.type === 'BIT') {
                var bit = field.buffer();
                if (bit) {
                    return bit[0] === 1;
                }
                return false;
            }
            return next();
        }
    });

    cleanUpList.push(function (next) {
        pool.end(function (err) {
            if (err) {
                logger.error(err);
                next(err);
            }
            else {
                logger.info('pool closed');
                next();
            }
        });
    });

    return pool;
};

// ----------
// Light Dao/Entity
// ----------
exports.daoFactory = (function () {
    function toCamel(name) {
        return name.replace(/_(\w)/g, function (m) {
            if (m && m.length === 2) {
                return m[1].toUpperCase();
            }
            return '';
        });
    }

    function toUnderLine(name) {
        return name.replace(/[A-Z]/g, function (m) {
            if (m) {
                return '_' + m.toLowerCase();
            }
            else {
                return '';
            }
        });
    }

    /*
     * defs = {
     *      string: [],
     *      number: [],
     *      json: []
     * }
     */
    function fieldsSetup(defs, fields) {
        if (defs && Object.prototype.toString.call(defs) === '[object Object]') {
            exObj.each(defs, function (list, type) {
                if (list && Object.prototype.toString.call(list) === '[object Array]') {
                    list.forEach(function (f) {
                        var underline = toUnderLine(f);
                        fields[underline] = {
                            c: f,
                            t: type
                        };
                    });
                }
            });
        }
    }

    function db2JsByType(value, type) {
        if (value === null || typeof value === 'undefined') {
            // return defaultValue(type);
            return null;
        }
        switch (type) {
            case 'bool': return value;
            case 'number': return parseInt(value, 10);
            case 'string': return '' + value;
            case 'json': 
                try {
                    return JSON.parse(value);
                }
                catch (e) {
                    return {};
                }
            default: return value;
        }
    }

    function js2DbByType(value, type) {
        switch (type) {
            case 'bool': return value ? 1 : 0;
            case 'number': return '' + value;
            case 'string': return value;
            case 'json':  return JSON.stringify(value);
            default: return value;
        }
    }

    /*
     * function defaultValue(type) {
     *     switch (type) {
     *         case 'number': return 0;
     *         case 'string': return '';
     *         case 'json': return '{}';
     *         default: return null;
     *     }
     * }
     */

    return function (pool, name, defs, commColsOverride) {
        var commonColsMapping = {
            id: 'id',
            version: 'version',
            create: 'createTime',
            update: 'updateTime'
        };
        exObj.copyProps(commColsOverride, commonColsMapping, true);       

        var commonCols = {number: [], string: []};
        if (commonColsMapping.id) {
            commonCols.number.push(commonColsMapping.id);
        }
        if (commonColsMapping.version) {
            commonCols.number.push(commonColsMapping.version);
        }
        if (commonColsMapping.create) {
            commonCols.string.push(commonColsMapping.create);
        }
        if (commonColsMapping.update) {
            commonCols.string.push(commonColsMapping.update);
        }

        function commonColsWrap(entity, op, oldVersion) {
            if (!entity) {
                return null;
            }

            if (op === 'update' || op === 'insert') {
                var now = exDate.format(new Date(), 'yyyy-MM-dd hh:mm:ss');
                var nowEsc = MYSQL.escape(now);
                if (commonColsMapping.version) {
                    entity[commonColsMapping.version] = 0;
                }
                if (commonColsMapping.update) {
                    entity[commonColsMapping.update] = now;
                }

                if (op === 'update') {
                    if (oldVersion !== null && typeof oldVersion !== 'undefined' 
                            && Object.prototype.toString.call(oldVersion) === '[object Number]' && commonColsMapping.version) {
                        entity[commonColsMapping.version] = oldVersion + 1;
                    }
                    var updateStatements = [];
                    if (commonColsMapping.version) {
                        updateStatements.push(MYSQL.escapeId(toUnderLine(commonColsMapping.version)) + '=' + entity[commonColsMapping.version]);
                    }
                    if (commonColsMapping.update) {
                        updateStatements.push(MYSQL.escapeId(toUnderLine(commonColsMapping.update)) + '=' + nowEsc);
                    }
                    return updateStatements;
                }
                else if (op === 'insert') {
                    if (commonColsMapping.create) {
                        entity[commonColsMapping.create] = now;
                    }
                    var insertCols = [];
                    var insertValues = [];
                    if (commonColsMapping.version) {
                        insertCols.push(MYSQL.escapeId(toUnderLine(commonColsMapping.version)));
                        insertValues.push(entity[commonColsMapping.version]);
                    }
                    if (commonColsMapping.create) {
                        insertCols.push(MYSQL.escapeId(toUnderLine(commonColsMapping.create)));
                        insertValues.push(nowEsc);
                    }
                    if (commonColsMapping.update) {
                        insertCols.push(MYSQL.escapeId(toUnderLine(commonColsMapping.update)));
                        insertValues.push(nowEsc);
                    }
                    return [insertCols, insertValues];
                }
            }
            return null;
        }

        var regexp = new RegExp(name + '\\.(\\w+)');
        var u2cAllFields = {};
        fieldsSetup(commonCols, u2cAllFields);
        fieldsSetup(defs, u2cAllFields);
        var u2cFields = {};
        fieldsSetup(defs, u2cFields);

        function criteriaValue(key, value) {
            var c = {};
            c[key] = value;
            var defaultCr = MYSQL.escape(c);

            if (ex.isArray(value) && value.length > 0) {
                return MYSQL.escapeId(key) + ' IN (' + value.map(function (va) { return MYSQL.escape(va); }).join(',') + ')';
            }
            else if (ex.isString(value) && /^LIKE /) {
                return MYSQL.escapeId(key) + ' LIKE ' + MYSQL.escape(value.replace(/^LIKE /, ''));
            }

            return defaultCr;
        }

        return {
            createEmptyEntity: function () {
                var entity = {};
                exObj.each(u2cAllFields, function (item) {
                    entity[item.c] = null;
                });
                return entity;
            },
            
            getEntityById: function (id, cb) {
                if (id === null || typeof id === 'undefined') {
                    cb(null, null);
                }
                else {
                    var criteria = {};
                    var primaryKey = commonColsMapping.id ? commonColsMapping.id : 'id';
                    criteria[primaryKey] = id;
                    var entity = null;
                    this.getEntityByCriteria(criteria, function (en) {
                        // this is to get by id, should only be one row !
                        if (!entity) {
                            entity = en;
                        }
                    }, function (err) {
                        if (err) {
                            cb(err);
                        }
                        else if (!entity) {
                            cb(null, null);
                        }
                        else {
                            cb(null, entity);
                        }
                    });
                }
            },

            /*
             * @parallSize: 
             *     parallSize = 3:
             *     row 1 ---\   :
             *              | ..:... process1 --> \
             *     row 2 <--/   :                 |
             *       .          :                 |
             *     row 2 ---\   :                 |
             *              | ..:... process2 --> |
             *     row 3 <--/   :                 |
             *       .          :                 |
             *     row 3 ---\   :                 |
             *              | ..:... process3 --> |
             *                  :                 |
             *     row 4 <------------------------/      
             */
            // TODO: to support tables join select !
            /*
             * criteria: {
             *    columnCamel-A: 'abc',
             *    columnCamel-B: 100,
             *    columnCamel-C: [1,2,3,4]  // trans to in
             *    colunmCamel-D: 'LIKE xxx' // trans to like
             *    colunmCamel-F: {
             *        distinct: true,   // default false
             *        orderBy: 'desc'     // desc/asc
             *        value: [1,2,3,4]  // just like colunmCamel-[A,B,C,D]
             *    }
             *    __LIMIT__: 100
             * }
             *
             */
            getEntityByCriteria: function () {
                var args = Array.prototype.slice.call(arguments, 0);
                var cb = args.pop();
                var criteria = args.shift();
                var processRow = args.shift();
                var parallSize = args.shift() || 0;

                var criteriaArr = [];
                var orderByArr = [];
                var distinctCol = '';
                var limit = 10000;
                exObj.each(criteria, function (v, k) {
                    if (k === '__LIMIT__' && ex.isNumber(v)) {
                        limit = v;
                        return;
                    }

                    var key = toUnderLine(k);
                    var cr = null;

                    if (ex.isObject(v)) {
                        if (ex.isValid(v.value)) {
                            cr = criteriaValue(key, v.value);   
                        }

                        if (v.distinct) {
                            distinctCol = name + '.' + key;
                        }

                        if (v.orderBy === 'desc') {
                            orderByArr.push(MYSQL.escapeId(key) + ' DESC ');
                        }
                        else if (v.orderBy === 'asc') {
                            orderByArr.push(MYSQL.escapeId(key) + ' ASC ');
                        }
                    }
                    else {
                        cr = criteriaValue(key, v);
                    }

                    if (cr) {
                        criteriaArr.push(cr);
                    }
                });

                var cols = Object.keys(u2cAllFields).map(function (u) {
                    return name + '.' + u;
                }).join(',');
                
                if (distinctCol) {
                    cols = 'DISTINCT ' + distinctCol + ', ' + cols;
                }

                var ops = {
                    sql: 'SELECT ' + cols + ' FROM ' + name,
                    nestTables: '.'
                };

                if (criteriaArr.length > 0) {
                    ops.sql += ' WHERE ' + criteriaArr.join(' AND ');
                }

                if (orderByArr.length > 0) {
                    ops.sql += ' ORDER BY ' + orderByArr.join(' , ');
                }

                ops.sql += ' LIMIT ' + limit;

                var me = this;
                logger.debug('[' + name + '] getEntityByCriteria, select ops = ' + JSON.stringify(ops));
                pool.getConnection(function(err, connection) {
                    if (err) {
                        logger.warn(err);
                        cb(err);
                    }
                    else {
                        var rowCount = 0;
                        var doneCount = 0;
                        var allEntitys = [];
                        if (typeof processRow === 'string' && /map/i.test(processRow)) {
                            allEntitys = {};
                        }
                        connection.query(ops)
                            .on('error', function (err) {
                                logger.warn(err);
                                cb(err);
                            })
                            .on('result', function (row) {
                                logger.debug('[' + name + '] getEntityByCriteria, row = ' + JSON.stringify(row));
                                var valid = false;
                                var entity = me.createEmptyEntity();
                                exObj.each(row, function (value, prop) {
                                    var m = prop.match(regexp);
                                    if (m && m.length === 2) {
                                        var col = m[1];
                                        if (u2cAllFields.hasOwnProperty(col)) {
                                            var item = u2cAllFields[col];
                                            entity[item.c] = db2JsByType(value, item.t);
                                            valid = true;
                                        }
                                    }
                                });
                                if (valid) {
                                    if (typeof processRow === 'function') {
                                        rowCount++;
                                        if (rowCount === parallSize) {
                                            rowCount = 0;
                                            connection.pause();
                                        }

                                        /*
                                         * // connection timeout is mysqlTimeout, so give processRow at most mysqlTimeout/2 to do its job no matter whether it is sync or async
                                         * var timer = setTimeout(function () {
                                         *     connection.resume();
                                         * }, mysqlTimeout/2);
                                         * var resumeWrap = function () {
                                         *     if (timer) {
                                         *         clearTimeout(timer);
                                         *         timer = null;
                                         *         connection.resume();
                                         *     }
                                         * };
                                         */
                                        
                                        processRow(entity, function () {
                                            doneCount++;
                                            if (doneCount === parallSize) {
                                                doneCount = 0;
                                                connection.resume();
                                            }
                                        });
                                    }
                                    else if (typeof processRow === 'string' && /map/i.test(processRow)) {
                                        var primaryKey = commonColsMapping.id ? commonColsMapping.id : 'id';
                                        allEntitys[entity[primaryKey]] = entity;
                                    }
                                    else {
                                        allEntitys.push(entity);
                                    }
                                }
                            })
                            .on('end', function () {
                                connection.release();
                                cb(null, allEntitys);
                            });
                    }
                });
            },

            // TODO: to support batch entitys save action !
            saveEntity: function () {
                var args = Array.prototype.slice.call(arguments, 0);
                var cb = args.pop();
                var entity = args.shift();
                var force = args.shift() || false;

                var me = this;
                if (Object.prototype.toString.call(entity) !== '[object Object]') {
                    entity = {};
                }

                var cols = [];
                var values = [];
                var cv = [];
                var len = 0;
                exObj.each(u2cFields, function (item, u) {
                    if (entity.hasOwnProperty(item.c)) {
                        var val = entity[item.c];
                        if (val !== null && typeof val !== 'undefined') {
                            var v = MYSQL.escape(js2DbByType(val, item.t));
                            var uEsc = MYSQL.escapeId(u);
                            cols.push(uEsc);
                            values.push(v);
                            cv.push(uEsc + '=' + v);
                            len++;
                        }
                    }
                    else {
                        entity[item.c] = null;
                    }
                });
                if (len > 0) {
                    var primaryKey = commonColsMapping.id ? commonColsMapping.id : 'id';
                    me.getEntityById(entity[primaryKey], function (err, oldEntity) {
                        if (err) {
                            cb(err);
                        }
                        else {
                            if (oldEntity) {
                                // check whether really need to update
                                // For json field, need to recursion each item, if json field very huge and qps high, this will be a performance bottle neck !
                                // Thus, involve 'force'
                                var needToUpdate = false;
                                if (force) {
                                    needToUpdate = true;
                                }
                                else {
                                    exObj.each(u2cFields, function (item) {
                                        var prop = item.c;
                                        var value = entity[prop];
                                        if (value !== null && !exObj.isEqual(value, oldEntity[prop])) {
                                            needToUpdate = true;
                                            // return false to break loop
                                            return false;
                                        }
                                    });
                                }
                                // update 
                                if (needToUpdate) {
                                    var update = 'UPDATE ' + name + ' SET ' + cv.concat(commonColsWrap(entity, 'update', commonColsMapping.version ? oldEntity[commonColsMapping.version] : null)).join(',') 
                                            + ' WHERE ' + MYSQL.escapeId(toUnderLine(primaryKey)) + ' = ' + MYSQL.escape(entity[primaryKey]);
                                    logger.debug('[' + name + '] saveEntity, update = ' + update);
                                    pool.query(update, function (err, result) {
                                        if (err) {
                                            logger.warn(err)
                                            cb(err);
                                        }
                                        else {
                                            if (result.affectedRows !== 1) {
                                                logger.warn('insert 1 row, but return ' + result.affectedRows + ' rows !');
                                            }
                                            exObj.each(entity, function (value, prop) {
                                                if (value === null && oldEntity.hasOwnProperty(prop)) {
                                                    entity[prop] = oldEntity[prop];
                                                }
                                            });
                                            cb(null, entity);
                                        }
                                    });
                                }
                                else {
                                    logger.debug('[' + name + '] saveEntity, entity already there and totally same, no need to update');
                                    cb(null, oldEntity);
                                }
                            }
                            else {
                                // insert
                                var ccv = commonColsWrap(entity, 'insert');
                                var insert = 'INSERT INTO ' + name + ' (' + cols.concat(ccv[0]).join(',') + ') VALUES (' + values.concat(ccv[1]).join(',') + ')'; 
                                logger.debug('[' + name + '] saveEntity, insert = ' + insert);
                                pool.query(insert, function (err, result) {
                                    if (err) {
                                        logger.warn(err)
                                        cb(err);
                                    }
                                    else {
                                        if (result.affectedRows !== 1) {
                                            logger.warn('insert 1 row, but return ' + result.affectedRows + ' rows !');
                                        }
                                        entity[primaryKey] = result.insertId;
                                        // TODO: verion, gmt_ still lost
                                        cb(null, entity);
                                    }
                                });
                            }
                        }
                    });
                }
                else {
                    cb('invalid entity');
                }
            }
        };
    };
}) ();

// ----------
// json spec check
// ----------
/*
 * var json = {
 *   head: {
 *      title: "",
 *      logo: {
 *          a: "",
 *          b: ""
 *      },
 *      list: [
 *          {
 *              text: "",
 *              options: 1
 *          },
 *          {
 *              text: "",
 *              options: 2
 *          }
 *      ]
 *   },
 *   body: {
 *      content: [
 *          {
 *              title: "",
 *              art: "",
 *              show: true,
 *              ids: [1,2,3]
 *          },
 *          {
 *              title: "",
 *              art: "",
 *              show: false,
 *              ids: [4,5,6]
 *          }
 *      ],
 *      total: 1000,
 *      days: ["2014-06-12 10:11:12", "2015-10-20 10:11:23"],
 *      opts: ["aaa", "bbb", "ccc"]
 *   }
 * }
 * var spec = [
 *     {
 *         "name": "head",
 *         "type": "object",
 *         "require": true
 *         "item": [
 *             {
 *                 "name": "title",
 *                 "type": "string",
 *                 "require": false
 *             },
 *             {
 *                 "name": "logo",
 *                 "type": "object",
 *                 "item": [
 *                     {
 *                         "name": "a",
 *                         "type": "string"
 *                     },
 *                     {
 *                         "name": "b",
 *                         "type": "string"
 *                     }
 *                 ]
 *             },
 *             {
 *                 "name": "list",
 *                 "type": "array-object",
 *                 "item": [
 *                     {
 *                         "name": "text",
 *                         "type": "string"
 *                     },
 *                     {
 *                         "name": "options",
 *                         "type": "number"
 *                     }
 *                 ]
 *             }
 *         ]
 *     },
 *     {
 *         "name": "body",
 *         "type": "object",
 *         "item": [
 *             {
 *                 "name": "content",
 *                 "type": "array-object",
 *                 "item": [
 *                     {
 *                         "name": "title",
 *                         "type": "string"
 *                     },
 *                     {
 *                         "name": "art",
 *                         "type": "string"
 *                     },
 *                     {
 *                         "name": "show",
 *                         "type": "boolean"
 *                     },
 *                     {
 *                         "name": "ids",
 *                         "type": "array-number"
 *                     }
 *                 ]
 *             },     
 *             {
 *                 "name": "total",
 *                 "type": "number"
 *             },     
 *             {
 *                 "name": "days",
 *                 "type": "array-string"
 *             },     
 *             {
 *                 "name": "opts",
 *                 "type": "array-string"
 *             },     
 *         ]
 *     }
 * ] 
 */
/*
 * TODO: the "array" part is totally wrong, the correct way is: 
 *      spec = [
 *          {
 *              "name": "thisIsAnArray",
 *              "type": "array",
 *              "item": {
 *                  "type": "object",
 *                  "property": [
 *                      {
 *                          "name": "a",
 *                          "type": "number"
 *                      },
 *                      {
 *                          "name": "b",
 *                          "type": "string"
 *                      }
 *                  ]
 *              }
 *          }
 *      ]
 *  Can refer JsonUtils (my java lib) ...
 */
var checkJsonObjBySpec = function (jsonObj, spec) {
    if (spec && Object.prototype.toString.call(spec) === '[object Array]' && 
        jsonObj && Object.prototype.toString.call(jsonObj) === '[object Object]') {
        return spec.every(function (s) {
            if (jsonObj.hasOwnProperty(s.name)) {
                logger.debug('[checkJsonObjBySpec] checking on ', s.name);
                switch (s.type) {
                    case 'number':
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object Number]';
                    case 'string':
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object String]';
                    case 'boolean':
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object Boolean]';
                    case 'regexp': 
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object RegExp]';
                    case 'date': 
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object Date]';
                    case 'function': 
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object Function]';
                    case 'object':
                        if (Object.prototype.toString.call(jsonObj[s.name]) === '[object Object]') {
                            if (s.hasOwnProperty('item')) {
                                return checkJsonObjBySpec(jsonObj[s.name], s['item']);
                            }
                            else {
                                return true;
                            }
                        }
                        else {
                            return false;
                        }
                    case 'array': 
                        return Object.prototype.toString.call(jsonObj[s.name]) === '[object Array]';
                    case 'array-number':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object Number]';
                        });
                    case 'array-string':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object String]';
                        });
                    case 'array-boolean':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object Boolean]';
                        });
                    case 'array-regexp':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object RegExp]';
                        });
                    case 'array-date':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object Date]';
                        });
                    case 'array-function':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            return Object.prototype.toString.call(j) === '[object Function]';
                        });
                    case 'array-object':
                        if (Object.prototype.toString.call(jsonObj[s.name]) !== '[object Array]') {
                            return false;
                        }
                        return jsonObj[s.name].every(function (j) {
                            if (Object.prototype.toString.call(j) === '[object Object]') {
                                return checkJsonObjBySpec(j, s.item);
                            }
                            else {
                                return false;
                            }
                        });
                    // TODO: what if 'array-array' ?
                    default: 
                        logger.debug('[checkJsonObjBySpec] unkonw type: ', s.type);
                        return false;
                }
            }
            else if (s.hasOwnProperty('require') && s['require'] === false) {
                return true;
            }
            else {
                return false;
            }
        });
    }
    else {
        return false;
    }
};

var extractSpecFromJsonObj = function (jsonObj) {
    var spec = [];
    if (jsonObj && Object.prototype.toString.call(jsonObj) === '[object Object]') {
        exObj.each(jsonObj, function (value, prop) {
            switch (Object.prototype.toString.call(value)) {
                case '[object Number]':
                    spec.push({
                        name: prop,
                        type: 'number'
                    });
                    break;
                case '[object String]':
                    spec.push({
                        name: prop,
                        type: 'string'
                    });
                    break;
                case '[object Boolean]':
                    spec.push({
                        name: prop,
                        type: 'boolean'
                    });
                    break;
                case '[object RegExp]':
                    spec.push({
                        name: prop,
                        type: 'regexp'
                    });
                    break;
                case '[object Date]':
                    spec.push({
                        name: prop,
                        type: 'date'
                    });
                    break;
                case '[object Function]':
                    spec.push({
                        name: prop,
                        type: 'function'
                    });
                    break;
                case '[object Object]':
                    spec.push({
                        name: prop,
                        type: 'object',
                        item: extractSpecFromJsonObj(value)
                    });
                    break;
                case '[object Array]':
                    if (value.length > 0) {
                        var ele = value[0];
                        switch (Object.prototype.toString.call(ele)) {
                            case '[object Number]':
                                spec.push({
                                    name: prop,
                                    type: 'array-number'
                                });
                                break;
                            case '[object String]':
                                spec.push({
                                    name: prop,
                                    type: 'array-string'
                                });
                                break;
                            case '[object Boolean]':
                                spec.push({
                                    name: prop,
                                    type: 'array-boolean'
                                });
                                break;
                            case '[object RegExp]':
                                spec.push({
                                    name: prop,
                                    type: 'array-regexp'
                                });
                                break;
                            case '[object Date]':
                                spec.push({
                                    name: prop,
                                    type: 'array-date'
                                });
                                break;
                            case '[object Function]':
                                spec.push({
                                    name: prop,
                                    type: 'array-function'
                                });
                                break;
                            case '[object Object]':
                                spec.push({
                                    name: prop,
                                    type: 'array-object',
                                    item: extractSpecFromJsonObj(ele)
                                });
                                break;
                            default: break;
                        }
                    }
                    break;
                default: break;
            }
        });
    }
    return spec;
};

exports.checkJsonObjBySpec = checkJsonObjBySpec;
exports.extractSpecFromJsonObj = extractSpecFromJsonObj;


// ------------------
// ConfLoader
// ------------------
exports.getConfLoader = (function () {

    // @confFile:  confFile must be absolute path !
    function ConfLoader(confFile) {
        this.index = 0;
        this.confContents = [null, null];
        var me = this;
        CHOKIDAR.watch(confFile).on('all', function (ev) {
            if (ev === 'add' || ev === 'change') {
                logger.info('ConfLoader: "' + confFile + '" add/changed, start to (re)load ...');
                FS.readFile(confFile, 'utf8', function (err, data) {
                    if (err) {
                        logger.warn('ConfLoader: "' + confFile + '" fail to (re)load: ', err);
                    }
                    else {
                        var localConfContent = null;
                        try {
                            localConfContent = JSON.parse(data);
                        }
                        catch (err) {
                            logger.warn('ConfLoader: "' + confFile + '" fail to parse json from conf: ', err);
                        }
        
                        if (localConfContent) {
                            me.confContents[me.index] = localConfContent;
                            me.index = 1 - me.index;
                        }
                    }
                });
            }
        });
    }
    
    ConfLoader.prototype.getConf = function () {
        return this.confContents[1 - this.index];
    };
    
    var confLoaderList = {};
    return function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var confFile = PH.join.apply(null, args);

        if (confLoaderList.hasOwnProperty(confFile)) {
            return confLoaderList[confFile];
        }

        var confLoader = new ConfLoader(confFile);
        confLoaderList[confFile] = confLoader;
        return confLoader;
    };
}) ();


// ----------------
// tag pool
// ----------------
exports.getTagPool = (function () {

    function Tag(prefix, index) {
        this.prefix = prefix;
        this.index = index;
        this.inUse = false;
    }

    Tag.prototype.back = function () {
        if (this.inUse && tagPoolList.hasOwnProperty(this.prefix)) {
            this.inUse = false;
            tagPoolList[this.prefix].tags.push(this);
        }
    };

    Tag.prototype.getName = function () {
        return this.prefix + '' + this.index;
    };

    Tag.prototype.toString = function () {
        return this.getName();
    }

    function TagPool(prefix, initSize) {
        this.tags = [];
        this.prefix = prefix;
        this.tagIndex = 0;
        for (this.tagIndex = 0; this.tagIndex < initSize; this.tagIndex++) {
            this.tags.push(new Tag(this.prefix, this.tagIndex));
        }
    }

    TagPool.prototype.take = function () {
        var tag;
        if (this.tags.length > 0) {
            tag = this.tags.shift();
        }
        else {
            this.tagIndex++;
            tag = new Tag(this.prefix, this.tagIndex);
        }
        tag.inUse = true;
        return tag;
    };

    var tagPoolList = {};
    return function (prefix, initSize) {
        var size = initSize || 100;
        if (!tagPoolList.hasOwnProperty(prefix)) {
            tagPoolList[prefix] = new TagPool(prefix, size);
        }
        return tagPoolList[prefix];
    };
}) ();


// ------------------
// Express & Logger
// ------------------
/*
 * log4js express part is too weak (can only log out req when res finish), 
 * create a new one myself !
 */
exports.expressLogger = function (level) {
    var expressLogger = log4js.getLogger('express-awen');
    expressLogger.setLevel(level || 'INFO');
    // return log4js.connectLogger(logger, {level: 'auto'});
    var logLevels = ['trace','debug','info','warn','error','fatal'];
    return function (req, res, next) {
        // all customized obj assign to 'awen'
        if (!req.awen) {
            req.awen = {};
            req.awen.data = {}; // to restore internal data
        }
        if (!res.awen) {
            res.awen = {};
        }

        var pattern = '[' + tools.genRandomPattern(10) + ']';
        req.awen.logger = {};
        logLevels.forEach(function (l) {
            req.awen.logger[l] = function () {
                var args = Array.prototype.slice.call(arguments, 0);
                args.unshift(pattern);
                args.unshift(l);
                expressLogger.log.apply(expressLogger, args);
            };
        });
        res.awen.logger = req.awen.logger;
        req.awen.recordTimeResult = [];
        
        // var start = null;
        // TODO: if req.on('end') never in, then exception will comes out inside res.on('finish')
        var start = new Date();

        var remote = req.ip;
        var method = req.method;
        var urlObj = URL.parse(req.url, true);
        var pathname = urlObj.pathname;
        var qs = urlObj.query;
        var body = '';
        req.on('data', function (chunk) {
            body += chunk;
        });
        req.on('end', function () {
            start = new Date();
            req.awen.logger.info('in >> ' + remote + ' - ' + method + ' - ' + pathname + ' - ' + JSON.stringify(qs) + ' - ' + body);
        });
        res.on('finish', function () {
            var logout = res.awen.logger.info;
            if(res.statusCode >= 400) {
                logout = res.awen.logger.error;   
            }
            else if(res.statusCode >= 300) {
                logout = res.awen.logger.warn;
            }
            var end = new Date();
            var consume = end.getTime() - start.getTime();
            logout('out << ' + remote + ' - ' + method + ' - ' + pathname + ' - ' + JSON.stringify(qs) + ' - ' + body + ' - ' + res.statusCode + ' - ' + consume + 'ms' + ' - ' + req.awen.recordTimeResult.join('|'));
        });
        next();
    };
};

/*
 * @app:
 *
 * @routerPath: 
 *      must be an absolute path, 
 *      its basename should be '[uri]-[method].js'
 *      this node module should contain: {
 *          qsSpec: [],
 *          bodySpec: [],
 *          steps: {},
 *          before: function () {},     // must be sync fn
 *          after: function () {}       // must be sync fn
 *      }
 *
 *  e.g.
 *      abc-post.js:
 *          module.exports = {
 *              bodySpec: [
 *                  { name: 'abc', type: 'string' },
 *                  { name: 'def', type: 'number' }
 *              ],
 *              steps: {
 *                  aaa: function (req, res, next) {
 *                      req.awen.logger.info('xxx');
 *                      next();
 *                  }
 *              }
 *          };
 *  Update: 
 *      @routerPath could not only be a js file but also a package (both with absolute path pointed) !!!
 *          This is also targeted for making much standard on CommonJS & AMD, which is: alway omitt .js when require
 *          Based on the node require strategy, @routerPath without .js is a good practice (not a must, but please follow) !
 *      @routerPath support multi-level uri like: a/b/c/d (use '.' to subtitude '/' for file/package name)
 *
 *      With both 2 new features in, examples could be:
 *          data.user.detail-get.js             --> call: loadExpressRouter(app, __dirname, data.user.detail-get)
 *                                                  Method: GET 
 *                                                  URI: /data/user/detail
 *                                                  This is a js file module
 *
 *          data.user.detail.update-post        --> call: loadExpressRouter(app, __dirname, data.user.detail.update-post)
 *              |-- package.json                    Method: POST
 *              |-- index.js                        URI: /data/user/detail/update 
 *              |-- lib                             This is a package module
 *              |-- node_modules
 */
// TODO: to support restful
exports.loadExpressRouter = function () {
    // args:
    var args = Array.prototype.slice.call(arguments, 0);
    // @app
    var app = args.shift();
    // @routerPath
    var routerPath = PH.join.apply(null, args);

    // var routerName = PH.basename(routerPath, '.js');
    var routerName = PH.basename(routerPath);
    var fields = routerName.split('-');
    if (fields.length !== 2) {
        logger.error('routerName wrong: ' + routerName);
        process.exit(-1);
    }
    var uri = fields[0].replace(/\./g, '/');
    uri = uri[0] === '/' ? uri : '/' + uri;
    var method = fields[1];
    
    // only support: post, get, delete, put
    if (method !== 'post' && method !== 'get' && method !== 'delete' && method !== 'put') {
        logger.error('unkonw method: ' + method + ' for ' + uri);
        process.exit(-1);
    }

    var actionOps = null;
    try {
        actionOps = require(routerPath);
    }
    catch (err) {
        logger.error('fail to load router: ', err);
        process.exit(-1);
    }

    var cleanup = actionOps.after;
    var init = actionOps.before;
    var appArgs = [uri, function (req, res, next) {
        // do init stuff due to this is the first middleware
        if (!req.awen) {
            req.awen = {};
        }
        if (!res.awen) {
            res.awen = {};
        }

        // use awen logger if expressLogger never use
        if (!req.awen.logger) {
            req.awen.logger = logger;
        }
        if (!res.awen.logger) {
            res.awen.logger = logger;
        }

        // define success & fail
        res.awen.json = res.json.bind(res);
        res.awen.success = function () {
            if (typeof cleanup === 'function') {
                cleanup(req, res);
            }
            res.awen.logger.info('success: ' + JSON.stringify(this.success.result));
            this.json({
                'status': 0,
                'message': 'success',
                'result': this.success.result
            });
        };
        res.awen.success.result = {};

        res.awen.fail = function (msg) {
            if (typeof cleanup === 'function') {
                cleanup(req, res);
            }
            res.awen.logger.warn('fail: ' + msg);
            this.json({
                'status': 1,
                'message': msg,
                'result': this.success.result
            });
        }

        if (typeof init === 'function') {
            init();
        }

        next();
    }];

    var qsSpec = actionOps.qsSpec;
    if (qsSpec) {
        appArgs.push(function (req, res, next) {
            if (checkJsonObjBySpec(req.query, qsSpec)) {
                next();
            }
            else {
                res.awen.fail('input qs format wrong');
            }
        });
    }

    var bodySpec = actionOps.bodySpec;
    if (bodySpec) {
        appArgs.push(function (req, res, next) {
            if (checkJsonObjBySpec(req.body, bodySpec)) {
                next();
            }
            else {
                res.awen.fail('input body format wrong');
            }
        });
    }

    exObj.each(actionOps.steps, function (stepFn, name) {
        if (typeof stepFn === 'function') {
            appArgs.push(function (req, res, next) {
                var start = new Date();
                stepFn(req, res, function () {
                    var end = new Date();
                    var consume = end.getTime() - start.getTime();
                    req.awen.logger.info('@' + name + ' done, consume ' + consume + 'ms');
                    if (Object.prototype.toString.call(req.awen.recordTimeResult) === '[object Array]') {
                        req.awen.recordTimeResult.push(consume);
                    }
                    next();
                })
            });
        }   
    });

    appArgs.push(function (req, res) {
        res.awen.success();
    });

    app[method].apply(app, appArgs);
};
