var AW = require('../awenlib.js');

AW.consoleOn();
var logger = AW.logger('test', 'debug');

var tagPool = AW.getTagPool('tagTest');

(function () {
var a1 = [1, 2, 3, 8, 1, 9];
AW.exArray.remove(a1, 1);
logger.debug(a1);
}) ();

// test simpleFlowController
(function () {
    logger.debug('----------------- test simpleFlowController -------------------');

    var asyncFn = function () {
        var args = Array.prototype.slice.call(arguments, 0);
        var cb = args.pop();
        var ms = args.shift();

        setTimeout(function () {
            cb();
        }, ms);
    }

    var flowController = AW.createSimpleFlowController();
    var asyncStepA = flowController.createStep(asyncFn, 1000, 'abc', function () { logger.debug('stepA done'); });
    var asyncStepB = flowController.createStep(asyncFn, 2000, 123, 'xxx', function () { logger.debug('stepB done'); });
    var asyncStepC = flowController.createStep(asyncFn, 3000, function () { logger.debug('stepC done'); });
    var asyncStepD = flowController.createStep(asyncFn, 4000, 1, 2, 3, 4, 5, function () { logger.debug('stepD done'); });
    var asyncStepE = flowController.createStep(asyncFn, 5000, 'a', 'b', function () {logger.debug('stepE done'); });
    var asyncStepF = flowController.createStep(asyncFn, 6000, function () { logger.debug('stepF done'); });
    
    asyncStepA.next(asyncStepB);
    asyncStepA.next(asyncStepC);
    asyncStepB.next(asyncStepF);
    asyncStepC.next(asyncStepE);
    asyncStepD.next(asyncStepC);

    // i wil fail on this:
    //      loop inside DAG 
    // case A:
    //   asyncStepC.next(asyncStepA);
    // case B:
    //   asyncStepB.next(asyncStepC);
    //   asyncStepE.next(asyncStepB);

    logger.debug('A = ' + asyncStepA.sign);
    logger.debug('B = ' + asyncStepB.sign);
    logger.debug('C = ' + asyncStepC.sign);
    logger.debug('D = ' + asyncStepD.sign);
    logger.debug('E = ' + asyncStepE.sign);
    logger.debug('F = ' + asyncStepF.sign);

    flowController.run(function (err) {
        if (err) {
            logger.error(err);
        }
        logger.debug('all done 1');

        // next round:

        // flowController.removeStep(asyncStepA);
        // flowController.removeStep(asyncStepB);
        // flowController.removeStep(asyncStepF);
        flowController.removeStep(asyncStepC);

        flowController.run(function (err) {
            if (err) {
                logger.error(err);
            }
            logger.debug('all done 2');
        }, 20000);

    }, 20000);
}) ();

/*
// -----------------------------------------------------
var no = -1;
function getNo() {
    no++
    if (no === 100) {
        no = 0
    }
    return no;
}
for (var i = 0; i < 103; i++) {
    console.log(getNo());
}
// process.exit();

(function () {
var tagOOO = {
    aaa: {
        killed: false
    },
    bbb: {
        killed: false
    },
    ccc: {
        killed: false
    }
};
var tags = [];
for (var i = 0; i < 10; i++) {
    tags.push(tagOOO.bbb);
}
logger.debug('------------------------------');
logger.debug(JSON.stringify(tags, null, 2));
logger.debug('------------------------------');
tagOOO.bbb.killed = true;
delete tagOOO.bbb;
logger.debug(JSON.stringify(tags, null, 2));
// process.exit();
}) ();

(function () {
var a1 = [1,2,3,4,5,6,7,8];
var a2 = [100, 200, 300, 4,5,88, 7, 88, 1];
logger.debug(AW.exArray.intersect(a1, a2));
// process.exit();
}) ();


(function () {
var tag1 = tagPool.take();
logger.debug(tag1.getName());
var tag2 = tagPool.take();
logger.debug(tag2.getName());
logger.debug(tagPool.tags);
tag1.back();
logger.debug(tagPool.tags);
// process.exit();
}) ();


// --------------------------------------------------------------
(function () {
var genRandomPattern = (function () {
   var candidates = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+`[]{}|\\;:\'",./<>?';
   var canlen = candidates.length;
   return function (length) {
       var pattern = '';
       for (var i = 0; i < length; i++) {
           pattern += candidates[AW.exMath.randInt(0, canlen - 1)];
       }
       return pattern;
   };
}) ();

var indexSpeedTest = {};
var arrSpeedTest = [];

var targetIndex = AW.exMath.randInt(500, 1000) - 1;
var target = '';
for (var i = 0; i < 1000; i++) { (function () {
    var key = genRandomPattern(64);
    indexSpeedTest[key] = i;
    arrSpeedTest.push(key);
    if (i === targetIndex) {
        target = key;
    }
}) ()}

logger.info(JSON.stringify(indexSpeedTest));
logger.info(JSON.stringify(arrSpeedTest));

(function () {
    var start = new Date();
    if (indexSpeedTest.hasOwnProperty(target)) {
        logger.info('index find, value = ' + indexSpeedTest[target]);
    }
    var end = new Date();
    logger.info('index find, used = ' + (end.getTime() - start.getTime()));
}) ();

(function () {
    var start = new Date();
    arrSpeedTest.some(function (item, index) {
        if (target === item) {
            logger.info('arr find, index = ' + index);
            return true;
        }
    });
    var end = new Date();
    logger.info('arr find, used = ' + (end.getTime() - start.getTime()));
}) ();

}) ();
// process.exit();

var shadowTest = {}
shadowTest.inst = {
    a: {h:1, v:1},
    b: {h:2, v:1},
    c: {h:3, v:0},
    d: {h:1, v:1},
    e: {h:2, v:1},
    f: {h:1, v:0},
    g: {h:3, v:1},
    h: {h:4, v:1},
    i: {h:1, v:1},
    j: {h:4, v:1},
    k: {h:2, v:0},
    l: {h:6, v:0},
    m: {h:5, v:0},
    n: {h:5, v:0},
    o: {h:5, v:0}
};
shadowTest.shadow = {};
// {
//      inst: {
//          a: 1,
//          b: 2,
//          g: 3,
//          h: 4
//      },
//      shadow: {
//          d: a,
//          i: a,
//          e: b,
//          j: h
//      }
// }
// AW.exObj.each(shadowTest.inst, function (value1, prop1) {
//     console.log('l1: ' + prop1 + ' = ' + value1);
//     AW.exObj.each(shadowTest.inst, function (value2, prop2) {
//         console.log('  l2: ' + prop2 + ' = ' + value2);
//         if (prop2 !== prop1 && value2 === value1) {
//             console.log('  --> shadow!');
//             shadowTest.shadow[prop2] = prop1;
//             delete shadowTest.inst[prop2];
//         }
//     });
// });
var keys = Object.keys(shadowTest.inst);
var len = keys.length;
for (var i = 0; i < len; i++) { (function (i) { var prop1 = keys[i]; if (shadowTest.inst.hasOwnProperty(prop1)) {
    var value1 = shadowTest.inst[prop1];
    console.log('l1: ' + prop1 + ' = ' + JSON.stringify(value1));
    var findSame = false;
    for (var j = i + 1; j < len; j++) { if ((function (j) { var prop2 = keys[j]; if (shadowTest.inst.hasOwnProperty(prop2)) {
        var value2 = shadowTest.inst[prop2];
        console.log('  l2: ' + prop2 + ' = ' + JSON.stringify(value2));
        
        if (value2.h === value1.h) {
            findSame = true;
            console.log('    --> looks like same');
            if (value1.v === 0) {
                console.log('      --> extra old v on 1, just delete');
                delete shadowTest.inst[prop1];
                return true;
            }
            else if (value2.v === 0) {
                console.log('      --> extra old v on 2, just delete');
                delete shadowTest.inst[prop2];
            }
            else {
                console.log('      --> shadow!');
                shadowTest.shadow[prop2] = prop1;
                delete shadowTest.inst[prop2];
            }
        }

    }}) (j)) { break; }}
    
    if (!findSame && value1.v === 0) {
        console.log('  --> delete itself, and drop host !');
        // value1.h.drop
        delete shadowTest.inst[prop1];
    }
}}) (i)}
console.log(JSON.stringify(shadowTest, null, 2));
// process.exit();
// --------------------------------------------------------------


//
// // eachOf bug:
// var async = require('async');
// async.forEachOf([1,2,3,4,5], function (item, index, cb) {
//     console.log(index + ':' + item);
//     cb();
// }, function () {
//     console.log('1 done');
// });
// async.forEachOf({a:1,b:2,c:3,d:4,e:5}, function (item, prop, cb) {
//     console.log(prop + ':' + item);
//     cb();
// }, function () {
//     console.log('2 done');
// });
// async.forEachOf('abcdefghi', function (item, key, cb) {
//     console.log(key + ':' + item);
//     cb()
// }, function () {
//     console.log('3 done');
// });
// process.exit();
//

var xs = ['a', 'b', 'c', 'd'];

AW.mainFlow(
    AW.proc(
    [
        AW.eachProcess(xs,
            AW.dataPrepare(function (x) {
                return 'echo ' + x + x + x;
            }),
            AW.exe,
            AW.dataProcess(function (echoRet) {
                var z = '***' + echoRet;
                return 'echo "' + z + '"';
            }),
            AW.exe
        ),
        AW.dataProcess(function (ret) {
            console.log('1 > ' + JSON.stringify(ret));
            return [1,2,3];
        })
    ],
    [
        AW.dataProcess(function() {
            console.log('2.1');
            return 'www';
        }),
        AW.dataProcess(function(r) {
            console.log('2.2 ' + r);
            return 'yyy';
        })
    ],
    [
    
    ]),

    AW.dataProcess(function(ret) {
        console.log('final: ' + JSON.stringify(ret));
    })
);

console.log('check isEqual ...');
var crub = [];
console.log(AW.exObj.isEqual(
        {a:1, b:"xyz", c:3, d: [100, {x: 5,  y: '9x'}, 'a', true], fuck: []}, 
        {b:"xyz", c:3, a:1, d: [100, {y: '9x', x: 5}, true, 'a'], fuck: '[[[[[]]]]]'}, 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        {a:1, b:"xyz"}, 
        {b:"xyz", fuck: '[[[[[]]]]]'}, 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        {b:"xyz"}, 
        {b:"xyz", fuck: '[[[[[]]]]]'}, 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        {b:"xyz", fuck: []}, 
        {b:"xyz", fuck: '[[[[[]]]]]'}, 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        [], 
        {b:"xyz", fuck: '[[[[[]]]]]'}, 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        [1,2,3], 
        [1,2,3,4], 
        crub));
console.log(crub);

console.log('check isEqual ...');
crub = [];
console.log(AW.exObj.isEqual(
        {a: [1,2,3], b: {}}, 
        {b: [1,2,3,4], b: {}}, 
        crub));
console.log(crub);

// test json & spec:
var json = {
  head: {
     title: "",
     logo: {
         a: "",
         b: ""
     },
     list: [
         {
             text: "",
             options: 1
         },
         {
             text: "",
             options: 2
         },
     ]
  },
  body: {
     content: [
         {
             title: "",
             art: "",
             show: true,
             ids: [1,2,3]
         },
         {
             title: "",
             art: "",
             show: false,
             ids: [4,5,6]
         }
     ],
     total: 1000,
     days: ["2014-06-12 10:11:12", "2015-10-20 10:11:23"],
     opts: ["aaa", "bbb", "ccc"]
  }
};
var spec = [
    {
        "name": "head",
        "type": "object",
        "property": [
            {
                "name": "title",
                "type": "string"
            },
            {
                "name": "logo",
                "type": "object",
                "property": [
                    {
                        "name": "a",
                        "type": "string"
                    },
                    {
                        "name": "b",
                        "type": "string"
                    }
                ]
            },
            {
                "name": "list",
                "type": "array",
                "item": {
                    "type": "object",
                    "property": [
                        {
                            "name": "text",
                            "type": "string"
                        },
                        {
                            "name": "options",
                            "type": "number"
                        }
                    ]
                }
            }
        ]
    },
    {
        "name": "body",
        "type": "object",
        "property": [
            {
                "name": "content",
                "type": "array",
                "item": {
                    "type": "object",
                    "property": [
                        {
                            "name": "title",
                            "type": "string"
                        },
                        {
                            "name": "art",
                            "type": "string"
                        },
                        {
                            "name": "show",
                            "type": "boolean"
                        },
                        {
                            "name": "ids",
                            "type": "array",
                            "item": {
                                "type": "number"
                            }
                        }
                    ]
                }
            },     
            {
                "name": "total",
                "type": "number"
            },     
            {
                "name": "days",
                "type": "array",
                "item": {
                    "type": "string"
                }
            },     
            {
                "name": "opts",
                "type": "array",
                "item": {
                    "type": "string"
                }
            }
        ]
    }
]; 
console.log('checkJsonObjBySpec: ' + AW.checkJsonObjBySpec(json, spec));
var s1 = JSON.stringify(spec);
var s2 = JSON.stringify(AW.extractSpecFromJsonObj(json));
console.log('extractSpec1: ' + s1);
console.log('extractSpec2: ' + s2);
console.log('extractSpecDiff: ' + (s1 === s2));

// DB:
var pool = AW.createDbPool('10.99.83.40', 8836, 'bdd_monitor', 'work', '123456');
var lawrenceCfgDao = AW.daoFactory(pool, 'lawrence_cfg', {
    string: ['product', 'name'],
    json: ['paramSignature', 'detectTree']
});

lawrenceCfgDao.getEntityById(1, function (err, entity) {
    if (err) {
        console.log(err);
    }
    else {
        console.log('lawrenceCfgDao.get = ' + JSON.stringify(entity, null, 2));
    }
});

var entity = lawrenceCfgDao.createEmptyEntity();
entity.id = 104;
entity.detectTree = {a: 1, b: 5};
lawrenceCfgDao.saveEntity(entity, function (err, entity) {
    if (err) {
        console.log(err);
    }
    else {
        console.log('lawrenceCfgDao.save = ' + JSON.stringify(entity, null, 2));
    }
});

// conf loader:
var conf1Loader = AW.getConfLoader(__dirname, '../testconf/conf1.json');
var conf1 = null;
setInterval(function () {
    var newConf = conf1Loader.getConf();
    if (newConf) {
        if (JSON.stringify(newConf) !== JSON.stringify(conf1)) {
            console.log(JSON.stringify(newConf));
        }
        conf1 = newConf;
    }
}, 1000);


var conf2Loader = AW.getConfLoader(__dirname, '../testconf/conf2.json');
var conf2 = null;
setInterval(function () {
    var newConf = conf2Loader.getConf();
    if (newConf) {
        if (JSON.stringify(newConf) !== JSON.stringify(conf2)) {
            console.log(JSON.stringify(newConf));
        }
        conf2 = newConf;
    }
}, 1000);
*/
