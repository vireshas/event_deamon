var eventEmitter  = require('events').EventEmitter,
    em = new eventEmitter(),
    redis = require("redis"),
    client = redis.createClient(),
    async = require("async")

client.on("error", function (err) {
    console.log("manager: Error " + err);
});

var startTime = new Date()
var redisEventsKey = "zevents"
var deamonWakeup = 2000 //in ms

var callback = function() {
    console.log("manager: callback called from the event")
}

//alert if an event real_event occures 10 times in 1min
var eventMeta = {}

eventMeta["real_event"] = {
    expiry : 30,//seconds
    count : 30,
    type : "after",//an cb to be called before or after matching the codition
    cb : callback
}


function getScore() {
    var now = new Date();
    var rankInMs = now - startTime;
    return Math.round((rankInMs)/1000);
}

em.on(redisEventsKey, function(eventName) {
    client.zscore(redisEventsKey, eventName, function(err, response) {
        if(response == null) { 
            var score = getScore() + eventMeta[eventName].expiry
            client.zadd(redisEventsKey, score, eventName, function(err, resp){
                console.log("manager: expiry set to ", score)
            });
        }
        client.incr(eventName, function(err, resp){
            console.log("manager: event count ", resp)
        });
    });
});

var daemon = function() {
    console.log("daemon: wokeup")
    var score = getScore()
    console.log("daemon: fetching events with score ", score)
    client.zrangebyscore(redisEventsKey, "-inf", score, function(err, response){
        console.log("daemon: expired events ", response)
        var toDo = function(eventName, cb){
            console.log("daemon: eventname ", eventName)
            client.get(eventName, function(err, resp) {
                console.log("daemon: event count  ", resp)
                if(resp >= eventMeta[eventName].count) {
                    console.log("daemon: event callback is being called")
                    eventMeta[eventName].cb()
                }
            })
        }
        async.map(response, toDo);
    });  
}

setInterval(daemon, deamonWakeup)

setInterval(function() {
    em.emit(redisEventsKey, "real_event")
}, 1000);
