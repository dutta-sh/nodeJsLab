const bluebird = require("bluebird");
const flat = require("flat");
const unflatten = flat.unflatten
const redis = require('redis');
const client = redis.createClient();

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

let addToCache = async(person) => {
    let hmSetAsyncPerson = await client.hmsetAsync(person.id, flat(person));
    if(hmSetAsyncPerson == 'OK') {
        console.log('added to cache: ' + person.id)
    } else {
        throw new Error(person.id + ' not added to cache !!');
    }
}

let fetchFromCache = async(id) => {
    let person = await client.hgetallAsync(id);
    if(person) {
        return unflatten(person);
    } else {
        throw new Error(id + ' is not cached !!');
    }
}

let addToHistory = async(id) => {
    let list = await client.hgetallAsync("history");
    var history;
    if(!list) {
        history = { visitors : [id] };
    } else {
        history = unflatten(list);
        history.visitors[history.visitors.length] = id;
    }
    console.log(history);
    let hmSetAsyncHistory = await client.hmsetAsync("history", flat(history));
}

let fetchHistory = async() => {
    let people = [];
    let list = await client.hgetallAsync("history");
    if(!list) 
        return people;
    
    let history = unflatten(list);
    let start = history.visitors.length - 1;
    let end = 0;
    if(history.visitors.length > 20) {
        end = history.visitors.length - 20;
    }
    for(i = start; i>=end; i--) {
        let id = history.visitors[i];        
        let person = await fetchFromCache(id).then((person => {
            people.push(person);
        }));
    }
    return people;
}

module.exports = {
    addToCache,
    fetchFromCache,
    addToHistory,
    fetchHistory
};