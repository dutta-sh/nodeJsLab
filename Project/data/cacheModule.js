const bluebird = require("bluebird");
const flat = require("flat");
const unflatten = flat.unflatten
const redis = require('redis');
const client = redis.createClient();

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

let addToCache = async(page) => {
    let hmSetAsyncPerson = await client.hmsetAsync(page.currPage, flat(page));
    if(hmSetAsyncPerson == 'OK') {
        console.log('added to cache: ' + page.currPage)
    } else {
        throw new Error(page.currPage + ' not added to cache !!');
    }
}

let fetchFromCache = async(currPage) => {
    let page = await client.hgetallAsync(currPage);
    if(page) {
        return unflatten(page);
    } else {
        throw new Error(currPage + ' is not cached !!');
    }
}

module.exports = {
    addToCache,
    fetchFromCache
};