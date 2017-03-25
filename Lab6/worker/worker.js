const redisConnection = require("./redis-connection");
const people = require("./data");

console.log('We have got a worker!!');

redisConnection.on('get-person:request:*', (message, channel) => respond(message, channel, getPerson));
redisConnection.on('add-person:request:*', (message, channel) => respond(message, channel, addPerson));
redisConnection.on('delete-person:request:*', (message, channel) => respond(message, channel, deletePerson));
redisConnection.on('update-person:request:*', (message, channel) => respond(message, channel, updatePerson));

async function respond(message, channel, callback) {
    console.log(JSON.stringify(message));
    let requestId = message.requestId;
    let eventName = message.eventName;
    let response = '';
    let event = '';
    try {
        response = await callback(message.data);
        event = `${eventName}:success:${requestId}`;
    } catch (ex) {        
        response = ex;
        event = `${eventName}:failed:${requestId}`;
    }
    
    redisConnection.emit(event, {
        requestId: requestId,
        data: response,
        eventName: eventName
    });
}

function getPerson(id) {
    return new Promise((fulfill, reject) => {
        try {
            validateId(id);
            person = people[id - 1];
            if(!person)
                reject(id + ' is invalid id !!');

            fulfill(person);
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function addPerson(person) {
    return new Promise((fulfill, reject) => {
        try {
            let id = people.length + 1;
            person.id = id;

            people[id - 1] = person;
            fulfill(person);
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function deletePerson(id) {
    return new Promise((fulfill, reject) => {
        try {
            validateId(id);
            person = people[id - 1];
            if(!person)
                reject(id + ' is invalid id !!');

            delete people[id - 1];
            fulfill('{ success : ' + id + ' deleted successfully !!}');
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function updatePerson(person) {
    return new Promise((fulfill, reject) => {
        try {
            validateId(person.id);
            p = people[person.id - 1];
            if(!p) {                
                people[person.id - 1] = person;
                fulfill(person);
            }
            
            if(person.first_name)
                p.first_name = person.first_name;
            if(person.last_name)
                p.last_name = person.last_name;
            if(person.email)
                p.email = person.email;
            if(person.gender)
                p.gender = person.gender;
            if(person.ip_address)
                p.ip_address = person.ip_address;

            fulfill(p);
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function validateId(id) {
    let idTemp = Number(id);
    if (isNaN(idTemp) || !Number.isInteger(idTemp) || idTemp < 1 || idTemp > people.length)        
        throw new Error(id + ' is invalid id !!');
}