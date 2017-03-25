const people = require("./data");

function getById(id) {
    return new Promise((fulfill, reject) => {
        setTimeout(() => {
            try {
                if (isNaN(id) || !Number.isInteger(id))
                    throw new Error(id + ' is Non integer id !!');

                if(id < 1 || id > people.length)
                    throw new Error(id + ' is Invalid id !!');
                    
                fulfill(people[id - 1]);
            } catch (ex) {
                reject(ex.message);
            }            
        }, 5000);
    })
}

module.exports = {getById};