const people = require("./data");

function getById(idStr) {
    return new Promise((fulfill, reject) => {
        try {
            id = Number(idStr);

            if(isNaN(id) || !Number.isInteger(id) || id < 1 || id > people.length)
                throw new Error(idStr + ' is Invalid id !!');
            
            fulfill(people[id - 1]);
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function getByPage(numStr) {
    return new Promise((fulfill, reject) => {
        try {
            num = Number(numStr);

            let minPage = 0;
            let maxPage = (Math.ceil(people.length/25) - 1);
            if(isNaN(num) || !Number.isInteger(num) || num < minPage || num > maxPage)
                throw new Error(numStr + ' is Invalid page number !!');
            
            let startIdx = num * 25;
            let endIdx = startIdx + 24 > people.length ? people.length : startIdx + 24;

            let users = [];
            for(i=startIdx; i <= endIdx; i++) {
                users.push({id : people[i].id, name : people[i].first_name + " " + people[i].last_name});
            }
            fulfill({currPage : num, minPage : minPage, maxPage: maxPage, users : users});
            //fulfill(people.slice(startIdx, endIdx + 1));
        } catch (ex) {
            reject(ex.message);
        }
    })
}

module.exports = {getById, getByPage};