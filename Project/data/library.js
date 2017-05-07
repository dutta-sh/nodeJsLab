const mongoCollections = require("./mongoCollections");
const libraryDB = mongoCollections.library;
const request = require("request");
const fs = require('fs');

function getAllBooksFromLibrary() {
    return libraryDB().then((library) => {
        return library.find({}).toArray();
    });
}

function getBookFromLibrary(id) {
    return libraryDB().then((library) => {
        return library.findOne({ _id: id }).then((book) => {
            return book;
        });
    });
}

function getLibraryByPage(numStr) {
    return new Promise((fulfill, reject) => {
        try {
            num = Number(numStr);
            this.getAllBooksFromLibrary().then(library => {
                let perPage = 25;
                let minPage = 0;
                let maxPage = (Math.ceil(library.length/perPage) - 1);
                if(library.length === 0)
                    reject('You have not added any books to the library !!');
                if(isNaN(num) || !Number.isInteger(num) || num < minPage || num > maxPage)
                    reject(numStr + ' is Invalid page number !!');
                
                let startIdx = num * perPage;
                let endIdx = startIdx + (perPage-1) > library.length ? library.length : startIdx + (perPage-1);

                let books = [];
                for(i=startIdx; i <= endIdx; i++) {
                    if(library[i])
                        books.push({_id : library[i]._id, title : library[i].title, folder : library[i].folder});
                }
                fulfill({currPage : num, minPage : minPage, maxPage: maxPage, books : books});
            });                
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function addBookToLibrary(id, title, folder) {
    return libraryDB().then((library) => {
        let newBook = {
            title: title,
            folder: folder,
            _id: id
        };

        return library.insertOne(newBook).then((newInsert) => {
            return newInsert.insertedId;
        }).then((newId) => {
            return this.getBookFromLibrary(newId);
        });
    });
}

function getFromGutenberg(url) {
    console.log("getting from gutenberg: " + url);
    return new Promise((fulfill, reject) => {
        request({
            url: url,
            json: false
        }, (error, response, result) => {
            if (!error && response && response.statusCode === 200) {
                fulfill(result);
            } else {
                if (error)
                    fulfill({ "error": error });
                if (!response.statusCode === 200)
                    fulfill({ "error": "httpResponse: " + response.statusCode });

                fulfill({ "error": "unknown error" });
            }
        })
    })
}

function writeToFile(content, location) {
    stream = fs.createWriteStream(location);
    stream.once('open', () => {
        stream.write(content);
        stream.end();
    });
}

// getFromGutenberg('http://www.gutenberg.org/ebooks/1.html.noimages');
module.exports = { getFromGutenberg, writeToFile, getBookFromLibrary, addBookToLibrary, getAllBooksFromLibrary, getLibraryByPage };