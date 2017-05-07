const mongoCollections = require("./mongoCollections");
const booksDB = mongoCollections.books;
const library = require("./library");
const fs = require('fs');

let exportedMethods = {
    getAllBooks() {
        return booksDB().then((books) => {
            return books.find({}).toArray();
        });
    },
    getBookById(id) {
        return booksDB().then((books) => {
            return books.findOne({ _id: id }).then((book) => {
                if (!book) throw "Book not found";
                return book;
            });
        });
    },
    getBooksByPage(numStr) {
        return new Promise((fulfill, reject) => {
            try {
                num = Number(numStr);
                this.getAllBooks().then(catalog => {
                    let perPage = 25;
                    let minPage = 0;
                    let maxPage = (Math.ceil(catalog.length/perPage) - 1);
                    if(catalog.length === 0)
                        reject('We are unable to retrieve the catalog. Please try again later !!');
                    if(isNaN(num) || !Number.isInteger(num) || num < minPage || num > maxPage)
                        reject(numStr + ' is Invalid page number !!');
                    
                    let startIdx = num * perPage;
                    let endIdx = startIdx + (perPage-1) > catalog.length ? catalog.length : startIdx + (perPage-1);

                    let books = [];
                    for(i=startIdx; i <= endIdx; i++) {
                        if(catalog[i])
                            books.push({_id : catalog[i]._id, title : catalog[i].title, url : catalog[i].url});
                    }
                    fulfill({currPage : num, minPage : minPage, maxPage: maxPage, books : books});
                });                
            } catch (ex) {
                reject(ex.message);
            }
        })
    },    
    addBook(book) {
        return booksDB().then((books) => {            
            books.insertOne(book);
            console.log('added to DB: ' + book._id);
        });        
    },
    addBooks(bookList) {
        return booksDB().then((books) => {            
            bookList.forEach(book => {
                books.insertOne(book);
                console.log('added to DB: ' + book._id);
            });            
        });
    },
    removeBook(id) {
        return booksDB().then((books) => {
            return books.removeOne({ _id: id }).then((deletionInfo) => {
                if (deletionInfo.deletedCount === 0) {
                    throw (`Could not delete Book with id of ${id}`)
                }
            });
        });
    },
    downloadBook(id) {
        return library.getBookFromLibrary(id).then(book => {
            if (!book) {
                return booksDB().then((books) => {
                    return books.findOne({ _id: id }).then(book => {
                        if (!book) throw "Book not found";
                        return library.getFromGutenberg(book.url).then(content => {
                            library.writeToFile(content, "./downloads/" + book._id + ".html");
                            library.addBookToLibrary(book._id, book.title, "./downloads/" + book._id + ".html");
                            book.content = content;
                            return book;
                        });
                    });
                });
            } else {
                try {                    
                    content = fs.readFileSync(book.folder, 'utf8');
                    book.content = content;
                    return book;
                } catch (er) {
                    return booksDB().then((books) => {
                    return books.findOne({ _id: id }).then(book => {
                        if (!book) throw "Book not found";
                        return library.getFromGutenberg(book.url).then(content => {
                            library.writeToFile(content, "./downloads/" + book._id + ".html");
                            book.content = content;
                            return book;
                        });
                    });
                });
                }
            }
        })
    }
}

module.exports = exportedMethods;