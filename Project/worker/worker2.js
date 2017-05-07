const redisConnection = require("./redis-connection");
const download = require('download-file');
const fs = require('fs');
const unzip = require('unzip');
const xml2js = require('xml2js');
const rdf = require('rdflib');
const store = rdf.graph();
const booksDB = require("../data/books");
const dbConnection = require("../data/mongoConnection");

console.log('We have got a worker!!');

redisConnection.on('initialize:request:*', (message, channel) => respond(message, channel, initialize));

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

options = { directory: "./downloads/", filename: "rdf-files.tar.zip" };
//books = [];

function initialize() {
    return new Promise((fulfill, reject) => {
        try {
            //download("https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip", options, (err) => {
                //if (err) throw err;
                //console.log("downloaded zip");
                dbConnection().then(
                    db => db.dropDatabase()).then(() => {
                        //decompressZip();
                        processFile();
                    });
            //});
            fulfill({ status: 'initializing' });
        } catch (ex) {
            reject(ex.message);
        }
    })
}

function decompressZip() {
    fs.createReadStream('./downloads/rdf-files.tar.zip')
        .pipe(unzip.Parse())
        .on('entry', entry => {
            status = entry.pipe(fs.createWriteStream('./downloads/rdf-files.tar'));
            status.on('finish', () => processFile())
        });
}

function processFile() {
    fs.createReadStream('./downloads/MY-rdf-files.tar.zip')
        .pipe(unzip.Parse())
        .on('entry', entry => {
            if (entry.type === "File") { // 'Directory' or 'File'
                id = entry.path.substring(entry.path.lastIndexOf('/') + 1);
                status = entry.pipe(fs.createWriteStream('./downloads/' + id + '.xml'));
                status.on('finish', () => {
                    book = getBookDetails('./downloads/' + id + '.xml', id);
                    if (book) {
                        booksDB.addBook(book._id, book.title, book.url).then(() => {
                            console.log('added to DB: ' + book._id);
                            //books.push(book);
                            //fs.unlinkSync('./downloads/' + id + '.xml');
                        });                        
                    }                    
                });
            } else {
                entry.autodrain();
            }
        });
}

function getBookDetails(fileLoc, id) {
    xmlStr = fs.readFileSync(fileLoc, 'utf8');
    if (xmlStr.indexOf('<dcterms:title>') != -1) {
        book = { _id: "", title: '', url: '' };
        book._id = id;
        book.title = xmlStr.substring(xmlStr.indexOf('<dcterms:title>') + '<dcterms:title>'.length, xmlStr.indexOf('</dcterms:title>'));
        rdf.parse(xmlStr, store, 'http://localhost', 'application/rdf+xml');
        stms = store.statementsMatching(undefined, undefined, undefined);
        for (var i = 0; i < stms.length; i++) {
            if (stms[i].subject && stms[i].subject.value.endsWith('htm')) {
                book.bookUrl = stms[i].subject.value;
            }
            if (stms[i].subject && stms[i].subject.value.indexOf('html') != -1) {
                book.bookUrl = stms[i].subject.value;
            }
        }
        return book;
    }
    return;
}