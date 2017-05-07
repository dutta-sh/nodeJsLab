const redisConnection = require("./redis-connection");
const download = require('download-file');
const fs = require('fs');
const unzip = require('unzip');
const rdf = require('rdflib');
const store = rdf.graph();
const booksDB = require("../data/books");
const dbConnection = require("../data/mongoConnection");
const redis = require('redis');
const client = redis.createClient();

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

books = [];
function initialize() {
    client.flushdb( function (err, succeeded) {
        console.log('flushed redis cache: ' + succeeded);
    });
    console.log('downloading zip');
    //download("https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip", { directory: "./downloads/", filename: "rdf-files.tar.zip" }, (err) => {
        //if (err) throw err;
        console.log("downloaded zip");
        console.log('extracting zip'); 
        //fs.createReadStream('./downloads/rdf-files.tar.zip').pipe(unzip.Extract({ path: './downloads/' })).on('close', () => {                    
            processFile();                    
        //});           
    //});
    return {status: "initializing"};
}

function processFile() {
    if (!fs.existsSync('./downloads/extract/')){
        fs.mkdirSync('./downloads/extract/');
    }
    fs.createReadStream('./downloads/MY-rdf-files.tar.zip').pipe(unzip.Extract({ path: './downloads/extract/' })).on('close', () => {
        console.log('extracted zip');
        console.log('parsing extract');
        walkSync('./downloads/extract/');
        console.log('parsed extract');
        dbConnection().then(
            db => db.dropDatabase()).then(() => {
            booksDB.addBooks(books);
        });
    });
}

function walkSync(dir) {
    files = fs.readdirSync(dir);
    files.forEach(file => {
        if (fs.statSync(dir + file).isDirectory()) {
            filelist = walkSync(dir + file + '/');
        } else {
            xmlStr = fs.readFileSync(dir + file, 'utf8');
            //fs.unlinkSync(dir + file);
            book = getBookDetails(xmlStr, file);
            if (book) {
                books.push(book);
            }
        }
    });
};

function getBookDetails(xmlStr, id) {
    if (xmlStr.indexOf('<dcterms:title>') != -1) {
        try {
            book = { _id: "", title: '', url: '' };
            book._id = id;
            book.title = xmlStr.substring(xmlStr.indexOf('<dcterms:title>') + '<dcterms:title>'.length, xmlStr.indexOf('</dcterms:title>'));
            book.title = book.title.replace('&#13', ' ');
            rdf.parse(xmlStr, store, 'http://www.gutenberg.org', 'application/rdf+xml');
            stms = store.statementsMatching(undefined, undefined, undefined);
            for (var i = 0; i < stms.length; i++) {
                if (stms[i].subject && stms[i].subject.value.endsWith('htm')) {
                    book.url = stms[i].subject.value;
                }
                if (stms[i].subject && stms[i].subject.value.indexOf('html') != -1) {
                    book.url = stms[i].subject.value;
                }
            }
            return book;
        } catch (err) { return; }
    }
    return;
}