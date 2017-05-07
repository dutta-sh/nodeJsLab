const express = require("express");
const bodyParser = require("body-parser");
const redisConnection = require("./redis-connection");
const nrpSender = require("./nrp-sender-shim");
const booksDB = require("../data/books");
const libraryDB = require("../data/library");
const cacheModule = require("../data/cacheModule");
const app = express();
const xssFilters = require('xss-filters');

app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Headers', req.get('Access-Control-Request-Headers'));
    next();
});

app.use(bodyParser.json());

// app.get("/catalog/:page", (req, res) => {
//     booksDB.getBooksByPage(req.params.page).then((page) => {        
//         //page = '<h1> Hello <script>alert(1);</script> there !!</h1>';
//         //res.send(page);
//         //res.send(xssFilters.inHTMLData(page));
//     }, (error) => {
//         res.status(500).json({ "error": error });
//     });
// });

app.get("/catalog/:page", (req, res) => {  
    cacheModule.fetchFromCache(req.params.page).then((pageDetails) => (res.json(pageDetails)), (error) => {
        console.log(error.message);
        booksDB.getBooksByPage(req.params.page).then((pageDetails) => {            
            cacheModule.addToCache(pageDetails).then(() => {
                res.json(pageDetails);
            });
        }, (error) => {            
            res.status(500).json({ "error" : error});
        });
    });    
});

app.get("/library/:page", (req, res) => {
    libraryDB.getLibraryByPage(req.params.page).then((page) => {
        res.json(page);
    }, (error) => {
        res.status(500).json({ "error": error });
    });
});

app.get("/download/:id", (req, res) => {
    booksDB.downloadBook(req.params.id).then((book) => {
        res.json(book);
    }, (error) => {
        res.status(400).json({ "error": error });
    });
});

app.get("/initialize", (req, res) => request(req, res, 'initialize'));
async function request(req, res, event) {
    try {                
        let response = await nrpSender.sendMessage({
            redis: redisConnection,
            eventName: event,
            data: event
        });
        res.json(response);
    } catch (e) {        
        res.json({ error: e });
    }
}

// const https = require('https');
// const fs = require('fs');
// var https_options = {
//     key: fs.readFileSync('key.pem'),
//     cert: fs.readFileSync('cert.pem')
// };
//https.createServer(https_options, app).listen(3001, () => {
app.listen(3001, () => {
    console.log("We've now got a server!");
     console.log("Your routes will be running on http://localhost:3001");
});