const express = require("express");
const bodyParser = require("body-parser");
const dataModule = require("./dataModule");
const app = express();

app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Headers', req.get('Access-Control-Request-Headers'));
    next();
});

app.use(bodyParser.json());

app.get("/person/:id", (req, res) => {
    dataModule.getById(req.params.id).
        then((person) => {
        res.json(person);
    }, (error) => {
        res.status(400).json({ "error" : error});
    });   
});

app.get("/page/:num", (req, res) => {
    dataModule.getByPage(req.params.num).
        then((person) => {
        res.json(person);
    }, (error) => {
        res.status(400).json({ "error" : error});
    });   
});

// app.use("*", (req, res) => {
//     res.status(404).json({ error: "URL not found" });
// })

app.listen(3001, () => {  
    console.log("We've now got a server!");
    console.log("Your routes will be running on http://localhost:3001");
});