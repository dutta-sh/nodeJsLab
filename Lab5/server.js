const dataModule = require("./dataModule");
const cacheModule = require("./cacheModule");
const express = require("express");
const app = express();

app.get("/api/people/history", (req, res) => {
    cacheModule.fetchHistory().then((history) => {
        res.json(history);
    })
});

app.get("/api/people/:id", (req, res) => {
    id = Number(req.params.id);       
    
    cacheModule.fetchFromCache(id).
        then((person) => {
        cacheModule.addToHistory(id).
        then(res.json(person));
    }, (error) => {
        console.log(error.message);
        dataModule.getById(id).
            then((person) => {
            cacheModule.addToHistory(id).
            then(cacheModule.addToCache(person).
            then(res.json(person)));
        }, (error) => {            
            res.status(400).json({ "error" : error});
        });
    });    
});

app.use("*", (req, res) => {
    res.status(404).json({error: "Not found"});
});

app.listen(3000, () => {
    console.log("We've now got a server!");
    console.log("Your routes will be running on http://localhost:3000");
});