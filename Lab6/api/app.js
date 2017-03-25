const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const redisConnection = require("./redis-connection");
const nrpSender = require("./nrp-sender-shim");

app.use(bodyParser.json());

app.get("/api/people/:id", (req, res) => request(req, res, 'get-person'));
app.post("/api/people", (req, res) => request(req, res, 'add-person')); 
app.delete("/api/people/:id", (req, res) => request(req, res, 'delete-person'));
app.put("/api/people/:id", (req, res) => request(req, res, 'update-person'));

app.use("*", (req, res) => {
    res.status(404).json({error: "Not found"});
});

app.listen(3000, () => {
    console.log("We've now got a server!");
    console.log("Your routes will be running on http://localhost:3000");
});

async function request(req, res, event) {
    try {
        let dataVal = req.body.message;
        if(event == 'get-person' || event == 'delete-person') 
            dataVal = req.params.id;
        if(event == 'update-person')
            dataVal.id = req.params.id;
        
        let response = await nrpSender.sendMessage({
            redis: redisConnection,
            eventName: event,
            data: dataVal
        });

        res.json(response);
    } catch (e) {        
        res.json({ error: e });
    }
}