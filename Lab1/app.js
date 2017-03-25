const express = require("express");
const bodyParser = require("body-parser");
let app = express();
let configRoutes = require("./routes");


var allPathsAccessed = {};
app.use(bodyParser.json());
app.use(function (req, res, next) {
    console.log("=========================================");
    console.log("Route:" + req.path);
    console.log("HTTP verb:" + req.method);
    console.log("HTTP Body:");
    console.log(req.body);
    next();
});

configRoutes(app);

app.listen(3000, () => {
    console.log("We've now got a server!");
    console.log("Your routes will be running on http://localhost:3000");
});