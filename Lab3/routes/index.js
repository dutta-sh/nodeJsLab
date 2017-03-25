const path = require('path');

const constructorMethod = (app) => {
    app.use("/", (req, res) => {
        if(req.path == '/') {
            let route = path.resolve(`static/about.html`);
            res.sendFile(route);
        } else {
            res.status(404).json({error: "Not found"});
        }
        
    });
};

module.exports = constructorMethod;