const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);
const chat = io.of("/chat");
const bodyParser = require("body-parser");
const redisConnection = require("./redis-connection");
const nrpSender = require("./nrp-sender-shim");
const static = require('express').static(__dirname + '/static');

const usersToSocket = {};

app.use("/static", static);
app.use(bodyParser.json());

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/static/index.html');
});

chat.on('connection', (socket) => {
    socket.on('setup', (connectionInfo) => {
        console.log('setup: ' + JSON.stringify(connectionInfo));
        usersToSocket[connectionInfo.userName] = socket;
    });

    socket.on('join-room', (rooms) => {
        console.log('join-room: ' + JSON.stringify(rooms));
        socket.leave(rooms.previous);
        socket.join(rooms.new);

        socket.emit("joined-room", rooms.new);
    });

    socket.on('send-message', async (payload) => {
        console.log('server received: ' + JSON.stringify(payload));
        let response = '';
        try {
            response = await nrpSender.sendMessage({
                redis: redisConnection,
                eventName: 'pixabay',
                data: payload
            });
        } catch (e) {
            response = { userName: payload.userName, message: payload.message, searchResult: {error: e.message} };            
            console.log('sending error to UI: ' + JSON.stringify(response));
        }
        chat.to(payload.room).emit('receive-message', response);
    });
});

http.listen(3000, () => {
    console.log('We have got a server!! listening on http://localhost:3000');
});

process.on('SIGINT', () => {
    console.log("Shutting down chat server");
    process.exit(0);
});