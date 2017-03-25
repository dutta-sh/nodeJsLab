const redisConnection = require("./redis-connection");
const querystring = require("querystring");
const request = require("request")

console.log('We have got a worker!!');

redisConnection.on('pixabay:request:*', async (message, channel) => {
    let payload = message.data;
    let response = await getFromPixabay(payload.search);
    let event = `${message.eventName}:success:${message.requestId}`;

    redisConnection.emit(event, {
        requestId: message.requestId,
        eventName: message.eventName,
        data: { userName: payload.userName, message: payload.message, searchResult: response }
    });
});

function getFromPixabay(search) {
    return new Promise((fulfill, reject) => {
        request({
            url: "http://pixabay.com/api/?" + querystring.stringify({ key: '4855353-8412fad6c6a1a015417df3a54', image_type: 'photo', pretty: 'true', q: search }),
            json: true
        }, (error, response, result) => {
            if (!error && response && response.statusCode === 200 && JSON.stringify(result).startsWith('{')) {              
                let urls = [];
                for (i = 0; i < result.hits.length; i++) {
                    urls[i] = result.hits[i].previewURL;
                }

                let formattedResponse = {};
                formattedResponse.hits = result.totalHits;
                formattedResponse.urls = urls;

                console.log(formattedResponse);
                fulfill(formattedResponse);
            } else {
                if (error)
                    fulfill({ "error": error });
                if (!response.statusCode === 200)
                    fulfill({ "error": "httpResponse: " + response.statusCode });

                fulfill({ "error": "unknown error" });
            }
        })
    })
}

process.on('SIGINT', () => {
    console.log("Shutting down worker");
    process.exit(0);
});