$(() => {
    const socket = io('http://localhost:3000/chat');
    let currentRoom = "";
    let isConnected = false;

    $("#uname").on("keyup", () => $.shouldSubmit());
    $("#search").on("keyup", () => $.shouldSubmit());
    $("#message").on("keyup", () => $.shouldSubmit());

    $("#room").on("change", () => {
        $.shouldSubmit();

        if (!isConnected) {
            currentRoom = $("#room").val();
            return;
        }

        if ($("#room").val() === currentRoom)
            return;

        $("#messages").empty();
        socket.emit("join-room", {
            new: $("#room").val(),
            previous: currentRoom
        });
    });

    // $('#btn').on('click', () => {
    $("#chat_form").submit(() => {
        if (!isConnected) {
            socket.emit('setup', { userName: $("#uname").val() });
            socket.emit('join-room', { new: currentRoom, previous: "" });

            $("#uname").prop('disabled', true);
            isConnected = true;
        }
        socket.emit('send-message', { room: currentRoom, userName: $("#uname").val(), message: $("#message").val(), search: $("#search").val() });
        $("#message").val('');
        $("#search").val('');
        $("#btn").prop('disabled', true);
        $("#message").focus();
        return false;
    });

    socket.on("joined-room", (roomId) => {
        currentRoom = roomId;
        $("#room").val(roomId);
    });

    socket.on('receive-message', (payload) => {
        $("#messages").append($('<li class="user">').text("USER: " + payload.userName + " MESSAGE:" + payload.message));
        if (payload.searchResult.error) {
            $("#messages").append($('<li>').text("ERROR: " + JSON.stringify(payload.searchResult.error)));
        }
        if (payload.searchResult.urls) {
            if (payload.searchResult.urls.length == 0) {
                $("#messages").append($('<li>').text("no search results returned !!"));
            } else {
                for (i = 0; i < payload.searchResult.urls.length; i++) {
                    target = payload.searchResult.urls[i];
                    $("#messages").append('<img src="' + target + '" alt="' + target + '">');
                }
            }
        }
        $("#messages").animate({ scrollTop: $("#messages").height() });
        if($("#volume").hasClass("glyphicon-volume-up")) {
            $('#ding')[0].play();
        }
    });

    socket.on("disconnect", () => {
        $("#chat_form input").prop("disabled", true);
        $("#chat_form select").prop("disabled", true);
        $("#messages").append($('<li>').text("DISCONNECTED FROM SERVER !!"));
    });

    $("#volume").on('click', () => {
        $("#volume").toggleClass("glyphicon glyphicon-volume-off glyphicon glyphicon-volume-up");
    });
})

$.shouldSubmit = () => {
    if ($('#uname').val() != '' && $("#room").val() != '' && $('#search').val() != '' && $('#message').val() != '')
        $("#btn").prop('disabled', false);
    else
        $("#btn").prop('disabled', true);
}