const express = require('express');
const router = express.Router();
const data = require("../data");
const taskData = data.tasks;

var getAllTasksCount = 0;
var getOneTaskCount = 0;
var postTaskCount = 0;
var putTaskCount = 0;
var patchTaskCount = 0;
var postCommentCount = 0;
var deleteCommentCount = 0;

router.get("/", (req, res) => {
    console.log("Request Count: " + ++getAllTasksCount);
    taskData.getAllTasks().then((taskList) => {
        res.json(taskList);
    }, (error) => {
        res.status(500).json({ "error" : error});
    });
});

router.get("/:id", (req, res) => {
    console.log("Request Count: " + ++getOneTaskCount);
    taskData.getTaskById(req.params.id).then((task) => {
        res.json(task);
    }, (error) => {
        res.status(400).json({ "error" : error});
    });
});

router.post("/", (req, res) => {
    console.log("Request Count: " + ++postTaskCount);
    let task = req.body;
    if(!task || !task.title || !task.description || !task.hoursEstimated || task.completed === null) {  
        res.status(400).json({ "error" : "post body is missing" });
        return;
    }

    taskData.addTask(task.title, task.description, task.hoursEstimated, task.completed).then((task) => {
            res.json(task);
        }).catch((e) => {
            console.log(e);
            res.status(500).json({ "error" : e });
        });
});

router.put("/:id", (req, res) => {
    console.log("Request Count: " + ++putTaskCount);
    let task = req.body;
    let getTask = taskData.getTaskById(req.params.id);

    if(!getTask) {  
        res.status(400).json({ "error" : "invalid task id" });
        return;
    }

    if(!task || !task.title || !task.description || !task.hoursEstimated || task.completed === null) {  
        res.status(400).json({ "error" : "post body is missing" });
        return;
    }

    getTask.then(() => {
        return taskData.updateTask(req.params.id, task.title, task.description, task.hoursEstimated, task.completed)
            .then((task) => {
                res.json(task);
            }).catch((e) => {
                console.log(e);
                res.status(400).json({ error: e });
            });
    }).catch((e) => {
        console.log(e);
        res.status(404).json({ error: "Task not found" });
    });
});

router.patch("/:id", (req, res) => {
    console.log("Request Count: " + ++patchTaskCount);
    let task = req.body;
    let getTask = taskData.getTaskById(req.params.id);

    if(!getTask) {  
        res.status(400).json({ "error" : "invalid task id" });
        return;
    }

    getTask.then(() => {
        return taskData.updateTaskWithPatch(req.params.id, task.title, task.description, task.hoursEstimated, task.completed)
            .then((task) => {
                res.json(task);
            }).catch((e) => {
                console.log(e);
                res.status(500).json({ error: e });
            });
    }).catch((e) => {
        console.log(e);
        res.status(404).json({ error: "Task not found" });
    });
});


router.post("/:id/comments", (req, res) => {
    console.log("Request Count: " + ++postCommentCount);
    let comments = req.body;
    let getTask = taskData.getTaskById(req.params.id);

    if(!getTask) {  
        res.status(400).json({ "error" : "invalid task id" });
        return;
    }

    if(!comments || !comments.name || !comments.comment) {  
        res.status(400).json({ "error" : "post body is missing" });
        return;
    }

    getTask.then(() => {
        return taskData.addComment(req.params.id, comments.name, comments.comment)
            .then((comments) => {
                res.json(comments);
            }).catch((e) => {
                console.log(e);
                res.status(500).json({ error: e });
            });
    }).catch((e) => {
        console.log(e);
        res.status(404).json({ error: "Task not found" });
    });
});

router.delete("/:taskId/:commentId", (req, res) => {
    console.log("Request Count: " + ++deleteCommentCount);
    let task = req.body;
    let getTask = taskData.getTaskById(req.params.taskId);

    if(!getTask) {  
        res.status(400).json({ "error" : "invalid task id" });
        return;
    }
    
    getTask.then(() => {
        return taskData.deleteComment(req.params.taskId, req.params.commentId)
            .then((task) => {
                res.json(task);
            }).catch((e) => {
                console.log(e);
                res.status(500).json({ error: e });
            });
    }).catch((e) => {
        console.log(e);
        res.status(400).json({ error: "Task and/or Comment not found!" });
    });
});

module.exports = router;