const mongoCollections = require("../config/mongoCollections");
const tasks = mongoCollections.tasks;
const uuid = require('node-uuid');

let exportedMethods = {
    getAllTasks() {
        return tasks().then((taskCollection) => {
            return taskCollection.find({}).toArray();
        });
    },
    getTaskById(id) {
        return tasks().then((taskCollection) => {
            return taskCollection.findOne({ _id: id }).then((task) => {
                if (!task) throw "Task not found";
                return task;
            });
        });
    },
    addTask(title, description, hoursEstimated, completed) {
        return tasks().then((taskCollection) => {
            let newTask = {
                title: title,
                description: description,
                hoursEstimated: hoursEstimated,
                completed: completed,
                _id: uuid.v4()
            };

            return taskCollection.insertOne(newTask).then((newInsert) => {
                return newInsert.insertedId;
            }).then((newId) => {
                return this.getTaskById(newId);
            });
        });
    },
    removeTask(id) {
        return tasks().then((taskCollection) => {
            return taskCollection.removeOne({ _id: id }).then((deletionInfo) => {
                if (deletionInfo.deletedCount === 0) {
                    throw (`Could not delete Task with id of ${id}`)
                }
            });
        });
    },
    updateTask(id, title, description, hoursEstimated, completed) {
        return tasks().then((taskCollection) => {
            let updatedPatch = {
                title: title,
                description: description,
                hoursEstimated: hoursEstimated,
                completed: completed
            };

            let updatedTask = {
                $set: updatedPatch
            };

            return taskCollection.updateOne({ _id: id }, updatedTask).then(() => {
                return this.getTaskById(id);
            });
        });
    },
    updateTaskWithPatch(id, title, description, hoursEstimated, completed) {
        return tasks().then((tasksCollection) => {
            let updatedPatch = {};

            if (title) {
                updatedPatch.title = title;
            }
            if (description) {
                updatedPatch.description = description;
            }
            if (hoursEstimated) {
                updatedPatch.hoursEstimated = hoursEstimated;
            }
            if (completed) {
                updatedPatch.completed = completed;
            }

            let updatedTask = {
                $set: updatedPatch
            };

            return tasksCollection.updateOne({ _id: id }, updatedTask).then(() => {
                return this.getTaskById(id);
            });
        });
    },
    addComment(taskId, name, comment) {
        return tasks().then((tasksCollection) => {
            let addComment = {
                $push: {
                    comments: {
                        id: uuid.v4(),
                        name: name,
                        comment: comment
                    }
                }
            };

            return tasksCollection.updateOne({ _id: taskId }, addComment).then(() => {
                return this.getTaskById(taskId);
            });
        });
    },
    deleteComment(taskId, commentId) {
        return tasks().then((tasksCollection) => {
            let deleteCom = {
                $pull: {
                    comments:
                    {
                        id: commentId
                    }
                }
            };
            
            return tasksCollection.update({ _id: taskId }, deleteCom).then(() => {
                return this.getTaskById(taskId);
            });
        });
    }
}

module.exports = exportedMethods;