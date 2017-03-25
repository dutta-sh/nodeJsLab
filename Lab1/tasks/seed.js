const dbConnection = require("../config/mongoConnection");
const data = require("../data/");
const tasks = data.tasks;

dbConnection().then(db => {
    return db.dropDatabase().then(() => {
        return dbConnection;
    }).then((db) => {
        return tasks.addTask("Make lab", "Make the first lab for CS-554. Maybe talk about dinosaurs in it, or something", 1, false);
    }).then((task) => {
        const id = task._id;

                return tasks.addComment(id, "Phil", "Considering lab about dinosaurs")
            .then(() => {
                return tasks.addComment(id, "Jason", "Don't do dinosaurs...");
            })
            .then(() => {
                return tasks.addComment(id, "Phil", "Maybe make lab about REST?");
            })
            .then(() => {
                return tasks.addComment(id, "Jason", "Definitely about REST.");
            })
            .then(() => {
                return tasks.addComment(id, "Phil", "Would dinosaurs agree with the idempotent standards of REST?");
            })
            .then(() => {
                return tasks.addComment(id, "Phil", "Do androids dream of electric velociraptors?");
            });
    }).then(() => {
        console.log("Done seeding database");
        db.close();
    });
}, (error) => {
    console.error(error);
});
