"use strict";

var AppComponent = function AppComponent() {
    return React.createElement(
        "div",
        { className: "row" },
        React.createElement(
            "div",
            { className: "col-sm-8" },
            React.createElement(RecipeContainer, { url: "/recipes" })
        ),
        React.createElement(
            "div",
            { className: "col-sm-4" },
            React.createElement(CommentContainer, null)
        )
    );
};
"use strict";

var CommentContainer = React.createClass({
    displayName: "CommentContainer",
    getInitialState: function getInitialState() {
        return {
            comments: [],
            newComment: "",
            commenterName: "",
            error: ""
        };
    },
    loadComments: function loadComments() {
        return $.get("/comments");
    },
    addComment: function addComment(commenter, comment) {
        return $.ajax({
            url: "/comments",
            dataType: "json",
            contentType: "application/json",
            method: "POST",
            data: JSON.stringify({
                comment: {
                    commenter: commenter, comment: comment
                }
            })
        });
    },
    componentDidMount: function componentDidMount() {
        var _this = this;

        this.loadComments().then(function (commentList) {
            _this.setState({ comments: commentList });
        });
    },
    handleCommentChange: function handleCommentChange(newText) {
        this.setState({ newComment: newText });
    },
    handleCommentSubmission: function handleCommentSubmission(newComment, newCommenterName) {
        var _this2 = this;

        if (!newComment) {
            this.setState({ error: "No comment provided" });
            return;
        };

        if (!newCommenterName) {
            this.setState({ error: "No comment name provided" });
            return;
        };

        var commentList = this.state.comments;
        var currentlyMatchingComment = commentList.filter(function (commentData) {
            return commentData.comment === newComment && commentData.commenter === newCommenterName;
        });

        if (currentlyMatchingComment.length > 0) {
            this.setState({ error: "No spamming allowed" });
            return;
        }

        this.setState({ error: "" });

        this.setState({
            newComment: ""
        });

        this.addComment(newCommenterName, newComment).then(function (newCommentObject) {
            _this2.setState({
                comments: _this2.state.comments.concat(newCommentObject)
            });
        }, function (error) {
            _this2.setState({ error: JSON.stringify(error) });
        });
    },
    handleCommentNameChange: function handleCommentNameChange(newCommenterName) {
        if (!newCommenterName) return;

        this.setState({
            commenterName: newCommenterName
        });
    },
    render: function render() {
        return React.createElement(
            "div",
            null,
            React.createElement(CommentList, { comments: this.state.comments }),
            React.createElement(CommentForm, {
                commenterName: this.state.commenterName,
                comment: this.state.newComment,
                onCommentChange: this.handleCommentChange,
                onCommentSubmit: this.handleCommentSubmission,
                onCommenterNameChange: this.handleCommentNameChange,
                formError: this.state.error
            })
        );
    }
});
"use strict";

var RecipeContainer = React.createClass({
    displayName: "RecipeContainer",

    // getInitialState() {
    //     return {recipes: []};        
    // },
    // componentDidMount() {        
    //     $.ajax({
    //         url: this.props.url,
    //         dataType: 'json',
    //         cache: false,
    //         success: (recipeList) => {
    //             this.setState({recipes: recipeList});
    //         },
    //         error: (xhr, status, err) => {
    //             console.error(this.props.url, status, err.toString());
    //         }
    //     });         
    // },
    render: function render() {
        return React.createElement(
            "div",
            { className: "recipe" },
            React.createElement(RecipeForm, null)
        );
    }
});
"use strict";

var Recipe = function Recipe(_ref) {
  var steps = _ref.steps,
      title = _ref.title,
      description = _ref.description,
      ingredients = _ref.ingredients;

  var viewSteps = steps.map(function (step) {
    return React.createElement(
      "li",
      null,
      step
    );
  });

  var viewIngredients = ingredients.map(function (ingredient) {
    return React.createElement(
      "li",
      null,
      ingredient
    );
  });

  return React.createElement(
    "div",
    { className: "panel panel-default" },
    React.createElement(
      "div",
      { className: "panel-heading" },
      title
    ),
    React.createElement(
      "div",
      { className: "panel-body" },
      React.createElement(
        "div",
        null,
        React.createElement(
          "p",
          null,
          description
        ),
        React.createElement(
          "div",
          { className: "row" },
          React.createElement(
            "div",
            { className: "col-md-8" },
            React.createElement(
              "ol",
              null,
              viewSteps
            )
          ),
          React.createElement(
            "div",
            { className: "col-sm-4" },
            React.createElement(
              "ul",
              null,
              viewIngredients
            )
          )
        )
      )
    )
  );
};
"use strict";

var RecipeForm = React.createClass({
    displayName: "RecipeForm",
    getInitialState: function getInitialState() {
        return {
            recipes: [],
            title: "",
            description: "",
            steps: [], newStep: "",
            ingredients: [], newIngredient: "",
            error: null
        };
    },
    componentDidMount: function componentDidMount() {
        var _this = this;

        $.get("/recipes").then(function (recipeList) {
            console.log(recipeList);
            _this.setState({ recipes: recipeList });
        }, function (error) {
            _this.setState({ error: JSON.stringify(error) });
        });
    },
    changeTitle: function changeTitle(e) {
        this.setState({ title: e.target.value });
    },
    changeDescription: function changeDescription(e) {
        this.setState({ description: e.target.value });
    },
    addIngredient: function addIngredient(e) {
        if (!this.state.newIngredient) {
            this.setState({ error: "Ingredient is missing !!" });
            return;
        }

        var ingredients = this.state.ingredients.concat([this.state.newIngredient]);

        this.setState({ ingredients: ingredients, newIngredient: "", error: null });
    },
    changeNewIngredientText: function changeNewIngredientText(e) {
        this.setState({ newIngredient: e.target.value });
    },
    addStep: function addStep(e) {
        if (!this.state.newStep) {
            this.setState({ error: "Step is missing !!" });
            return;
        }

        var steps = this.state.steps.concat([this.state.newStep]);

        this.setState({ steps: steps, newStep: "", error: null });
    },
    changeNewStepText: function changeNewStepText(e) {
        this.setState({ newStep: e.target.value });
    },
    addRecipe: function addRecipe(e) {
        var _this2 = this;

        if (!this.state.title) {
            this.setState({ error: "Title is missing !!" });
            return;
        }
        var recipeList = this.state.recipes;
        var currentlyMatchingRecipe = recipeList.filter(function (recipe) {
            return recipe.title === _this2.state.title;
        });
        if (currentlyMatchingRecipe.length > 0) {
            this.setState({ error: "Duplicate Recipe titles not allowed !!" });
            return;
        }
        if (!this.state.description) {
            this.setState({ error: "Description is missing !!" });
            return;
        }
        if (this.state.ingredients.length == 0) {
            this.setState({ error: "ingredients are missing !!" });
            return;
        }
        if (this.state.steps.length == 0) {
            this.setState({ error: "Steps are missing !!" });
            return;
        }

        this.addToServer().then(function (newRecipe) {
            _this2.setState({
                recipes: _this2.state.recipes.concat(newRecipe),
                title: "",
                description: "",
                steps: [], newStep: "",
                ingredients: [], newIngredient: "",
                error: null
            });
        }, function (error) {
            _this2.setState({ error: JSON.stringify(error) });
        });
        // alert(JSON.stringify(this.state));
    },
    addToServer: function addToServer() {
        return $.ajax({
            url: "/recipes",
            dataType: "json",
            contentType: "application/json",
            method: "POST",
            data: JSON.stringify({
                recipe: {
                    "title": this.state.title,
                    "description": this.state.description,
                    "ingredients": this.state.ingredients,
                    "steps": this.state.steps
                }
            })
        });
    },
    render: function render() {
        var newTitleText = "New Recipe: " + (this.state.title || '') + " (" + this.state.ingredients.length + " ingredients, " + this.state.steps.length + " steps)";
        var visibleFormError = undefined;
        if ("" + this.state.error != 'null') {
            visibleFormError = React.createElement(
                "div",
                { className: "alert alert-danger" },
                this.state.error
            );
        }

        return React.createElement(
            "div",
            { className: "recipe" },
            React.createElement(RecipeList, { recipes: this.state.recipes }),
            React.createElement("hr", null),
            React.createElement(
                "h3",
                null,
                "Add a New Recipe:"
            ),
            React.createElement(
                "div",
                { className: "form-horizontal" },
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "label",
                        { htmlFor: "newTitle", className: "col-sm-3 control-label" },
                        "Title"
                    ),
                    React.createElement(
                        "div",
                        { className: "col-sm-9" },
                        React.createElement("input", {
                            className: "form-control",
                            id: "newTitle",
                            placeholder: "New Recipe",
                            onChange: this.changeTitle,
                            value: this.state.title,
                            type: "text" })
                    )
                ),
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "label",
                        { htmlFor: "newDescription", className: "col-sm-3 control-label" },
                        "Description"
                    ),
                    React.createElement(
                        "div",
                        { className: "col-sm-9" },
                        React.createElement("input", {
                            className: "form-control",
                            id: "newDescription",
                            placeholder: "Recipe description",
                            onChange: this.changeDescription,
                            value: this.state.description,
                            type: "text" })
                    )
                ),
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "label",
                        { htmlFor: "newIngredientText", className: "col-sm-3 control-label" },
                        "New Ingredient"
                    ),
                    React.createElement(
                        "div",
                        { className: "col-sm-9" },
                        React.createElement(
                            "div",
                            { className: "input-group" },
                            React.createElement("input", {
                                className: "form-control",
                                type: "text",
                                id: "newIngredientText",
                                placeholder: "New Ingredient",
                                value: this.state.newIngredient,
                                onChange: this.changeNewIngredientText }),
                            React.createElement(
                                "span",
                                { className: "input-group-btn" },
                                React.createElement(
                                    "button",
                                    { className: "btn btn-primary", type: "button", onClick: this.addIngredient },
                                    "Add Ingredient"
                                )
                            )
                        )
                    )
                ),
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "label",
                        { htmlFor: "newStepText", className: "col-sm-3 control-label" },
                        "New Step"
                    ),
                    React.createElement(
                        "div",
                        { className: "col-sm-9" },
                        React.createElement("input", {
                            className: "form-control",
                            type: "text",
                            id: "newStepText",
                            placeholder: "New Step Instructions",
                            value: this.state.newStep,
                            onChange: this.changeNewStepText })
                    )
                ),
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "div",
                        { className: "col-sm-offset-3 col-sm-9" },
                        React.createElement(
                            "button",
                            { className: "btn btn-primary", type: "button", onClick: this.addStep },
                            "Add Step"
                        )
                    )
                ),
                React.createElement(
                    "div",
                    { className: "form-group" },
                    React.createElement(
                        "div",
                        { className: "col-sm-12" },
                        React.createElement(
                            "button",
                            { type: "submit", className: "btn btn-default", onClick: this.addRecipe },
                            "Add Recipe"
                        )
                    )
                )
            ),
            visibleFormError,
            React.createElement(Recipe, {
                title: newTitleText,
                description: this.state.description,
                steps: this.state.steps,
                ingredients: this.state.ingredients })
        );
    }
});
"use strict";

var CommentForm = function CommentForm(_ref) {
    var comment = _ref.comment,
        onCommentChange = _ref.onCommentChange,
        onCommentSubmit = _ref.onCommentSubmit,
        commenterName = _ref.commenterName,
        onCommenterNameChange = _ref.onCommenterNameChange,
        formError = _ref.formError;


    var visibleFormError = formError ? React.createElement(
        "div",
        { className: "alert alert-danger" },
        formError
    ) : undefined;

    return React.createElement(
        "form",
        {
            onSubmit: function onSubmit(e) {
                e.preventDefault();
                onCommentSubmit(comment, commenterName);
            } },
        React.createElement(
            "div",
            { className: "form-group" },
            React.createElement(
                "label",
                { htmlFor: "comment", className: "input-control" },
                "Comment"
            ),
            React.createElement("input", {
                id: "comment",
                type: "text",
                value: comment,
                onChange: function onChange(e) {
                    onCommentChange(e.target.value);
                },
                className: "form-control" })
        ),
        React.createElement(
            "div",
            { className: "form-group" },
            React.createElement(
                "label",
                { htmlFor: "commenter", className: "input-control" },
                "Your Name"
            ),
            React.createElement("input", {
                id: "commenter",
                type: "text",
                value: commenterName,
                onChange: function onChange(e) {
                    onCommenterNameChange(e.target.value);
                },
                className: "form-control" })
        ),
        React.createElement(
            "div",
            { className: "form-group" },
            React.createElement(
                "button",
                { type: "submit", className: "btn btn-primary" },
                "Submit"
            )
        ),
        visibleFormError
    );
};
"use strict";

var CommentList = function CommentList(_ref) {
    var comments = _ref.comments;

    return React.createElement(
        "ul",
        { className: "list-unstyled" },
        comments.map(function (commentData) {
            return React.createElement(
                "li",
                null,
                "[",
                commentData.commenter,
                "]: ",
                commentData.comment
            );
        })
    );
};
"use strict";

var RecipeList = function RecipeList(_ref) {
    var recipes = _ref.recipes;

    return React.createElement(
        "div",
        null,
        recipes.map(function (recipe) {
            return React.createElement(Recipe, {
                key: recipe.id,
                title: recipe.title,
                description: recipe.description,
                id: recipe.id,
                steps: recipe.steps,
                ingredients: recipe.ingredients });
        })
    );
};
'use strict';

ReactDOM.render(React.createElement(AppComponent, null), document.getElementById('content'));