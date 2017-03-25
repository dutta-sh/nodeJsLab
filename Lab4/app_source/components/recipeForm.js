const RecipeForm = React.createClass({
    getInitialState() {
        return {
            recipes: [],
            title: "",
            description: "",
            steps: [], newStep: "",
            ingredients: [], newIngredient: "",
            error: null
        };
    },
    componentDidMount() {
        $.get("/recipes").then((recipeList) => {
            console.log(recipeList);
            this.setState({ recipes: recipeList });
        }, (error) => {
            this.setState({ error: JSON.stringify(error) });
        });
    },
    changeTitle(e) {        
        this.setState({ title: e.target.value });
    },
    changeDescription(e) {        
        this.setState({ description: e.target.value });
    },
    addIngredient(e) {        
        if(!this.state.newIngredient) {
            this.setState({ error: "Ingredient is missing !!" });
            return;
        }
        
        let ingredients = this
            .state
            .ingredients
            .concat([this.state.newIngredient]);

        this.setState({ ingredients: ingredients, newIngredient: "", error: null });
    },
    changeNewIngredientText(e) {        
        this.setState({ newIngredient: e.target.value });
    },
    addStep(e) {        
        if(!this.state.newStep) {
            this.setState({ error: "Step is missing !!" });
            return;           
        }
        
        let steps = this
            .state
            .steps
            .concat([this.state.newStep]);

        this.setState({ steps: steps, newStep: "", error: null });
    },
    changeNewStepText(e) {        
        this.setState({ newStep: e.target.value });
    },
    addRecipe(e) {
        if(!this.state.title) {
            this.setState({ error: "Title is missing !!" });
            return;
        }
        let recipeList = this.state.recipes;
        let currentlyMatchingRecipe = recipeList.filter(recipe => {
            return recipe.title === this.state.title;
        });
        if (currentlyMatchingRecipe.length > 0) {
            this.setState({ error: "Duplicate Recipe titles not allowed !!" });
            return;
        }
        if(!this.state.description) {
            this.setState({ error: "Description is missing !!" });
            return;
        }
        if(this.state.ingredients.length == 0) {
            this.setState({ error: "ingredients are missing !!" });
            return;
        }
        if(this.state.steps.length == 0) {
            this.setState({ error: "Steps are missing !!" });
            return;
        }        

        this.addToServer().then((newRecipe) => {
            this.setState({
                recipes: this.state.recipes.concat(newRecipe),
                title: "",
                description: "",
                steps: [], newStep: "",
                ingredients: [], newIngredient: "",
                error: null
            });
        }, (error) => {
            this.setState({ error: JSON.stringify(error) });
        });        
        // alert(JSON.stringify(this.state));
    },
    addToServer() {
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
            }),
        }); 
    },    
    render() {
        let newTitleText = `New Recipe: ${this.state.title || ''} (${this.state.ingredients.length} ingredients, ${this.state.steps.length} steps)`;
        let visibleFormError = undefined;
        if(`${this.state.error}` != 'null') {
            visibleFormError = <div className="alert alert-danger">{this.state.error}</div>;
        }
            
        return (
            <div className="recipe">
                <RecipeList recipes={this.state.recipes} />
                <hr />
                <h3>Add a New Recipe:</h3>
                <div className="form-horizontal">
                    <div className="form-group">
                        <label htmlFor="newTitle" className="col-sm-3 control-label">Title</label>
                        <div className="col-sm-9">
                            <input
                                className="form-control"
                                id="newTitle"
                                placeholder="New Recipe"
                                onChange={this.changeTitle}
                                value={this.state.title}
                                type="text" />
                        </div>
                    </div>
                    <div className="form-group">
                        <label htmlFor="newDescription" className="col-sm-3 control-label">Description</label>
                        <div className="col-sm-9">
                            <input
                                className="form-control"
                                id="newDescription"
                                placeholder="Recipe description"
                                onChange={this.changeDescription}
                                value={this.state.description}
                                type="text"/>
                        </div>
                    </div>
                    <div className="form-group">
                        <label htmlFor="newIngredientText" className="col-sm-3 control-label">New Ingredient</label>
                        <div className="col-sm-9">
                            <div className="input-group">
                                <input
                                    className="form-control"
                                    type="text"
                                    id="newIngredientText"
                                    placeholder="New Ingredient"
                                    value={this.state.newIngredient}
                                    onChange={this.changeNewIngredientText} />
                                <span className="input-group-btn">
                                    <button className="btn btn-primary" type="button" onClick={this.addIngredient}>Add Ingredient</button>
                                </span>
                            </div>
                        </div>
                    </div>
                    <div className="form-group">
                        <label htmlFor="newStepText" className="col-sm-3 control-label">New Step</label>
                        <div className="col-sm-9">
                            <input
                                className="form-control"
                                type="text"
                                id="newStepText"
                                placeholder="New Step Instructions"
                                value={this.state.newStep}
                                onChange={this.changeNewStepText} />
                        </div>
                    </div>
                    <div className="form-group">
                        <div className="col-sm-offset-3 col-sm-9">
                            <button className="btn btn-primary" type="button" onClick={this.addStep}>Add Step</button>
                        </div>
                    </div>
                    <div className="form-group">
                        <div className="col-sm-12">
                            <button type="submit" className="btn btn-default" onClick={this.addRecipe}>Add Recipe</button>
                        </div>
                    </div>
                </div>
                {visibleFormError}                
                <Recipe
                    title={newTitleText}
                    description={this.state.description}
                    steps={this.state.steps}
                    ingredients={this.state.ingredients} />
            </div>
        );
    }
});
