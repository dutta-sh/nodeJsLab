import React from 'react';
import ReactDOM from 'react-dom';
import {
  BrowserRouter as Router,
  Route
} from 'react-router-dom'
import Home from './home';
import User from './user';
import logo from './logo.svg';
import './index.css';

const App = () => (
  <Router>
    <div className="App">
      <div className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <h2>API Driven People Navigator</h2>        

      </div>
      <div className="App-body">
        <Route exact path="/" component={Home} />
        <Route path="/archive/:page" component={Home} />
        <Route path="/user/:id" component={User} />
      </div>
    </div>
  </Router>
)

ReactDOM.render(
  <App />,
  document.getElementById('root')
);