import React from 'react';
import ReactDOM from 'react-dom';
import {
  BrowserRouter as Router,
  Route,
  Link
} from 'react-router-dom'
import Catalog from './catalog';
import Library from './library';
import Book from './book';
import logo from './logo.svg';
import './index.css';

const App = () => (
  <Router>
    <div className="App">
      <div className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <h2>Gutenberg Search SPA</h2>
        <nav>
          <ul>
            <li><Link to="/catalog">Gutenberg Catalog</Link></li>
            <li><Link to="/library">My Library</Link></li>
          </ul>
        </nav>

      </div>
      <div className="App-body">
        <Route exact path="/" component={Catalog} />

        <Route exact path="/catalog" component={Catalog} />
        <Route exact path="/library" component={Library} />
        <Route path="/catalog/:page" component={Catalog} />        
        <Route path="/library/:page" component={Library} />
        <Route path="/download/:id" component={Book} />
      </div>
    </div>
  </Router>
)

ReactDOM.render(
  <App />,
  document.getElementById('root')
);