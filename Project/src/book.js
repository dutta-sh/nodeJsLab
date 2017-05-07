import './book.css';
import React, { Component } from 'react';
import { getBook } from './utils/api';
import {
    Link
} from 'react-router-dom'

const CurrentBook = ({ book, contentUrl }) => (
    <div>
        <article>            
            {<h3>{book.title}</h3>}            
        </article>
        <main>
            <iframe src={contentUrl} />
        </main>
        <h5>
            <Link to={`/catalog`}>Back to Catalog</Link>
            &nbsp;&nbsp;&nbsp;
            <Link to={`/library`}>Back to Library</Link>
        </h5>
    </div>
);

class Book extends Component {
    constructor(props) {
        super(props);
        this.state = {
            book: undefined,
            bookBlobUrl: undefined
        };
    }

    async componentDidMount() {
        const book = await getBook(this.props.match.params.id);
        const blob = new Blob([book.content], { type: 'text/html' });
        const bookBlobUrl = window.URL.createObjectURL(blob);
        this.setState({ book, bookBlobUrl });
    }

    async componentWillReceiveProps(nextProps) {
        const book = await getBook(this.props.match.params.id);  
        const blob = new Blob([book.content], { type: 'text/html' });
        const bookBlobUrl = window.URL.createObjectURL(blob);      
        this.setState({ book, bookBlobUrl });
    }

    render() {
        if (this.state.book === undefined) return <div>Loading...</div>;
        return <CurrentBook book={this.state.book} contentUrl={this.state.bookBlobUrl}/>
    }
}

export default Book;