import React, { Component } from 'react';
import { getLibraryPage } from './utils/api';
import {
    Link
} from 'react-router-dom'

const CurrentList = ({ pageData }) => (
    <div>
        {pageData.error && <h3>Bad request: {pageData.error}</h3>}
        {!pageData.error && Number(pageData.currPage) > 0 && <h5><Link to={`/library`}>Back to Home</Link></h5>}

        {!pageData.error && <h3>My Library:</h3>}
        {!pageData.error && pageData.books.map((book, index) => (
            <article key={index}>
                <header>
                    <h4><Link to={`/download/${book._id}`}>{book.title}</Link></h4>
                </header>
            </article>
        ))}
        <h5>
            {Number(pageData.currPage) > Number(pageData.minPage) && <Link to={`/library/${Number(pageData.currPage) - 1}`}>&lt;&lt;Prev page</Link>}
            &nbsp;&nbsp;&nbsp;
            {Number(pageData.currPage) < Number(pageData.maxPage) && <Link to={`/library/${Number(pageData.currPage) + 1}`}>Next page&gt;&gt;</Link>}
        </h5>
        {!pageData.error && <h5>Page {Number(pageData.currPage) + 1} of {Number(pageData.maxPage) + 1}</h5>}
    </div>
);

class Library extends Component {
    constructor(props) {
        super(props);
        this.state = {
            pageData : undefined
        };
    }

    async componentDidMount() {
        const pageData = await getLibraryPage(this.props.match.params.page);
        console.log(pageData);
        this.setState({ pageData });
    }

    async componentWillReceiveProps(nextProps) {
        const pageData = await getLibraryPage(nextProps.match.params.page);
        console.log(pageData);
        this.setState({ pageData });
    }

    render() {
        if (this.state.pageData === undefined) return <div>Loading...</div>;
        return <CurrentList pageData={this.state.pageData} />
    }
}

export default Library;