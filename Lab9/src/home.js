import React, { Component } from 'react';
import { getPage } from './utils/api';
import {
    Link
} from 'react-router-dom'

const CurrentList = ({ pageData }) => (
    <div>
        {pageData.error && <h3>Bad request: {pageData.error}</h3>}
        {pageData.error && <h5><Link to={`/`}>Back to Home</Link></h5>}

        {!pageData.error && <h3>List of users</h3>}        
        {!pageData.error && pageData.users.map((user, index) => (
            <article key={index}>
                <header>
                    <h3><Link to={`/user/${user.id}`}>{user.name}</Link></h3>
                </header>
            </article>
        ))}
        <h5>
            {pageData.currPage === 1 ? <Link to={`/`}>&lt;&lt;Prev page</Link> : pageData.currPage > pageData.minPage && <Link to={`/archive/${pageData.currPage - 1}`}>&lt;&lt;Prev page</Link>}
            &nbsp;&nbsp;&nbsp;
            {pageData.currPage < pageData.maxPage && <Link to={`/archive/${pageData.currPage + 1}`}>Next page&gt;&gt;</Link>}
        </h5>
    </div>
);

class Home extends Component {
    constructor(props) {
        super(props);
        this.state = {
            pageData : undefined
        };
    }

    async componentDidMount() {
        const pageData = await getPage(this.props.match.params.page);
        console.log(pageData);
        this.setState({ pageData });
    }

    async componentWillReceiveProps(nextProps) {
        const pageData = await getPage(nextProps.match.params.page);
        console.log(pageData);
        this.setState({ pageData });
    }

    render() {
        if (this.state.pageData === undefined) return <div>Loading...</div>;
        return <CurrentList pageData={this.state.pageData} />
    }
}

export default Home;