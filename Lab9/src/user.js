import React, { Component } from 'react';
import { getUser } from './utils/api';
import {
    Link
} from 'react-router-dom'

const CurrentUser = ({ user }) => (
    <div>
        <article>
            {user.error && <h3>Bad request: {user.error}</h3>}
            {!user.error && <h3>User ID: {user.id}</h3>}
            {!user.error && <h3>First Name: {user.first_name}</h3>}
            {!user.error && <h3>Last Name: {user.last_name}</h3>}
            {!user.error && <h3>Gender: {user.gender}</h3>}
            {!user.error && <h3>Email: {user.email}</h3>}
            {!user.error && <h3>IP Address: {user.ip_address}</h3>}
            
        </article>        
        <h5><Link to={`/`}>Back to Home</Link></h5>
    </div>
);

class User extends Component {
    constructor(props) {
        super(props);
        this.state = {
            user: undefined
        };
    }

    async componentDidMount() {
        const user = await getUser(this.props.match.params.id);
        console.log(user);
        this.setState({ user });
    }

    async componentWillReceiveProps(nextProps) {
    }

    render() {
        if (this.state.user === undefined) return <div>Loading...</div>;
        return <CurrentUser user={this.state.user} />
    }
}

export default User;