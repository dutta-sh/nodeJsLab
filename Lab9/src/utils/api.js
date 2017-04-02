import { processRequest } from './apiUtils';
const baseUrl = "http://localhost:3001";

export const getPage = (pageNum) => {
    if(!pageNum)
        return processRequest(`${baseUrl}/page/0`, "GET", {}, false);
    else
        return processRequest(`${baseUrl}/page/${pageNum}`, "GET", {}, false);
}

export const getUser = (id) => {
    if (!id) return Promise.reject(new Error("ID required to get user"));

    return processRequest(`${baseUrl}/person/${id}`, "GET", {}, false);
}