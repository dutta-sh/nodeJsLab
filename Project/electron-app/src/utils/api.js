import { processRequest } from './apiUtils';
const baseUrl = "http://localhost:3001";

export const getCatalogPage = (pageNum) => {
    if(!pageNum)
        return processRequest(`${baseUrl}/catalog/0`, "GET", {}, false);
    else
        return processRequest(`${baseUrl}/catalog/${pageNum}`, "GET", {}, false);
}

export const getLibraryPage = (pageNum) => {
    if(!pageNum)
        return processRequest(`${baseUrl}/library/0`, "GET", {}, false);
    else
        return processRequest(`${baseUrl}/library/${pageNum}`, "GET", {}, false);
}

export const getBook = (id) => {
    if (!id) return Promise.reject(new Error("ID required to get book"));
    
    return processRequest(`${baseUrl}/download/${id}`, "GET", {}, false);
}