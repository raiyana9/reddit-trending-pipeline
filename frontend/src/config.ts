const env = import.meta.env;

const defaultApi = `${window.location.protocol}//${window.location.hostname}:8000`;
const defaultWs = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.hostname}:8000/ws/trends`;

export const API_URL: string = (env.VITE_API_URL as string | undefined) || defaultApi;
export const WS_URL: string = (env.VITE_WS_URL as string | undefined) || defaultWs;
export const POSTS_WS_URL: string = WS_URL.replace(/\/ws\/trends.*$/, "/ws/posts");
