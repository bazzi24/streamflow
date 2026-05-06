import axios from "axios";

const API_BASE = import.meta.env.VITE_API_URL || "/api/v1";

const client = axios.create({
  baseURL: API_BASE,
  headers: { "Content-Type": "application/json" },
  withCredentials: true, // Send httpOnly cookies automatically
});

// Request interceptor: add Authorization header from sessionStorage
client.interceptors.request.use(
  (config) => {
    const token = sessionStorage.getItem("access_token");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for token refresh on 401
client.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalConfig = error.config;

    // Only handle 401 errors that haven't been retried yet
    if (error.response?.status === 401 && !originalConfig._retry) {
      originalConfig._retry = true;

      try {
        // Attempt to refresh the token using the httpOnly refresh cookie
        const refreshResponse = await client.post("/auth/refresh");
        const newToken = refreshResponse.data.access_token;

        // Store the new access token in sessionStorage for WebSocket usage
        if (newToken) {
          sessionStorage.setItem("access_token", newToken);
        }

        // Retry the original request
        return client(originalConfig);
      } catch (refreshError) {
        // Refresh failed - clear any stored tokens and redirect to login
        sessionStorage.removeItem("access_token");
        window.location.href = "/login";
        return Promise.reject(refreshError);
      }
    }

    return Promise.reject(error);
  }
);

export default client;
