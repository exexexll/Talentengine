import ReactDOM from "react-dom/client";
import { App } from "./App";

// Send session cookies on same-origin ``/api`` calls (DigitalOcean / single-host deploy).
// Also surface auth expiry: our API returns ``401`` + ``{"detail":"Not authenticated"}`` when
// the session cookie is missing/invalid — notify the shell so it can return to the login screen.
const _origFetch = window.fetch.bind(window);
window.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  const merged: RequestInit =
    url.startsWith("/api") ? { ...init, credentials: "include" as RequestCredentials } : { ...init };
  const res = await _origFetch(input, merged);
  if (res.status !== 401 || typeof url !== "string" || !url.startsWith("/api")) {
    return res;
  }
  const exempt =
    url.startsWith("/api/auth/me") ||
    url.startsWith("/api/auth/login") ||
    url.startsWith("/api/auth/logout");
  if (exempt) {
    return res;
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) {
    try {
      const body = (await res.clone().json()) as { detail?: string };
      if (body?.detail === "Not authenticated") {
        window.dispatchEvent(new CustomEvent("figwork:session-expired"));
      }
    } catch {
      /* ignore */
    }
  }
  return res;
};

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
