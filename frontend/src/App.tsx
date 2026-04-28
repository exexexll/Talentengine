import React from "react";
import { LoginScreen } from "./auth/LoginScreen";

const MapDashboard = React.lazy(() =>
  import("./pages/map-dashboard").then(m => ({ default: m.MapDashboard })),
);
const RankingsPage = React.lazy(() =>
  import("./pages/rankings").then(m => ({ default: m.RankingsPage })),
);
const SdrWorkspace = React.lazy(() =>
  import("./pages/sdr-workspace").then(m => ({ default: m.SdrWorkspace })),
);
const WorkTriggerAnalyticsPage = React.lazy(() =>
  import("./pages/worktrigger-analytics").then(m => ({ default: m.WorkTriggerAnalyticsPage })),
);

type View = "map" | "rankings" | "sdr" | "wt_analytics";

function pathToView(): View {
  const raw = window.location.pathname || "/";
  const p = raw.replace(/\/+$/, "") || "/";
  if (p === "/rankings" || p.endsWith("/rankings")) return "rankings";
  if (p === "/worktrigger/analytics" || p.endsWith("/worktrigger/analytics")) return "wt_analytics";
  if (p === "/sdr" || p.startsWith("/sdr") || p === "/worktrigger" || p.startsWith("/worktrigger")) return "sdr";
  return "map";
}

function RouteFallback(): JSX.Element {
  return (
    <div style={{
      display: "grid",
      placeItems: "center",
      minHeight: "100vh",
      background: "#f4f6f7",
      fontFamily: "'Avenir Next', 'Segoe UI', sans-serif",
      color: "#4d6b75",
      gap: 12,
    }}>
      <div style={{
        width: 28,
        height: 28,
        border: "3px solid #d0e3ea",
        borderTopColor: "#1a6b5a",
        borderRadius: "50%",
        animation: "app-spin 0.8s linear infinite",
      }} />
      <div style={{ fontSize: 13, letterSpacing: 0.4, textTransform: "uppercase", fontWeight: 600 }}>
        Loading Figwork…
      </div>
      <style>{`@keyframes app-spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

type AuthState =
  | { phase: "loading" }
  | { phase: "need_login" }
  | { phase: "ready"; username: string; displayName: string; authDisabled: boolean };

export function App(): JSX.Element {
  const [view, setView] = React.useState<View>(() => pathToView());
  const [auth, setAuth] = React.useState<AuthState>({ phase: "loading" });

  const refreshAuth = React.useCallback(async () => {
    try {
      const r = await fetch("/api/auth/me", { credentials: "include" });
      if (!r.ok) {
        setAuth({ phase: "need_login" });
        return;
      }
      const j = (await r.json()) as { username?: string; display_name?: string; auth_disabled?: boolean };
      if (j.auth_disabled) {
        setAuth({
          phase: "ready",
          username: j.username || "anonymous",
          displayName: j.display_name || "Anonymous",
          authDisabled: true,
        });
        return;
      }
      setAuth({
        phase: "ready",
        username: j.username || "",
        displayName: j.display_name || j.username || "",
        authDisabled: false,
      });
    } catch {
      setAuth({ phase: "need_login" });
    }
  }, []);

  React.useEffect(() => {
    void refreshAuth();
  }, [refreshAuth]);

  React.useEffect(() => {
    const onExpired = () => setAuth({ phase: "need_login" });
    window.addEventListener("figwork:session-expired", onExpired);
    return () => window.removeEventListener("figwork:session-expired", onExpired);
  }, []);

  React.useLayoutEffect(() => { setView(pathToView()); }, []);

  React.useEffect(() => {
    const onPop = () => setView(pathToView());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const navigate = React.useCallback((path: string) => {
    window.history.pushState({}, "", path);
    setView(pathToView());
  }, []);

  const logout = React.useCallback(async () => {
    try {
      await fetch("/api/auth/logout", { method: "POST", credentials: "include" });
    } catch { /* ignore */ }
    setAuth({ phase: "need_login" });
  }, []);

  React.useEffect(() => {
    const idle = (cb: () => void) =>
      (window as unknown as { requestIdleCallback?: (cb: () => void) => number })
        .requestIdleCallback?.(cb) ?? window.setTimeout(cb, 1500);
    idle(() => {
      if (view !== "sdr") void import("./pages/sdr-workspace");
      if (view !== "wt_analytics") void import("./pages/worktrigger-analytics");
      if (view !== "rankings") void import("./pages/rankings");
      if (view !== "map") void import("./pages/map-dashboard");
    });
  }, [view]);

  if (auth.phase === "loading") {
    return <RouteFallback />;
  }
  if (auth.phase === "need_login") {
    return <LoginScreen onLoggedIn={() => void refreshAuth()} />;
  }

  const showSessionBar = !auth.authDisabled && auth.username;

  let page: JSX.Element;
  if (view === "rankings") {
    page = <RankingsPage onBack={() => navigate("/")} />;
  } else if (view === "wt_analytics") {
    page = <WorkTriggerAnalyticsPage onBack={() => navigate("/sdr")} />;
  } else if (view === "sdr") {
    page = (
      <SdrWorkspace
        onBack={() => navigate("/")}
        onOpenMap={() => navigate("/")}
        onOpenWtAnalytics={() => navigate("/worktrigger/analytics")}
      />
    );
  } else {
    page = (
      <MapDashboard
        onOpenFullRankingsPage={() => navigate("/rankings")}
        onOpenSdr={() => navigate("/sdr")}
      />
    );
  }

  return (
    <>
      {showSessionBar ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            gap: 12,
            padding: "6px 14px",
            fontSize: 12,
            color: "#334155",
            background: "#f1f5f9",
            borderBottom: "1px solid #e2e8f0",
            fontFamily: "'Avenir Next', 'Segoe UI', system-ui, sans-serif",
          }}
        >
          <span>Signed in as <strong>{auth.displayName}</strong></span>
          <button
            type="button"
            onClick={() => void logout()}
            style={{
              border: "1px solid #cbd5e1",
              background: "#fff",
              borderRadius: 6,
              padding: "4px 10px",
              fontSize: 12,
              cursor: "pointer",
              color: "#475569",
            }}
          >
            Sign out
          </button>
        </div>
      ) : null}
      <React.Suspense fallback={<RouteFallback />}>{page}</React.Suspense>
    </>
  );
}
