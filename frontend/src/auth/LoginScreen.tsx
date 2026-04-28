import React from "react";

type Props = {
  onLoggedIn: () => void;
};

export function LoginScreen({ onLoggedIn }: Props): JSX.Element {
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErr(null);
    setBusy(true);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ username, password }),
      });
      if (!res.ok) {
        const raw = await res.text();
        let msg = raw || "Login failed";
        try {
          const j = JSON.parse(raw) as { detail?: string };
          if (typeof j.detail === "string") {
            msg = j.detail;
          }
        } catch {
          /* keep raw */
        }
        setErr(msg);
        return;
      }
      onLoggedIn();
    } catch {
      setErr("Network error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "grid",
        placeItems: "center",
        background: "linear-gradient(160deg, #0f172a 0%, #1e293b 45%, #0f172a 100%)",
        fontFamily: "'Avenir Next', 'Segoe UI', system-ui, sans-serif",
        padding: 24,
      }}
    >
      <form
        onSubmit={submit}
        style={{
          width: "100%",
          maxWidth: 380,
          background: "rgba(255,255,255,0.06)",
          border: "1px solid rgba(255,255,255,0.12)",
          borderRadius: 12,
          padding: "28px 26px",
          boxShadow: "0 24px 48px rgba(0,0,0,0.35)",
        }}
      >
        <div style={{ fontSize: 11, letterSpacing: 2, textTransform: "uppercase", color: "#94a3b8", marginBottom: 6 }}>
          Figwork
        </div>
        <h1 style={{ margin: "0 0 18px", fontSize: 22, fontWeight: 600, color: "#f8fafc" }}>Sign in</h1>
        <label style={{ display: "block", fontSize: 12, color: "#cbd5e1", marginBottom: 6 }}>Username</label>
        <input
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoComplete="username"
          style={{
            width: "100%",
            boxSizing: "border-box",
            marginBottom: 14,
            padding: "10px 12px",
            borderRadius: 8,
            border: "1px solid rgba(255,255,255,0.15)",
            background: "rgba(15,23,42,0.6)",
            color: "#f8fafc",
            fontSize: 14,
          }}
        />
        <label style={{ display: "block", fontSize: 12, color: "#cbd5e1", marginBottom: 6 }}>Password</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
          style={{
            width: "100%",
            boxSizing: "border-box",
            marginBottom: 16,
            padding: "10px 12px",
            borderRadius: 8,
            border: "1px solid rgba(255,255,255,0.15)",
            background: "rgba(15,23,42,0.6)",
            color: "#f8fafc",
            fontSize: 14,
          }}
        />
        {err ? (
          <div style={{ fontSize: 12, color: "#fca5a5", marginBottom: 12 }}>{err}</div>
        ) : null}
        <button
          type="submit"
          disabled={busy || !username.trim() || !password}
          style={{
            width: "100%",
            padding: "11px 14px",
            borderRadius: 8,
            border: "none",
            background: busy ? "#475569" : "#14b8a6",
            color: "#042f2e",
            fontWeight: 600,
            fontSize: 14,
            cursor: busy ? "default" : "pointer",
          }}
        >
          {busy ? "Signing in…" : "Continue"}
        </button>
      </form>
    </div>
  );
}
