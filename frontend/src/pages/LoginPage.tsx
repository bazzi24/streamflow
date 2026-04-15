import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { authApi } from "../api/stockApi";
import { useAppStore } from "../stores/appStore";
import styles from "./LoginPage.module.css";

// ── Anime SVG Buffalo (Trâu) ──────────────────────────────────────────────────
function BuffaloSVG() {
  return (
    <svg viewBox="0 0 160 200" width="160" height="200" xmlns="http://www.w3.org/2000/svg">
      <path d="M38 75 C20 60 5 40 10 25 C15 10 30 5 42 20" fill="none" stroke="#e0c060" strokeWidth="6" strokeLinecap="round" />
      <path d="M122 75 C140 60 155 40 150 25 C145 10 130 5 118 20" fill="none" stroke="#e0c060" strokeWidth="6" strokeLinecap="round" />
      <path d="M38 75 C22 62 10 45 14 30" fill="none" stroke="rgba(255,255,255,0.3)" strokeWidth="2" strokeLinecap="round" />
      <path d="M122 75 C138 62 150 45 146 30" fill="none" stroke="rgba(255,255,255,0.3)" strokeWidth="2" strokeLinecap="round" />
      <ellipse cx="42" cy="88" rx="14" ry="10" fill="#7ec8e3" />
      <ellipse cx="42" cy="88" rx="8" ry="6" fill="#b8d4e8" />
      <ellipse cx="118" cy="88" rx="14" ry="10" fill="#7ec8e3" />
      <ellipse cx="118" cy="88" rx="8" ry="6" fill="#b8d4e8" />
      <ellipse cx="80" cy="105" rx="50" ry="45" fill="#4a90d9" />
      <ellipse cx="65" cy="90" rx="20" ry="14" fill="rgba(255,255,255,0.2)" />
      <ellipse cx="80" cy="132" rx="26" ry="18" fill="#d4ecf7" />
      <ellipse cx="70" cy="135" rx="5" ry="4" fill="#2d6a9f" />
      <ellipse cx="90" cy="135" rx="5" ry="4" fill="#2d6a9f" />
      <ellipse cx="52" cy="115" rx="9" ry="5" fill="rgba(255,120,120,0.35)" />
      <ellipse cx="108" cy="115" rx="9" ry="5" fill="rgba(255,120,120,0.35)" />
      <ellipse cx="60" cy="100" rx="12" ry="13" fill="white" />
      <ellipse cx="100" cy="100" rx="12" ry="13" fill="white" />
      <ellipse cx="61" cy="101" rx="8" ry="9" fill="#1a3a6b" />
      <ellipse cx="101" cy="101" rx="8" ry="9" fill="#1a3a6b" />
      <ellipse cx="63" cy="99" rx="4" ry="4" fill="white" />
      <ellipse cx="103" cy="99" rx="4" ry="4" fill="white" />
      <rect x="48" y="96" width="24" height="6" rx="3" fill="#4a90d9" />
      <rect x="88" y="96" width="24" height="6" rx="3" fill="#4a90d9" />
      <path d="M66 126 Q80 138 94 126" fill="none" stroke="#2d6a9f" strokeWidth="2.5" strokeLinecap="round" />
      <ellipse cx="80" cy="168" rx="38" ry="28" fill="#4a90d9" />
      <ellipse cx="60" cy="158" rx="12" ry="10" fill="#3a7fc8" />
      <ellipse cx="100" cy="158" rx="12" ry="10" fill="#3a7fc8" />
      <g transform="translate(110, 175) rotate(15)">
        <rect x="0" y="0" width="32" height="22" rx="3" fill="white" />
        <line x1="4" y1="6" x2="28" y2="6" stroke="#00b4d8" strokeWidth="1.5" />
        <line x1="4" y1="11" x2="22" y2="11" stroke="#00b4d8" strokeWidth="1.5" />
        <line x1="4" y1="16" x2="26" y2="16" stroke="#00b4d8" strokeWidth="1.5" />
        <path d="M20 18 L24 12 L28 18" fill="none" stroke="#059669" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="24" cy="12" r="1.5" fill="#059669" />
      </g>
      <text x="72" y="172" fontSize="14" fill="#e0c060">★</text>
    </svg>
  );
}

// ── Anime SVG Cow (Bò) ────────────────────────────────────────────────────────
function CowSVG() {
  return (
    <svg viewBox="0 0 160 200" width="140" height="200" xmlns="http://www.w3.org/2000/svg">
      <path d="M48 78 C38 65 30 55 32 42 C34 30 44 22 52 32" fill="none" stroke="#c8a84b" strokeWidth="5" strokeLinecap="round" />
      <path d="M112 78 C122 65 130 55 128 42 C126 30 116 22 108 32" fill="none" stroke="#c8a84b" strokeWidth="5" strokeLinecap="round" />
      <ellipse cx="40" cy="90" rx="13" ry="9" fill="#d4a0a0" />
      <ellipse cx="40" cy="90" rx="7" ry="5" fill="#e8c0c0" />
      <ellipse cx="120" cy="90" rx="13" ry="9" fill="#d4a0a0" />
      <ellipse cx="120" cy="90" rx="7" ry="5" fill="#e8c0c0" />
      <ellipse cx="80" cy="108" rx="48" ry="43" fill="white" />
      <ellipse cx="55" cy="95" rx="18" ry="14" fill="#00b4d8" />
      <ellipse cx="105" cy="118" rx="14" ry="11" fill="#00b4d8" />
      <ellipse cx="72" cy="82" rx="8" ry="6" fill="#00b4d8" />
      <ellipse cx="60" cy="100" rx="13" ry="14" fill="white" />
      <ellipse cx="100" cy="100" rx="13" ry="14" fill="white" />
      <ellipse cx="61" cy="101" rx="9" ry="10" fill="#2c1810" />
      <ellipse cx="101" cy="101" rx="9" ry="10" fill="#2c1810" />
      <ellipse cx="63" cy="98" rx="4" ry="4" fill="white" />
      <ellipse cx="103" cy="98" rx="4" ry="4" fill="white" />
      <rect x="47" y="91" width="26" height="20" rx="5" fill="none" stroke="#334" strokeWidth="2" />
      <rect x="87" y="91" width="26" height="20" rx="5" fill="none" stroke="#334" strokeWidth="2" />
      <line x1="73" y1="100" x2="87" y2="100" stroke="#334" strokeWidth="2" />
      <rect x="47" y="95" width="26" height="5" rx="2.5" fill="#4a90d9" />
      <rect x="87" y="95" width="26" height="5" rx="2.5" fill="#4a90d9" />
      <ellipse cx="46" cy="112" rx="10" ry="6" fill="rgba(255,150,150,0.4)" />
      <ellipse cx="114" cy="112" rx="10" ry="6" fill="rgba(255,150,150,0.4)" />
      <ellipse cx="80" cy="128" rx="22" ry="16" fill="#f5d0d0" />
      <ellipse cx="72" cy="130" rx="5" ry="4" fill="#c07070" />
      <ellipse cx="88" cy="130" rx="5" ry="4" fill="#c07070" />
      <path d="M66 122 Q80 133 94 122" fill="none" stroke="#c07070" strokeWidth="2.2" strokeLinecap="round" />
      <ellipse cx="80" cy="170" rx="36" ry="26" fill="white" />
      <ellipse cx="55" cy="165" rx="14" ry="11" fill="#00b4d8" />
      <ellipse cx="105" cy="175" rx="10" ry="8" fill="#00b4d8" />
      <g transform="translate(105, 178) rotate(-10)">
        <rect x="0" y="0" width="26" height="18" rx="3" fill="#f0f8ff" />
        <rect x="0" y="0" width="26" height="18" rx="3" fill="none" stroke="#00b4d8" strokeWidth="1" />
        <line x1="4" y1="5" x2="22" y2="5" stroke="#00b4d8" strokeWidth="1.2" />
        <line x1="4" y1="9" x2="18" y2="9" stroke="#00b4d8" strokeWidth="1.2" />
        <line x1="4" y1="13" x2="20" y2="13" stroke="#00b4d8" strokeWidth="1.2" />
      </g>
    </svg>
  );
}

// ── Login Page ────────────────────────────────────────────────────────────────
type Mode = "login" | "register";

export function LoginPage() {
  const navigate = useNavigate();
  const { setAuth, theme, toggleTheme } = useAppStore();

  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [username, setUsername] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setError("");
      setLoading(true);
      try {
        if (mode === "login") {
          const res = await authApi.login({ email, password });
          const { access_token, user } = res.data;
          localStorage.setItem("access_token", access_token);
          setAuth(access_token, user);
          navigate("/");
        } else {
          const res = await authApi.register({ email, username, password });
          const { access_token, user } = res.data;
          localStorage.setItem("access_token", access_token);
          setAuth(access_token, user);
          navigate("/");
        }
      } catch (err: unknown) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const apiErr = (err as any);
        const errMsg = apiErr?.response?.data?.detail;
        setError(errMsg ?? "Something went wrong. Please try again.");
      } finally {
        setLoading(false);
      }
    },
    [mode, email, password, username, navigate, setAuth]
  );

  return (
    <div className={styles.page}>
      {/* ── Left: Mascots ── */}
      <div className={styles["left-panel"]}>
        <div className={`${styles["deco-circle"]} ${styles["deco-1"]}`} />
        <div className={`${styles["deco-circle"]} ${styles["deco-2"]}`} />
        <div className={`${styles["deco-circle"]} ${styles["deco-3"]}`} />

        <div className={styles["hose-badge"]}>🇻🇳 Ho Chi Minh Stock Exchange</div>

        <div className={styles.mascots}>
          <div className={styles["mascot-wrapper"]}>
            <div className={styles["buffalo-wrapper"]}>
              <BuffaloSVG />
            </div>
            <div className={styles["mascot-label"]}>Trâu StreamFlow</div>
          </div>
          <div className={styles["mascot-wrapper"]}>
            <div className={styles["cow-wrapper"]}>
              <CowSVG />
            </div>
            <div className={styles["mascot-label"]}>Bò Market Pro</div>
          </div>
        </div>

        <div className={styles["market-cards"]}>
          <div className={styles["market-card"]}>
            <div className={styles["market-card-name"]}>VN-Index</div>
            <div className={styles["market-card-value"]}>1,350.24</div>
            <div className={`${styles["market-card-change"]} ${styles.up}`}>+12.34 ▲</div>
          </div>
          <div className={styles["market-card"]}>
            <div className={styles["market-card-name"]}>VN30</div>
            <div className={styles["market-card-value"]}>1,048.76</div>
            <div className={`${styles["market-card-change"]} ${styles.up}`}>+8.91 ▲</div>
          </div>
          <div className={styles["market-card"]}>
            <div className={styles["market-card-name"]}>HNX</div>
            <div className={styles["market-card-value"]}>228.45</div>
            <div className={`${styles["market-card-change"]} ${styles.down}`}>−2.13 ▼</div>
          </div>
        </div>
      </div>

      {/* ── Right: Form ── */}
      <div className={styles["right-panel"]}>
        <button
          className={styles["theme-toggle"]}
          onClick={toggleTheme}
          title={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
        >
          {theme === "light" ? "🌙" : "☀️"}
        </button>

        <div className={styles["form-card"]}>
          <div className={styles["form-logo"]}>
            <div className={styles["form-logo-icon"]}>SF</div>
            <span className={styles["form-logo-text"]}>StreamFlow</span>
          </div>

          <h1 className={styles["form-title"]}>
            {mode === "login" ? "Welcome back!" : "Create account"}
          </h1>
          <p className={styles["form-subtitle"]}>
            {mode === "login"
              ? "Sign in to access your watchlist and trading dashboard."
              : "Join StreamFlow to track Vietnam's stock market."}
          </p>

          <div className={styles["toggle-tabs"]}>
            <button
              className={`${styles["toggle-tab"]} ${mode === "login" ? styles.active : ""}`}
              onClick={() => { setMode("login"); setError(""); }}
            >
              Sign In
            </button>
            <button
              className={`${styles["toggle-tab"]} ${mode === "register" ? styles.active : ""}`}
              onClick={() => { setMode("register"); setError(""); }}
            >
              Sign Up
            </button>
          </div>

          {error && <div className={styles["error-msg"]}>{error}</div>}

          <form onSubmit={handleSubmit}>
            {mode === "register" && (
              <div className={styles["field-group"]}>
                <label className={styles["field-label"]}>Username</label>
                <input
                  className={styles["field-input"]}
                  type="text"
                  placeholder="Your username"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                  autoComplete="username"
                />
              </div>
            )}

            <div className={styles["field-group"]}>
              <label className={styles["field-label"]}>Email</label>
              <input
                className={styles["field-input"]}
                type="email"
                placeholder="you@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                autoComplete="email"
              />
            </div>

            <div className={styles["field-group"]}>
              <label className={styles["field-label"]}>Password</label>
              <input
                className={styles["field-input"]}
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete={mode === "login" ? "current-password" : "new-password"}
              />
            </div>

            <button
              className={styles["submit-btn"]}
              type="submit"
              disabled={loading}
            >
              {loading ? "Please wait…" : mode === "login" ? "Sign In" : "Create Account"}
            </button>
          </form>

          <div className={styles.divider}>or</div>

          <div className={styles["form-footer"]}>
            {mode === "login" ? (
              <>
                No account?{" "}
                <span
                  className={styles["field-link"]}
                  onClick={() => { setMode("register"); setError(""); }}
                >
                  Sign up free
                </span>
              </>
            ) : (
              <>
                Have an account?{" "}
                <span
                  className={styles["field-link"]}
                  onClick={() => { setMode("login"); setError(""); }}
                >
                  Sign in
                </span>
              </>
            )}
          </div>

          <div className={styles["form-footer"]} style={{ marginTop: 10 }}>
            By continuing, you agree to StreamFlow's Terms of Service.
          </div>
        </div>
      </div>
    </div>
  );
}
