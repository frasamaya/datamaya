import { useEffect, useState, type FormEvent } from "react";

type LoginFormProps = {
  loginUsername: string;
  password: string;
  error: string | null;
  onUsernameChange: (value: string) => void;
  onPasswordChange: (value: string) => void;
  onSubmit: (event: FormEvent) => void;
};

export function LoginForm({
  loginUsername,
  password,
  error,
  onUsernameChange,
  onPasswordChange,
  onSubmit,
}: LoginFormProps) {
  const [mode, setMode] = useState<"signin" | "register">("signin");
  const [registerNotice, setRegisterNotice] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const params = new URLSearchParams(window.location.search);
    const tab = params.get("tab");
    if (tab === "register") {
      setMode("register");
    }
  }, []);

  const handleSubmit = (event: FormEvent) => {
    if (mode === "register") {
      event.preventDefault();
      setRegisterNotice("Registration requests are handled by your admin.");
      return;
    }
    setRegisterNotice(null);
    onSubmit(event);
  };

  const handleModeChange = (next: "signin" | "register") => {
    setMode(next);
    setRegisterNotice(null);
  };

  return (
    <form className="card login" onSubmit={handleSubmit}>
      <section className="login-panel">
        <header className="login-brand">
          <img className="brand-logo" src="/logo.png" alt="Logo" />
          <span>Datamaya</span>
        </header>
        <div className="login-body">
          <p className="login-eyebrow">
            {mode === "signin" ? "Welcome Back" : "Create Account"}
          </p>
          <h2>{mode === "signin" ? "Manage your vault, securely." : "Start your secure vault."}</h2>
          <p className="login-subtitle">
            {mode === "signin"
              ? "Please enter your details to continue."
              : "Register to access your free cloud storage workspace."}
          </p>
          <div className="login-tabs" role="tablist" aria-label="Authentication options">
            <button
              type="button"
              className={`login-tab${mode === "signin" ? " is-active" : ""}`}
              role="tab"
              aria-selected={mode === "signin"}
              onClick={() => handleModeChange("signin")}
            >
              Sign In
            </button>
            <button
              type="button"
              className={`login-tab${mode === "register" ? " is-active" : ""}`}
              role="tab"
              aria-selected={mode === "register"}
              onClick={() => handleModeChange("register")}
            >
              Register
            </button>
          </div>
          {mode === "signin" ? (
            <>
              <div className="login-fields">
                <label className="login-label">
                  <span>Email Address</span>
                  <input
                    type="text"
                    value={loginUsername}
                    onChange={(event) => onUsernameChange(event.target.value)}
                    placeholder="name@company.com"
                    autoComplete="username"
                    autoFocus
                  />
                </label>
                <label className="login-label">
                  <span>Password</span>
                  <input
                    type="password"
                    value={password}
                    onChange={(event) => onPasswordChange(event.target.value)}
                    placeholder="Enter your password"
                    autoComplete="current-password"
                  />
                </label>
              </div>
              <button type="submit" className="login-submit">
                Continue
              </button>
              <div className="login-divider">
                <span>Or Continue With</span>
              </div>
              <div className="login-social">
                <button type="button" className="login-social-btn" aria-label="Continue with Google">
                  G
                </button>
                <button type="button" className="login-social-btn" aria-label="Continue with Apple">
                  A
                </button>
                <button
                  type="button"
                  className="login-social-btn"
                  aria-label="Continue with Facebook"
                >
                  f
                </button>
              </div>
              <p className="login-note">
                Access your vault from anywhere with secure, private storage.
              </p>
              {error ? <p className="error">{error}</p> : null}
            </>
          ) : (
            <>
              <div className="login-fields">
                <label className="login-label">
                  <span>Full Name</span>
                  <input type="text" placeholder="Jane Cooper" autoComplete="name" />
                </label>
                <label className="login-label">
                  <span>Email Address</span>
                  <input type="email" placeholder="name@company.com" autoComplete="email" />
                </label>
                <label className="login-label">
                  <span>Password</span>
                  <input type="password" placeholder="Create a password" autoComplete="new-password" />
                </label>
                <label className="login-label">
                  <span>Confirm Password</span>
                  <input type="password" placeholder="Repeat your password" autoComplete="new-password" />
                </label>
              </div>
              <button type="submit" className="login-submit">
                Request Access
              </button>
              {registerNotice ? <p className="login-note">{registerNotice}</p> : null}
            </>
          )}
        </div>
      </section>
      <aside className="login-visual" aria-hidden="true">
        <div className="login-visual-glow" />
        <svg className="login-safe" viewBox="0 0 240 240" aria-hidden="true">
          <defs>
            <linearGradient id="safeBody" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stopColor="#b4d6f3" />
              <stop offset="100%" stopColor="#6da8de" />
            </linearGradient>
            <linearGradient id="safeDoor" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stopColor="#7fb6e7" />
              <stop offset="100%" stopColor="#4f8fc5" />
            </linearGradient>
          </defs>
          <rect x="32" y="32" width="176" height="176" rx="28" fill="url(#safeBody)" />
          <rect x="68" y="68" width="104" height="104" rx="20" fill="url(#safeDoor)" />
          <circle cx="120" cy="120" r="28" fill="#bfe0f3" stroke="#4f86b0" strokeWidth="8" />
          <circle cx="120" cy="120" r="10" fill="#3a6c90" />
          <line x1="120" y1="84" x2="120" y2="110" stroke="#f8fbff" strokeWidth="6" />
          <line x1="120" y1="130" x2="120" y2="156" stroke="#f8fbff" strokeWidth="6" />
          <line x1="84" y1="120" x2="110" y2="120" stroke="#f8fbff" strokeWidth="6" />
          <line x1="130" y1="120" x2="156" y2="120" stroke="#f8fbff" strokeWidth="6" />
        </svg>
      </aside>
    </form>
  );
}
