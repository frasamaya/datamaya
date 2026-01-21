import { useEffect, useRef } from "react";
import { BRAND_SUBTITLE, BRAND_TITLE } from "../constants";

export function LandingPage() {
  const year = new Date().getFullYear();
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext("2d");
    if (!context) {
      return;
    }
    const particles = Array.from({ length: 20 }, () => ({
      x: Math.random(),
      y: Math.random(),
      vx: (Math.random() - 0.5) * 0.35,
      vy: (Math.random() - 0.5) * 0.35,
    }));
    let width = 0;
    let height = 0;
    let raf = 0;

    const resize = () => {
      const dpr = window.devicePixelRatio || 1;
      width = window.innerWidth;
      height = window.innerHeight;
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      context.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const tick = () => {
      context.clearRect(0, 0, width, height);
      context.lineWidth = 1;
      context.strokeStyle = "rgba(31, 99, 241, 0.08)";
      context.fillStyle = "rgba(31, 99, 241, 0.5)";

      for (const particle of particles) {
        particle.x += particle.vx / width;
        particle.y += particle.vy / height;
        if (particle.x <= 0 || particle.x >= 1) {
          particle.vx *= -1;
        }
        if (particle.y <= 0 || particle.y >= 1) {
          particle.vy *= -1;
        }
        const px = particle.x * width;
        const py = particle.y * height;
        context.beginPath();
        context.arc(px, py, 2.2, 0, Math.PI * 2);
        context.fill();
      }

      for (let i = 0; i < particles.length; i += 1) {
        for (let j = i + 1; j < particles.length; j += 1) {
          const a = particles[i];
          const b = particles[j];
          context.beginPath();
          context.moveTo(a.x * width, a.y * height);
          context.lineTo(b.x * width, b.y * height);
          context.stroke();
        }
      }

      raf = window.requestAnimationFrame(tick);
    };

    resize();
    window.addEventListener("resize", resize);
    raf = window.requestAnimationFrame(tick);

    return () => {
      window.removeEventListener("resize", resize);
      window.cancelAnimationFrame(raf);
    };
  }, []);
  return (
    <div className="landing-shell">
      <canvas className="landing-particles" ref={canvasRef} aria-hidden="true" />
      <header className="landing-topbar">
        <div className="landing-brand">
          <img className="brand-logo" src="/logo.png" alt="Logo" />
          <span>{BRAND_TITLE}</span>
        </div>
        <nav className="landing-nav">
          {/* <a href="#benefits">Benefits</a>
          <a href="#features">Features</a>
          <a href="#pricing">Pricing</a> */}
          <a href="/login" className="ghost">
            Sign In
          </a>
        </nav>
      </header>

      <section className="landing-hero">
        <div className="landing-hero-grid">
          <div className="landing-hero-copy">
            <p className="landing-eyebrow">Free Cloud Storage</p>
            <h1>
              Free cloud storage
              <br />
              for smart file management.
            </h1>
            <p className="landing-subtitle">
              {BRAND_SUBTITLE} Store, preview, and share your files with a clean, secure workspace.
            </p>
            <div className="landing-cta actions">
              <a className="primary" href="/login?tab=register">
                Get Started
              </a>
            </div>
          </div>
          <div className="landing-hero-media">
            <div className="hero-panel hero-panel-left">
              <p className="label">Design Assets</p>
              <p className="meta">Brand Kit</p>
              <div className="hero-chip">Syncing</div>
            </div>
            <div className="hero-panel hero-panel-top">
              <p className="label">API Stream</p>
              <p className="meta">Files/sec 1,204</p>
              <div className="hero-chip">Realtime</div>
            </div>
            <div className="hero-panel hero-panel-center">
              <p className="label">Shared Links</p>
              <p className="meta">Resume CV.pdf</p>
              <div className="hero-tags">
                <span>Preview</span>
                <span>Download</span>
                <span>Share</span>
              </div>
            </div>
            <div className="hero-panel hero-panel-right">
              <p className="label">Marketing</p>
              <p className="meta">Launch Deck</p>
              <div className="hero-chip">Secured</div>
            </div>
            <div className="hero-terminal">
              <div className="terminal-head">
                <span />
                <span />
                <span />
              </div>
              <div className="terminal-body">
                <p>$ sync --delta ./assets</p>
                <p className="muted">+ 38 files · 2.4GB</p>
                <p>$ share Resume\\ CV.pdf</p>
                <p className="muted">link generated · 2s</p>
              </div>
            </div>
            <div className="hero-orbit">
              <span />
              <span />
              <span />
            </div>
          </div>
        </div>
      </section>
      <footer className="landing-footer">
        <div>
          <p className="label">{BRAND_TITLE}</p>
          <p className="meta">Copyright {year} {BRAND_TITLE}. All rights reserved.</p>
        </div>
      </footer>
    </div>
  );
}
