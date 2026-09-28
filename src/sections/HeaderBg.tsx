import { useEffect, useRef } from "react";
import classes from "./HeaderBg.module.css";

/* ── Particle types ── */
interface Cell {
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
  membraneRadius: number;
  opacity: number;
  phase: number;
  hue: number;          // 0→primary, 1→secondary
  pulseSpeed: number;
  nucleusRatio: number;  // nucleus relative size
  layer: number;         // 0=far, 1=mid, 2=foreground
}

interface HelixStrand {
  x: number;
  y: number;
  angle: number;
  length: number;
  speed: number;
  opacity: number;
  phase: number;
  drift: number;
}

/* ── Color constants ── */
const P = { r: 226, g: 63,  b: 255 }; // --primary #e23fff
const S = { r: 85,  g: 110, b: 255 }; // --secondary #556eff
const TEAL = { r: 60, g: 200, b: 180 }; // biology green

function lerp3(a: typeof P, b: typeof P, t: number, alpha: number): string {
  return `rgba(${Math.round(a.r + (b.r - a.r) * t)},${Math.round(a.g + (b.g - a.g) * t)},${Math.round(a.b + (b.b - a.b) * t)},${alpha})`;
}

/* ── Configuration ── */
const CELL_COUNT = 80;
const HELIX_COUNT = 3;
const CONNECTION_DIST = 180;
const MOUSE_RADIUS = 220;

function createCell(w: number, h: number): Cell {
  const layer = Math.random() < 0.3 ? 0 : Math.random() < 0.6 ? 1 : 2;
  const scale = [0.5, 0.8, 1.2][layer];
  return {
    x: Math.random() * w,
    y: Math.random() * h,
    vx: (Math.random() - 0.5) * 0.25 * scale,
    vy: (Math.random() - 0.5) * 0.25 * scale,
    radius: (3 + Math.random() * 5) * scale,
    membraneRadius: (5 + Math.random() * 7) * scale,
    opacity: [0.08, 0.18, 0.35][layer],
    phase: Math.random() * Math.PI * 2,
    hue: Math.random(),
    pulseSpeed: 0.004 + Math.random() * 0.012,
    nucleusRatio: 0.3 + Math.random() * 0.3,
    layer,
  };
}

function createHelix(w: number, h: number): HelixStrand {
  return {
    x: Math.random() * w,
    y: Math.random() * h,
    angle: Math.random() * Math.PI * 2,
    length: 60 + Math.random() * 100,
    speed: 0.002 + Math.random() * 0.003,
    opacity: 0.04 + Math.random() * 0.06,
    phase: Math.random() * Math.PI * 2,
    drift: (Math.random() - 0.5) * 0.15,
  };
}

/* ── Draw cell ── */
function drawCell(ctx: CanvasRenderingContext2D, c: Cell, pulse: number) {
  const mr = c.membraneRadius * (0.95 + pulse * 0.05);
  const nr = c.radius * c.nucleusRatio * (0.9 + pulse * 0.1);

  // Membrane — semi-transparent outer ring
  ctx.beginPath();
  ctx.arc(c.x, c.y, mr, 0, Math.PI * 2);
  const memColor = c.hue < 0.5
    ? lerp3(P, S, c.hue * 2, c.opacity * 0.3)
    : lerp3(S, TEAL, (c.hue - 0.5) * 2, c.opacity * 0.3);
  ctx.strokeStyle = memColor;
  ctx.lineWidth = 0.8;
  ctx.stroke();

  // Cytoplasm — radial gradient fill
  const grad = ctx.createRadialGradient(c.x, c.y, 0, c.x, c.y, mr);
  const bodyColor = c.hue < 0.5
    ? lerp3(P, S, c.hue * 2, c.opacity * (0.6 + pulse * 0.4))
    : lerp3(S, TEAL, (c.hue - 0.5) * 2, c.opacity * (0.6 + pulse * 0.4));
  grad.addColorStop(0, bodyColor);
  grad.addColorStop(0.6, lerp3(P, S, c.hue, c.opacity * 0.15));
  grad.addColorStop(1, "rgba(0,0,0,0)");
  ctx.fillStyle = grad;
  ctx.beginPath();
  ctx.arc(c.x, c.y, mr, 0, Math.PI * 2);
  ctx.fill();

  // Nucleus
  ctx.beginPath();
  ctx.arc(c.x, c.y, nr, 0, Math.PI * 2);
  ctx.fillStyle = lerp3(P, S, c.hue, c.opacity * 0.8);
  ctx.fill();
}

/* ── Draw DNA double helix ribbon ── */
function drawHelix(ctx: CanvasRenderingContext2D, h: HelixStrand, time: number) {
  const segments = 24;
  const amp = 8;
  const waveLen = h.length / 4;
  const t = time * h.speed + h.phase;

  ctx.save();
  ctx.translate(h.x, h.y);
  ctx.rotate(h.angle);

  // Two strands
  for (let strand = 0; strand < 2; strand++) {
    const offset = strand * Math.PI;
    ctx.beginPath();
    for (let i = 0; i <= segments; i++) {
      const frac = i / segments;
      const px = (frac - 0.5) * h.length;
      const py = Math.sin(frac * Math.PI * 4 + t + offset) * amp;
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    }
    ctx.strokeStyle = strand === 0
      ? lerp3(P, S, 0.3, h.opacity)
      : lerp3(S, TEAL, 0.5, h.opacity);
    ctx.lineWidth = 1.2;
    ctx.lineCap = "round";
    ctx.stroke();
  }

  // Base-pair connecting lines
  for (let i = 0; i < 8; i++) {
    const frac = (i + 0.5) / 8;
    const px = (frac - 0.5) * h.length;
    const py1 = Math.sin(frac * Math.PI * 4 + t) * amp;
    const py2 = Math.sin(frac * Math.PI * 4 + t + Math.PI) * amp;
    ctx.beginPath();
    ctx.moveTo(px, py1);
    ctx.lineTo(px, py2);
    ctx.strokeStyle = lerp3(P, S, 0.5, h.opacity * 0.5);
    ctx.lineWidth = 0.6;
    ctx.stroke();
  }

  ctx.restore();
}

/* ── Main component ── */
const HeaderBg = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const DPR = Math.min(window.devicePixelRatio || 1, 2);
    let w = 0;
    let h = 0;
    let mouse = { x: -9999, y: -9999 };
    let animId = 0;
    let time = 0;
    const cells: Cell[] = [];
    const helices: HelixStrand[] = [];

    const resize = () => {
      w = canvas.clientWidth;
      h = canvas.clientHeight;
      canvas.width = w * DPR;
      canvas.height = h * DPR;
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
    };

    const init = () => {
      resize();
      cells.length = 0;
      helices.length = 0;
      for (let i = 0; i < CELL_COUNT; i++) cells.push(createCell(w, h));
      for (let i = 0; i < HELIX_COUNT; i++) helices.push(createHelix(w, h));
    };

    const onMouseMove = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      mouse = { x: e.clientX - rect.left, y: e.clientY - rect.top };
    };
    const onMouseLeave = () => { mouse = { x: -9999, y: -9999 }; };

    const animate = () => {
      ctx.clearRect(0, 0, w, h);
      time++;

      /* ── Background gradient glow ── */
      const bgGrd = ctx.createRadialGradient(w * 0.5, h * 0.4, 0, w * 0.5, h * 0.4, Math.max(w, h) * 0.6);
      bgGrd.addColorStop(0, "rgba(226, 63, 255, 0.025)");
      bgGrd.addColorStop(0.3, "rgba(85, 110, 255, 0.015)");
      bgGrd.addColorStop(0.6, "rgba(60, 200, 180, 0.008)");
      bgGrd.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = bgGrd;
      ctx.fillRect(0, 0, w, h);

      /* ── Update and draw DNA helices ── */
      for (const hl of helices) {
        hl.x += hl.drift;
        hl.y += Math.sin(time * 0.003 + hl.phase) * 0.1;
        hl.angle += 0.0003;
        // Boundary wrap-around
        if (hl.x < -hl.length) hl.x = w + hl.length;
        if (hl.x > w + hl.length) hl.x = -hl.length;
        if (hl.y < -hl.length) hl.y = h + hl.length;
        if (hl.y > h + hl.length) hl.y = -hl.length;
        drawHelix(ctx, hl, time);
      }

      /* ── Update cells ── */
      for (const c of cells) {
        // Mouse interaction — soft attract / repel
        const dx = c.x - mouse.x;
        const dy = c.y - mouse.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < MOUSE_RADIUS && dist > 0) {
          const force = (1 - dist / MOUSE_RADIUS) * 0.5;
          c.vx += (dx / dist) * force;
          c.vy += (dy / dist) * force;
        }

        c.x += c.vx;
        c.y += c.vy;
        c.vx *= 0.992;
        c.vy *= 0.992;
        c.phase += c.pulseSpeed;

        // Boundary wrap-around
        if (c.x < -30) c.x = w + 30;
        if (c.x > w + 30) c.x = -30;
        if (c.y < -30) c.y = h + 30;
        if (c.y > h + 30) c.y = -30;
      }

      /* ── Sort by depth layer before drawing ── */
      const sorted = [...cells].sort((a, b) => a.layer - b.layer);

      /* ── Network connections — mid and foreground only ── */
      for (let i = 0; i < sorted.length; i++) {
        const a = sorted[i];
        if (a.layer === 0) continue;
        for (let j = i + 1; j < sorted.length; j++) {
          const b = sorted[j];
          if (b.layer === 0) continue;
          const ddx = a.x - b.x;
          const ddy = a.y - b.y;
          const d = Math.sqrt(ddx * ddx + ddy * ddy);
          if (d < CONNECTION_DIST) {
            const alpha = (1 - d / CONNECTION_DIST) * 0.08;
            ctx.beginPath();
            ctx.moveTo(a.x, a.y);
            ctx.lineTo(b.x, b.y);
            ctx.strokeStyle = `rgba(170, 183, 255, ${alpha})`;
            ctx.lineWidth = 0.5;
            ctx.stroke();
          }
        }
      }

      /* ── Draw cells ── */
      for (const c of sorted) {
        const pulse = Math.sin(c.phase) * 0.5 + 0.5;
        drawCell(ctx, c, pulse);
      }

      /* ── Foreground particles — tiny floating spores ── */
      for (let i = 0; i < 30; i++) {
        const seed = i * 7919;
        const px = ((seed * 13 + time * 0.3) % w + w) % w;
        const py = ((seed * 17 + time * 0.15) % h + h) % h;
        const sz = 0.5 + (seed % 10) * 0.1;
        const alpha = 0.05 + (seed % 5) * 0.015;
        ctx.beginPath();
        ctx.arc(px, py, sz, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(228, 232, 238, ${alpha})`;
        ctx.fill();
      }

      animId = requestAnimationFrame(animate);
    };

    init();
    canvas.addEventListener("mousemove", onMouseMove);
    canvas.addEventListener("mouseleave", onMouseLeave);
    window.addEventListener("resize", () => resize());
    animId = requestAnimationFrame(animate);

    return () => {
      cancelAnimationFrame(animId);
      canvas.removeEventListener("mousemove", onMouseMove);
      canvas.removeEventListener("mouseleave", onMouseLeave);
      window.removeEventListener("resize", resize);
    };
  }, []);

  return <canvas ref={canvasRef} className={classes.canvas} aria-hidden="true" />;
};

export default HeaderBg;
