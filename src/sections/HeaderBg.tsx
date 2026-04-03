import { useEffect, useRef } from "react";
import classes from "./HeaderBg.module.css";

/* ── 微生物类型 ── */
type MicrobeKind = "coccus" | "bacillus" | "spirillum";

interface Microbe {
  x: number;
  y: number;
  vx: number;
  vy: number;
  size: number;
  kind: MicrobeKind;
  rotation: number;
  rotSpeed: number;
  opacity: number;
  phase: number;        // 用于脉动动画
  hue: number;          // 在 primary↔secondary 之间插值
  pulseSpeed: number;
}

/* ── 颜色常量 ── */
const PRIMARY  = { r: 226, g: 63,  b: 255 }; // #e23fff
const SECONDARY = { r: 85,  g: 110, b: 255 }; // #556eff
const NETWORK_COLOR = "rgba(170, 183, 255, 0.06)";
const NETWORK_COLOR_NEAR = "rgba(170, 183, 255, 0.12)";

/* ── 配置 ── */
const IS_MOBILE = typeof matchMedia !== "undefined" && matchMedia("(max-width: 768px)").matches;
const PREFERS_REDUCED = typeof matchMedia !== "undefined" && matchMedia("(prefers-reduced-motion: reduce)").matches;
const PARTICLE_COUNT = IS_MOBILE ? 30 : 80;
const CONNECTION_DIST = IS_MOBILE ? 100 : 160;
const MOUSE_RADIUS = 200;
const MOUSE_FORCE = 0.8;

function lerp(a: number, b: number, t: number) {
  return a + (b - a) * t;
}

function microbeColor(hue: number, alpha: number): string {
  const r = Math.round(lerp(PRIMARY.r, SECONDARY.r, hue));
  const g = Math.round(lerp(PRIMARY.g, SECONDARY.g, hue));
  const b = Math.round(lerp(PRIMARY.b, SECONDARY.b, hue));
  return `rgba(${r},${g},${b},${alpha})`;
}

function createMicrobe(w: number, h: number): Microbe {
  const kind: MicrobeKind = (["coccus", "bacillus", "spirillum"] as const)[
    Math.floor(Math.random() * 3)
  ];
  return {
    x: Math.random() * w,
    y: Math.random() * h,
    vx: (Math.random() - 0.5) * 0.4,
    vy: (Math.random() - 0.5) * 0.4,
    size: kind === "spirillum" ? 3 + Math.random() * 3 : 2 + Math.random() * 4,
    kind,
    rotation: Math.random() * Math.PI * 2,
    rotSpeed: (Math.random() - 0.5) * 0.01,
    opacity: 0.15 + Math.random() * 0.35,
    phase: Math.random() * Math.PI * 2,
    hue: Math.random(),
    pulseSpeed: 0.005 + Math.random() * 0.015,
  };
}

/* ── 绘制不同形态的微生物 ── */
function drawCoccus(ctx: CanvasRenderingContext2D, m: Microbe, pulse: number) {
  const r = m.size * (0.9 + pulse * 0.1);
  ctx.beginPath();
  ctx.arc(m.x, m.y, r, 0, Math.PI * 2);
  ctx.fillStyle = microbeColor(m.hue, m.opacity * (0.8 + pulse * 0.2));
  ctx.fill();
  // 内部高光
  ctx.beginPath();
  ctx.arc(m.x - r * 0.25, m.y - r * 0.25, r * 0.4, 0, Math.PI * 2);
  ctx.fillStyle = microbeColor(m.hue, m.opacity * 0.15);
  ctx.fill();
}

function drawBacillus(ctx: CanvasRenderingContext2D, m: Microbe, pulse: number) {
  const len = m.size * 2.5;
  const w = m.size * 0.7 * (0.9 + pulse * 0.1);
  ctx.save();
  ctx.translate(m.x, m.y);
  ctx.rotate(m.rotation);
  ctx.beginPath();
  if (ctx.roundRect) {
    ctx.roundRect(-len / 2, -w / 2, len, w, w);
  } else {
    // fallback for older browsers
    const r = w / 2;
    ctx.moveTo(-len / 2 + r, -w / 2);
    ctx.lineTo(len / 2 - r, -w / 2);
    ctx.arc(len / 2 - r, 0, r, -Math.PI / 2, Math.PI / 2);
    ctx.lineTo(-len / 2 + r, w / 2);
    ctx.arc(-len / 2 + r, 0, r, Math.PI / 2, -Math.PI / 2);
    ctx.closePath();
  }
  ctx.fillStyle = microbeColor(m.hue, m.opacity * (0.8 + pulse * 0.2));
  ctx.fill();
  ctx.restore();
}

function drawSpirillum(ctx: CanvasRenderingContext2D, m: Microbe, pulse: number) {
  const amp = m.size * 0.6;
  const waveLen = m.size * 1.2;
  const segments = 5;
  ctx.save();
  ctx.translate(m.x, m.y);
  ctx.rotate(m.rotation);
  ctx.beginPath();
  ctx.moveTo(-segments * waveLen / 2, 0);
  for (let i = 0; i < segments; i++) {
    const x0 = -segments * waveLen / 2 + i * waveLen;
    const dir = i % 2 === 0 ? 1 : -1;
    ctx.quadraticCurveTo(
      x0 + waveLen / 2, dir * amp * (0.9 + pulse * 0.1),
      x0 + waveLen, 0
    );
  }
  ctx.strokeStyle = microbeColor(m.hue, m.opacity * (0.7 + pulse * 0.3));
  ctx.lineWidth = m.size * 0.35;
  ctx.lineCap = "round";
  ctx.stroke();
  ctx.restore();
}

/* ── 主组件 ── */
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
    const microbes: Microbe[] = [];

    const resize = () => {
      w = canvas.clientWidth;
      h = canvas.clientHeight;
      canvas.width = w * DPR;
      canvas.height = h * DPR;
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
    };

    const init = () => {
      resize();
      microbes.length = 0;
      for (let i = 0; i < PARTICLE_COUNT; i++) {
        microbes.push(createMicrobe(w, h));
      }
    };

    const onMouseMove = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      mouse = { x: e.clientX - rect.left, y: e.clientY - rect.top };
    };
    const onMouseLeave = () => { mouse = { x: -9999, y: -9999 }; };

    const animate = () => {
      ctx.clearRect(0, 0, w, h);

      /* ── 更新粒子 ── */
      for (const m of microbes) {
        // 鼠标排斥
        const dx = m.x - mouse.x;
        const dy = m.y - mouse.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < MOUSE_RADIUS && dist > 0) {
          const force = (1 - dist / MOUSE_RADIUS) * MOUSE_FORCE;
          m.vx += (dx / dist) * force;
          m.vy += (dy / dist) * force;
        }

        // 缓慢漂浮
        m.x += m.vx;
        m.y += m.vy;
        m.vx *= 0.99;
        m.vy *= 0.99;
        m.rotation += m.rotSpeed;
        m.phase += m.pulseSpeed;

        // 边界回弹（柔和）
        if (m.x < -20) m.x = w + 20;
        if (m.x > w + 20) m.x = -20;
        if (m.y < -20) m.y = h + 20;
        if (m.y > h + 20) m.y = -20;
      }

      /* ── 绘制网络连线 ── */
      for (let i = 0; i < microbes.length; i++) {
        for (let j = i + 1; j < microbes.length; j++) {
          const a = microbes[i];
          const b = microbes[j];
          const dx = a.x - b.x;
          const dy = a.y - b.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          if (dist < CONNECTION_DIST) {
            const alpha = 1 - dist / CONNECTION_DIST;
            ctx.beginPath();
            ctx.moveTo(a.x, a.y);
            ctx.lineTo(b.x, b.y);
            ctx.strokeStyle = alpha > 0.5 ? NETWORK_COLOR_NEAR : NETWORK_COLOR;
            ctx.globalAlpha = alpha * 0.5;
            ctx.lineWidth = 0.5;
            ctx.stroke();
            ctx.globalAlpha = 1;
          }
        }
      }

      /* ── 绘制微生物 ── */
      for (const m of microbes) {
        const pulse = Math.sin(m.phase) * 0.5 + 0.5;
        switch (m.kind) {
          case "coccus":
            drawCoccus(ctx, m, pulse);
            break;
          case "bacillus":
            drawBacillus(ctx, m, pulse);
            break;
          case "spirillum":
            drawSpirillum(ctx, m, pulse);
            break;
        }
      }

      /* ── 中心光晕 ── */
      const grd = ctx.createRadialGradient(w / 2, h / 2, 0, w / 2, h / 2, Math.max(w, h) * 0.5);
      grd.addColorStop(0, "rgba(226, 63, 255, 0.03)");
      grd.addColorStop(0.4, "rgba(85, 110, 255, 0.015)");
      grd.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = grd;
      ctx.fillRect(0, 0, w, h);

      animId = requestAnimationFrame(animate);
    };

    const onResize = () => { resize(); };

    init();
    if (PREFERS_REDUCED) {
      // Draw single static frame for users who prefer reduced motion
      animate();
      return () => { cancelAnimationFrame(animId); };
    }
    canvas.addEventListener("mousemove", onMouseMove);
    canvas.addEventListener("mouseleave", onMouseLeave);
    window.addEventListener("resize", onResize);
    animId = requestAnimationFrame(animate);

    return () => {
      cancelAnimationFrame(animId);
      canvas.removeEventListener("mousemove", onMouseMove);
      canvas.removeEventListener("mouseleave", onMouseLeave);
      window.removeEventListener("resize", onResize);
    };
  }, []);

  return <canvas ref={canvasRef} className={classes.canvas} />;
};

export default HeaderBg;
