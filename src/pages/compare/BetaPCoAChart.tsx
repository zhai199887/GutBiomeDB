import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";

import * as d3 from "d3";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { BetaMetric, BetaPoint, DiffResult } from "./types";

// Compute 2σ confidence ellipse parameters in pixel space.
// Matches the paper's matplotlib conf_ellipse(n_std=2.0, np.cov) exactly:
// - sample covariance (n-1 denominator)
// - covariance matrix transformed to pixel space before eigendecomposition
//   so the angle and radii are correct even when x/y axes have different scales
function computeEllipseParams(
  points: BetaPoint[],
  xScale: d3.ScaleLinear<number, number>,
  yScale: d3.ScaleLinear<number, number>,
  nStd = 2.0,
): { cx: number; cy: number; rx: number; ry: number; angle: number } | null {
  if (points.length < 3) return null;

  const n = points.length;
  const meanX = d3.mean(points, (p) => p.x) ?? 0;
  const meanY = d3.mean(points, (p) => p.y) ?? 0;
  const xs = points.map((p) => p.x - meanX);
  const ys = points.map((p) => p.y - meanY);

  // Sample covariance with n-1 denominator (matches numpy.cov)
  const f = n / (n - 1);
  const covXX = (d3.mean(xs.map((v) => v * v)) ?? 0) * f;
  const covYY = (d3.mean(ys.map((v) => v * v)) ?? 0) * f;
  const covXY = (d3.mean(xs.map((v, i) => v * ys[i]!)) ?? 0) * f;

  // Pixels-per-unit for each axis (y-axis is inverted in SVG)
  const xF = Math.abs(xScale(1) - xScale(0));
  const yF = Math.abs(yScale(1) - yScale(0));

  // Transform covariance to pixel space: T = diag(xF, -yF)
  // C_pix = T @ C_data @ T^T  →  off-diagonal gets negated by y-flip
  const pCovXX = xF * xF * covXX;
  const pCovYY = yF * yF * covYY;
  const pCovXY = -xF * yF * covXY;

  // Eigendecomposition of 2×2 symmetric pixel covariance
  const trace = pCovXX + pCovYY;
  const det = pCovXX * pCovYY - pCovXY * pCovXY;
  const gap = Math.sqrt(Math.max(0, trace * trace / 4 - det));
  const lambda1 = trace / 2 + gap;
  const lambda2 = trace / 2 - gap;
  const angle = Math.atan2(lambda1 - pCovXX, pCovXY || 1e-9) * 180 / Math.PI;

  return {
    cx: xScale(meanX),
    cy: yScale(meanY),
    rx: Math.sqrt(Math.max(lambda1, 1e-9)) * nStd,
    ry: Math.sqrt(Math.max(lambda2, 1e-9)) * nStd,
    angle,
  };
}

const BetaPCoAChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [metric, setMetric] = useState<BetaMetric>("braycurtis");

  useEffect(() => {
    if (result.beta_diversity.default_metric === "aitchison") {
      setMetric("aitchison");
    }
  }, [result.beta_diversity.default_metric]);

  const current = useMemo(
    () => result.beta_diversity.metrics[metric] ?? result.beta_diversity.metrics[result.beta_diversity.default_metric],
    [metric, result.beta_diversity.default_metric, result.beta_diversity.metrics],
  );

  useEffect(() => {
    if (!svgRef.current || !current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const coords = current.pcoa_coords;
    if (!coords.length) {
      svg.attr("viewBox", "0 0 680 120");
      svg.append("text")
        .attr("x", 20)
        .attr("y", 60)
        .attr("fill", "currentColor")
        .attr("font-size", 14)
        .text(locale === "zh" ? "暂无 Beta 多样性坐标" : "No beta-diversity coordinates available");
      return;
    }

    const width = 760;
    const height = 460;
    const margin = { top: 44, right: 160, bottom: 64, left: 60 };
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const groupA = coords.filter((point) => point.group === "A");
    const groupB = coords.filter((point) => point.group === "B");
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xDomain = d3.extent(coords, (point) => point.x) as [number, number];
    const yDomain = d3.extent(coords, (point) => point.y) as [number, number];
    const x = d3.scaleLinear().domain(xDomain).nice().range([0, innerWidth]);
    const y = d3.scaleLinear().domain(yDomain).nice().range([innerHeight, 0]);

    const ellipseA = computeEllipseParams(groupA, x, y);
    if (ellipseA) {
      g.append("ellipse")
        .attr("cx", ellipseA.cx)
        .attr("cy", ellipseA.cy)
        .attr("rx", ellipseA.rx)
        .attr("ry", ellipseA.ry)
        .attr("transform", `rotate(${ellipseA.angle}, ${ellipseA.cx}, ${ellipseA.cy})`)
        .attr("fill", "rgba(34, 197, 94, 0.10)")
        .attr("stroke", "var(--secondary)")
        .attr("stroke-width", 1.2);
    }

    const ellipseB = computeEllipseParams(groupB, x, y);
    if (ellipseB) {
      g.append("ellipse")
        .attr("cx", ellipseB.cx)
        .attr("cy", ellipseB.cy)
        .attr("rx", ellipseB.rx)
        .attr("ry", ellipseB.ry)
        .attr("transform", `rotate(${ellipseB.angle}, ${ellipseB.cx}, ${ellipseB.cy})`)
        .attr("fill", "rgba(59, 130, 246, 0.10)")
        .attr("stroke", "var(--primary)")
        .attr("stroke-width", 1.2);
    }

    g.selectAll("circle")
      .data(coords)
      .join("circle")
      .attr("cx", (point) => x(point.x))
      .attr("cy", (point) => y(point.y))
      .attr("r", 4.6)
      .attr("fill", (point) => point.group === "A" ? "var(--secondary)" : "var(--primary)")
      .attr("opacity", 0.85)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (point) => renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "分组" : "Group"}</span>
          <span>{point.group === "A" ? result.summary.group_a_name : result.summary.group_b_name}</span>
          <span>PCo1</span><span>{point.x.toFixed(4)}</span>
          <span>PCo2</span><span>{point.y.toFixed(4)}</span>
        </div>,
      ));

    g.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(d3.axisBottom(x).ticks(6))
      .attr("font-size", 11);

    g.append("g").call(d3.axisLeft(y).ticks(6)).attr("font-size", 11);

    svg.append("text")
      .attr("x", margin.left + innerWidth / 2)
      .attr("y", height - 12)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 12)
      .text(`PCo1 (${current.metric}) ${current.variance_explained[0] ?? 0}%`);

    svg.append("text")
      .attr("transform", `translate(16,${height / 2}) rotate(-90)`)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 12)
      .text(`PCo2 (${current.metric}) ${current.variance_explained[1] ?? 0}%`);

    svg.append("text")
      .attr("x", margin.left + innerWidth / 2)
      .attr("y", 20)
      .attr("text-anchor", "middle")
      .attr("fill", "var(--light-gray)")
      .attr("font-size", 12)
      .text(locale === "zh" ? "95% 置信椭圆展示组内离散度" : "95% confidence ellipses show within-group dispersion");

    [
      { color: "var(--secondary)", label: result.summary.group_a_name, n: groupA.length },
      { color: "var(--primary)", label: result.summary.group_b_name, n: groupB.length },
    ].forEach((item, index) => {
      const legendX = width - margin.right + 12;
      const legendY = margin.top + 16 + index * 22;
      svg.append("circle")
        .attr("cx", legendX)
        .attr("cy", legendY)
        .attr("r", 5)
        .attr("fill", item.color);
      const displayLabel = item.label.length > 14 ? `${item.label.slice(0, 13)}...` : item.label;
      svg.append("text")
        .attr("x", legendX + 14)
        .attr("y", legendY + 4)
        .attr("fill", "var(--light-gray)")
        .attr("font-size", 10)
        .text(`${displayLabel} (n=${item.n})`);
    });

    // PERMANOVA stats box — mirrors the paper figure
    const perm = result.permanova;
    if (perm) {
      const bx = margin.left + innerWidth - 192;
      const by = margin.top + 10;
      const bw = 190;
      const bh = 70;
      svg.append("rect")
        .attr("x", bx).attr("y", by)
        .attr("width", bw).attr("height", bh)
        .attr("rx", 5)
        .attr("fill", "var(--card-bg, #fff)")
        .attr("stroke", "var(--border, #ccc)")
        .attr("stroke-width", 1)
        .attr("opacity", 0.9);
      const permLines = [
        { text: "PERMANOVA", bold: true },
        { text: `R² = ${perm.r_squared.toFixed(4)},  F = ${perm.f_statistic.toFixed(2)}`, bold: false },
        { text: `p = ${perm.p_value} (${perm.permutations} perm.)`, bold: false },
      ];
      permLines.forEach((line, i) => {
        svg.append("text")
          .attr("x", bx + 10).attr("y", by + 18 + i * 18)
          .attr("fill", "currentColor")
          .attr("font-size", i === 0 ? 11 : 10)
          .attr("font-weight", line.bold ? 700 : 400)
          .text(line.text);
      });
    }

    svg.attr("viewBox", `0 0 ${width} ${height}`);
  }, [current, locale, result.summary.group_a_name, result.summary.group_b_name, result.permanova]);

  if (!current) {
    return null;
  }

  return (
    <div className={classes.betaWrap}>
      <div className={classes.metricTabs}>
        {(["braycurtis", "aitchison"] as const).map((item) => (
          <button
            key={item}
            type="button"
            className={classes.metricTab}
            data-active={metric === item}
            onClick={() => setMetric(item)}
          >
            {item}
          </button>
        ))}
      </div>
      <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />
    </div>
  );
};

export default BetaPCoAChart;
