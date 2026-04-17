import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";

import * as d3 from "d3";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { BetaMetric, BetaPoint, DiffResult } from "./types";

// Compute 2σ confidence ellipse in pixel space.
// Steps: sample covariance (n-1) → eigendecomposition in data space →
// semi-axes = n_std * sqrt(eigenvalues) → 64-gon in data coords → project via xScale/yScale.
// Covariance fit uses IQR-trimmed core to avoid outlier inflation.
function ellipsePathFromPoints(
  points: BetaPoint[],
  xScale: d3.ScaleLinear<number, number>,
  yScale: d3.ScaleLinear<number, number>,
  nStd = 2.0,
): string | null {
  if (points.length < 3) return null;

  // IQR-trim core to exclude extreme outliers from covariance estimation
  const sortedX = [...points].map((p) => p.x).sort((a, b) => a - b);
  const sortedY = [...points].map((p) => p.y).sort((a, b) => a - b);
  const q1x = d3.quantile(sortedX, 0.25) ?? 0; const q3x = d3.quantile(sortedX, 0.75) ?? 0;
  const q1y = d3.quantile(sortedY, 0.25) ?? 0; const q3y = d3.quantile(sortedY, 0.75) ?? 0;
  const core = points.filter((p) =>
    p.x >= q1x - 2.5 * (q3x - q1x) && p.x <= q3x + 2.5 * (q3x - q1x) &&
    p.y >= q1y - 2.5 * (q3y - q1y) && p.y <= q3y + 2.5 * (q3y - q1y),
  );
  if (core.length < 3) return null;

  const n = core.length;
  const mx = d3.mean(core, (p) => p.x) ?? 0;
  const my = d3.mean(core, (p) => p.y) ?? 0;
  const dxs = core.map((p) => p.x - mx);
  const dys = core.map((p) => p.y - my);

  // Sample covariance (n-1 denominator) — matches numpy.cov
  const f = n / (n - 1);
  const covXX = (d3.mean(dxs.map((v) => v * v)) ?? 0) * f;
  const covYY = (d3.mean(dys.map((v) => v * v)) ?? 0) * f;
  const covXY = (d3.mean(dxs.map((v, i) => v * dys[i]!)) ?? 0) * f;

  // Eigendecomposition in DATA space — same as np.linalg.eigh then descending sort
  const trace = covXX + covYY;
  const det = covXX * covYY - covXY * covXY;
  const gap = Math.sqrt(Math.max(0, trace * trace / 4 - det));
  const lambda1 = trace / 2 + gap; // larger eigenvalue
  const lambda2 = trace / 2 - gap;

  // Angle of major eigenvector = arctan2(v[1], v[0]) where v = [covXY, λ1-covXX]
  // (equivalent to: arctan2(*vecs[:,0][::-1]))
  const theta = Math.atan2(lambda1 - covXX, covXY || 1e-9);

  // Semi-axes in data space
  const rx = nStd * Math.sqrt(Math.max(lambda1, 1e-9));
  const ry = nStd * Math.sqrt(Math.max(lambda2, 1e-9));

  // Parameterise ellipse in data space, project to pixel space via xScale/yScale
  const N = 64;
  const segs: string[] = [];
  for (let i = 0; i <= N; i++) {
    const t = (i / N) * 2 * Math.PI;
    const dx = rx * Math.cos(t) * Math.cos(theta) - ry * Math.sin(t) * Math.sin(theta);
    const dy = rx * Math.cos(t) * Math.sin(theta) + ry * Math.sin(t) * Math.cos(theta);
    segs.push(`${i === 0 ? "M" : "L"} ${xScale(mx + dx)} ${yScale(my + dy)}`);
  }
  return segs.join(" ") + " Z";
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

    const pathA = ellipsePathFromPoints(groupA, x, y);
    if (pathA) {
      g.append("path")
        .attr("d", pathA)
        .attr("fill", "rgba(34, 197, 94, 0.10)")
        .attr("stroke", "var(--secondary)")
        .attr("stroke-width", 1.2);
    }

    const pathB = ellipsePathFromPoints(groupB, x, y);
    if (pathB) {
      g.append("path")
        .attr("d", pathB)
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

    // PERMANOVA stats box
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
