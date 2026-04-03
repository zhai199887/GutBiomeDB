/**
 * AlphaBoxChart – alpha diversity boxplots (Shannon + Simpson)
 * Alpha多样性箱线图
 */
import { useEffect, useRef } from "react";
import * as d3 from "d3";
import type { DiffResult } from "./types";
import classes from "../ComparePage.module.css";

const AlphaBoxChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const margin = { top: 35, right: 20, bottom: 40, left: 50 };
    const W = 620, H = 360;
    const iH = H - margin.top - margin.bottom;

    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const { group_a, group_b } = result.alpha_diversity;
    const gA = result.summary.group_a_name;
    const gB = result.summary.group_b_name;

    const boxW = 80;
    const positions = [
      { x: 60,  data: group_a.shannon, color: "var(--secondary)", label: gA },
      { x: 170, data: group_b.shannon, color: "var(--primary)", label: gB },
      { x: 330, data: group_a.simpson, color: "var(--secondary)", label: gA },
      { x: 440, data: group_b.simpson, color: "var(--primary)", label: gB },
    ];

    const allVals = positions.flatMap((p) => p.data);

    if (allVals.length === 0) {
      svg.attr("viewBox", `0 0 ${W} 100`);
      svg.append("text").attr("x", 20).attr("y", 50)
        .text("No alpha diversity data available")
        .attr("fill", "currentColor").attr("font-size", 13);
      return;
    }

    const yScale = d3.scaleLinear()
      .domain([0, d3.max(allVals) ?? 5])
      .range([iH, 0]);

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const drawBox = (
      grp: d3.Selection<SVGGElement, unknown, null, undefined>,
      vals: number[], cx: number, color: string,
    ): { lower: number; upper: number } => {
      const sorted = [...vals].sort((a, b) => a - b);
      const q1 = d3.quantile(sorted, 0.25) ?? 0;
      const median = d3.quantile(sorted, 0.5) ?? 0;
      const q3 = d3.quantile(sorted, 0.75) ?? 0;
      const iqr = q3 - q1;
      const lower = Math.max(sorted[0]!, q1 - 1.5 * iqr);
      const upper = Math.min(sorted[sorted.length - 1]!, q3 + 1.5 * iqr);

      grp.append("rect")
        .attr("x", cx - boxW / 2).attr("y", yScale(q3))
        .attr("width", boxW).attr("height", Math.abs(yScale(q1) - yScale(q3)))
        .attr("fill", color).attr("opacity", 0.3)
        .attr("stroke", color).attr("stroke-width", 1.5);

      grp.append("line")
        .attr("x1", cx - boxW / 2).attr("x2", cx + boxW / 2)
        .attr("y1", yScale(median)).attr("y2", yScale(median))
        .attr("stroke", color).attr("stroke-width", 2.5);

      for (const [y1, y2] of [[yScale(q1), yScale(lower)], [yScale(q3), yScale(upper)]]) {
        grp.append("line")
          .attr("x1", cx).attr("x2", cx)
          .attr("y1", y1!).attr("y2", y2!)
          .attr("stroke", color).attr("stroke-width", 1.5).attr("stroke-dasharray", "3,2");
      }

      return { lower, upper };
    };

    const drawOutliers = (
      grp: d3.Selection<SVGGElement, unknown, null, undefined>,
      vals: number[], cx: number, color: string,
      lower: number, upper: number,
    ) => {
      const outliers = vals.filter((v) => v < lower || v > upper);
      grp.selectAll(`.out${cx}`)
        .data(outliers)
        .join("circle")
        .attr("class", `out${cx}`)
        .attr("cx", (_, i) => cx + ((i % 3) - 1) * 3)
        .attr("cy", (v) => yScale(v))
        .attr("r", 2.5)
        .attr("fill", "none")
        .attr("stroke", color)
        .attr("opacity", 0.55);
    };

    const fA_sh = drawBox(g, group_a.shannon, 60, "var(--secondary)");
    drawOutliers(g, group_a.shannon, 60, "var(--secondary)", fA_sh.lower, fA_sh.upper);
    const fB_sh = drawBox(g, group_b.shannon, 170, "var(--primary)");
    drawOutliers(g, group_b.shannon, 170, "var(--primary)", fB_sh.lower, fB_sh.upper);
    const fA_si = drawBox(g, group_a.simpson, 330, "var(--secondary)");
    drawOutliers(g, group_a.simpson, 330, "var(--secondary)", fA_si.lower, fA_si.upper);
    const fB_si = drawBox(g, group_b.simpson, 440, "var(--primary)");
    drawOutliers(g, group_b.simpson, 440, "var(--primary)", fB_si.lower, fB_si.upper);

    // Y axis
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 11).text("Diversity Index");

    // Panel titles
    svg.append("text").attr("x", margin.left + 115).attr("y", 22)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 13).attr("font-weight", 600).text("Shannon Index");
    svg.append("text").attr("x", margin.left + 385).attr("y", 22)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 13).attr("font-weight", 600).text("Simpson Index (1-D)");

    // X labels
    for (const pos of positions) {
      svg.append("text")
        .attr("x", pos.x + margin.left).attr("y", margin.top + iH + 18)
        .attr("text-anchor", "middle").attr("fill", pos.color)
        .attr("font-size", 10)
        .text(pos.label.length > 12 ? pos.label.slice(0, 11) + "…" : pos.label);
    }

    // Divider
    svg.append("line")
      .attr("x1", margin.left + 250).attr("x2", margin.left + 250)
      .attr("y1", margin.top).attr("y2", margin.top + iH)
      .attr("stroke", "var(--gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.5);

    // Legend
    const legendX = margin.left + 470;
    const legendY = margin.top + 20;
    const legendItems = [
      { color: "var(--secondary)", label: gA.length > 10 ? gA.slice(0, 9) + "…" : gA },
      { color: "var(--primary)",   label: gB.length > 10 ? gB.slice(0, 9) + "…" : gB },
    ];
    legendItems.forEach(({ color, label }, i) => {
      svg.append("rect")
        .attr("x", legendX).attr("y", legendY + i * 18 - 9)
        .attr("width", 10).attr("height", 10)
        .attr("fill", color).attr("opacity", 0.7).attr("rx", 2);
      svg.append("text")
        .attr("x", legendX + 14).attr("y", legendY + i * 18)
        .attr("font-size", 10).attr("fill", color).text(label);
    });
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default AlphaBoxChart;
