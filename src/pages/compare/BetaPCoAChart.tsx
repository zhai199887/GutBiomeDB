/**
 * BetaPCoAChart – beta diversity PCoA scatter plot (Bray-Curtis)
 * Beta多样性PCoA散点图
 */
import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import type { DiffResult } from "./types";
import classes from "../ComparePage.module.css";

const BetaPCoAChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const coords = result.beta_diversity.pcoa_coords;
    if (!coords.length) {
      svg.attr("viewBox", "0 0 560 100");
      svg.append("text").attr("x", 20).attr("y", 40)
        .text("PCoA data not available").attr("fill", "currentColor").attr("font-size", 14);
      return;
    }

    const W = 560, H = 420;
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const margin = { top: 30, right: 120, bottom: 60, left: 60 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xExtent = d3.extent(coords, (d) => d.x) as [number, number];
    const yExtent = d3.extent(coords, (d) => d.y) as [number, number];

    const xScale = d3.scaleLinear().domain(xExtent).nice().range([0, iW]);
    const yScale = d3.scaleLinear().domain(yExtent).nice().range([iH, 0]);

    // Points / 散点
    g.selectAll(".dot")
      .data(coords)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (d) => xScale(d.x))
      .attr("cy", (d) => yScale(d.y))
      .attr("r", 5)
      .attr("fill", (d) => d.group === "A" ? "var(--secondary)" : "var(--primary)")
      .attr("opacity", 0.7)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Group</span>
            <span style={{ color: d.group === "A" ? "var(--secondary)" : "var(--primary)" }}>
              {d.group === "A" ? result.summary.group_a_name : result.summary.group_b_name}
            </span>
            <span>PCo1</span><span>{d.x.toFixed(4)}</span>
            <span>PCo2</span><span>{d.y.toFixed(4)}</span>
          </div>
        )
      );

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).ticks(5)).attr("font-size", 11);
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);

    // Axis labels
    svg.append("text").attr("x", W / 2).attr("y", H - 16)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("PCo1 (Bray-Curtis)");
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("PCo2 (Bray-Curtis)");
    svg.append("text").attr("x", W / 2).attr("y", H - 8)
      .attr("text-anchor", "middle").attr("fill", "var(--light-gray)").attr("font-size", 9)
      .text("Note: % variance explained per axis not yet available");

    // Legend / 图例
    const lx = iW + 10;
    const truncateText = (s: string, max: number) => s.length > max ? s.slice(0, max) + "…" : s;
    svg.append("circle").attr("cx", margin.left + lx + 6).attr("cy", margin.top + 20).attr("r", 6)
      .attr("fill", "var(--secondary)");
    svg.append("text").attr("x", margin.left + lx + 16).attr("y", margin.top + 24)
      .attr("font-size", 11).attr("fill", "var(--secondary)")
      .text(truncateText(result.summary.group_a_name, 12));
    svg.append("circle").attr("cx", margin.left + lx + 6).attr("cy", margin.top + 44).attr("r", 6)
      .attr("fill", "var(--primary)");
    svg.append("text").attr("x", margin.left + lx + 16).attr("y", margin.top + 48)
      .attr("font-size", 11).attr("fill", "var(--primary)")
      .text(truncateText(result.summary.group_b_name, 12));
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default BetaPCoAChart;
