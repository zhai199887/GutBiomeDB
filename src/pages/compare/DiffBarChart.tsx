/**
 * DiffBarChart – differential abundance bar chart (log₂FC)
 * 差异丰度柱状图
 */
import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import type { DiffResult } from "./types";
import classes from "../ComparePage.module.css";

const DiffBarChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Show top 30 significant taxa / 展示前30个显著差异分类群
    const data = result.diff_taxa
      .filter((t) => t.adjusted_p < 0.05)
      .slice(0, 30)
      .sort((a, b) => a.log2fc - b.log2fc);

    if (data.length === 0) {
      svg.attr("viewBox", "0 0 700 100");
      svg.append("text").attr("x", 20).attr("y", 60)
        .text("No significant taxa (adjusted p < 0.05)")
        .attr("fill", "currentColor").attr("font-size", 14);
      return;
    }

    const margin = { top: 20, right: 120, bottom: 40, left: 160 };
    const W = 700 - margin.left - margin.right;
    const H = Math.max(300, data.length * 22);
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xExtent = d3.max(data, (d) => Math.abs(d.log2fc)) ?? 1;
    const xScale = d3.scaleLinear().domain([-xExtent, xExtent]).range([0, W]);
    const yScale = d3.scaleBand().domain(data.map((d) => d.taxon)).range([0, H]).padding(0.2);

    // Color bars by direction / 按方向上色
    g.selectAll(".bar")
      .data(data)
      .join("rect")
      .attr("class", "bar")
      .attr("x", (d) => d.log2fc < 0 ? xScale(d.log2fc) : xScale(0))
      .attr("y", (d) => yScale(d.taxon) ?? 0)
      .attr("width", (d) => Math.abs(xScale(d.log2fc) - xScale(0)))
      .attr("height", yScale.bandwidth())
      .attr("fill", (d) => d.log2fc > 0 ? "var(--secondary)" : "var(--primary)")
      .attr("opacity", 0.85)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Taxon</span><span>{d.taxon}</span>
            <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
            <span>adj.p</span><span>{d.adjusted_p.toExponential(2)}</span>
            <span>Mean A</span><span>{(d.mean_a * 100).toFixed(3)}%</span>
            <span>Mean B</span><span>{(d.mean_b * 100).toFixed(3)}%</span>
          </div>
        )
      );

    // Center axis / 中心轴
    g.append("line")
      .attr("x1", xScale(0)).attr("x2", xScale(0))
      .attr("y1", 0).attr("y2", H)
      .attr("stroke", "currentColor").attr("stroke-width", 1).attr("opacity", 0.4);

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${H})`)
      .call(d3.axisBottom(xScale).ticks(6).tickFormat((d) => `${d}`))
      .attr("font-size", 11);
    g.append("g")
      .call(d3.axisLeft(yScale).tickFormat((d) => d.length > 18 ? d.slice(0, 16) + "…" : d))
      .attr("font-size", 11);

    // Group labels / 组标签
    g.append("text").attr("x", xScale(-xExtent / 2)).attr("y", -8)
      .attr("text-anchor", "middle").attr("fill", "var(--primary)")
      .attr("font-size", 12).text(`← ${result.summary.group_b_name}`);
    g.append("text").attr("x", xScale(xExtent / 2)).attr("y", -8)
      .attr("text-anchor", "middle").attr("fill", "var(--secondary)")
      .attr("font-size", 12).text(`${result.summary.group_a_name} →`);

    // X-axis label / X轴标签
    svg.append("text")
      .attr("x", margin.left + W / 2).attr("y", H + margin.top + margin.bottom - 8)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 11).text("log₂ Fold Change");

    // Set viewBox / 设置viewBox
    svg.attr("viewBox", `0 0 ${W + margin.left + margin.right} ${H + margin.top + margin.bottom}`);

    // Method note / 方法注释
    const totalW = W + margin.left + margin.right;
    svg.append("text").attr("x", totalW - 4).attr("y", H + margin.top + margin.bottom - 4)
      .attr("text-anchor", "end").attr("font-size", 10).attr("fill", "var(--light-gray)")
      .text(`Showing taxa with adj. p < 0.05 (BH FDR) · ${result.summary.method}`);
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default DiffBarChart;
