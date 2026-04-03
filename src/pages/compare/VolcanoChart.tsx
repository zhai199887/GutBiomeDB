/**
 * VolcanoChart – volcano plot (log₂FC vs −log₁₀ adj.p)
 * 火山图
 */
import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import type { DiffResult, DiffTaxon } from "./types";
import classes from "../ComparePage.module.css";

const VolcanoChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const data = result.diff_taxa;
    const margin = { top: 30, right: 40, bottom: 60, left: 60 };
    const W = 600, H = 400;
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const xExt = d3.max(data, (d) => Math.abs(d.log2fc)) ?? 1;
    const negLogP = data.map((d) => -Math.log10(Math.max(d.adjusted_p, 1e-300)));
    const yMax = d3.max(negLogP) ?? 5;

    const xScale = d3.scaleLinear().domain([-xExt, xExt]).range([0, iW]);
    const yScale = d3.scaleLinear().domain([0, yMax * 1.05]).range([iH, 0]);

    // Color by significance / 按显著性上色
    const getColor = (d: DiffTaxon) => {
      const sig = d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1;
      if (!sig) return "var(--gray)";
      return d.log2fc > 0 ? "var(--secondary)" : "var(--primary)";
    };

    // Threshold lines / 阈值线
    const pThresh = -Math.log10(0.05);
    g.append("line")
      .attr("x1", 0).attr("x2", iW)
      .attr("y1", yScale(pThresh)).attr("y2", yScale(pThresh))
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);
    g.append("text").attr("x", iW - 2).attr("y", yScale(pThresh) - 4)
      .attr("text-anchor", "end").attr("font-size", 9).attr("fill", "var(--light-gray)")
      .text("adj.p=0.05");

    g.append("line")
      .attr("x1", xScale(-1)).attr("x2", xScale(-1))
      .attr("y1", 0).attr("y2", iH)
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);
    g.append("text").attr("x", xScale(-1) - 3).attr("y", 10)
      .attr("text-anchor", "end").attr("font-size", 9).attr("fill", "var(--light-gray)")
      .text("|log₂FC|=1");

    g.append("line")
      .attr("x1", xScale(1)).attr("x2", xScale(1))
      .attr("y1", 0).attr("y2", iH)
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);
    g.append("text").attr("x", xScale(1) + 3).attr("y", 10)
      .attr("text-anchor", "start").attr("font-size", 9).attr("fill", "var(--light-gray)")
      .text("|log₂FC|=1");

    // Points / 散点
    g.selectAll(".dot")
      .data(data)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (d) => xScale(d.log2fc))
      .attr("cy", (_, i) => yScale(negLogP[i]!))
      .attr("r", (d) => d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1 ? 5 : 3)
      .attr("fill", getColor)
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d, i) =>
        renderToString(
          <div className="tooltip-table">
            <span>Taxon</span><span>{d.taxon}</span>
            <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
            <span>−log₁₀(adj.p)</span><span>{negLogP[i]!.toFixed(2)}</span>
            <span>adj.p</span><span>{d.adjusted_p.toExponential(2)}</span>
          </div>
        )
      );

    // Labels for top significant points / 为最显著的点添加标签
    const topSig = data
      .filter((d) => d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1)
      .sort((a, b) => a.adjusted_p - b.adjusted_p)
      .slice(0, 8);

    g.selectAll(".label")
      .data(topSig)
      .join("text")
      .attr("class", "label")
      .attr("x", (d) => xScale(d.log2fc) + 6)
      .attr("y", (d) => yScale(-Math.log10(Math.max(d.adjusted_p, 1e-300))) - 4)
      .attr("font-size", 9)
      .attr("fill", "var(--white)")
      .text((d) => d.taxon.slice(0, 15));

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).ticks(6))
      .attr("font-size", 11);
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);

    // Axis labels / 坐标轴标签
    svg.append("text")
      .attr("x", W / 2).attr("y", H - 10)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("log₂ Fold Change");
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("−log₁₀(adj. p)");
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default VolcanoChart;
