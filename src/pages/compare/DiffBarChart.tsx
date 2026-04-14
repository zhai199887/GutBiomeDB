import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";

import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { phylumColor } from "@/util/phylumColors";

import classes from "../ComparePage.module.css";
import type { DiffResult } from "./types";

const DiffBarChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const data = [...result.diff_taxa]
      .sort((a, b) => a.adjusted_p - b.adjusted_p || Math.abs(b.log2fc) - Math.abs(a.log2fc))
      .slice(0, 30)
      .sort((a, b) => a.log2fc - b.log2fc);

    const lvl = (result.summary?.taxonomy_level || "genus").toLowerCase();
    const unitEn = lvl === "phylum" ? "phyla" : lvl === "family" ? "families" : "genera";
    const unitZh = lvl === "phylum" ? "差异门" : lvl === "family" ? "差异科" : "差异属";

    if (!data.length) {
      svg.attr("viewBox", "0 0 720 120");
      svg.append("text")
        .attr("x", 20)
        .attr("y", 60)
        .attr("fill", "currentColor")
        .attr("font-size", 14)
        .text(locale === "zh" ? `没有可展示的${unitZh}` : `No ${unitEn} available for display`);
      return;
    }

    const margin = { top: 40, right: 220, bottom: 52, left: 340 };
    const width = 1280;
    const height = Math.max(440, data.length * 28 + margin.top + margin.bottom);
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const extent = d3.max(data, (d) => Math.abs(d.log2fc)) ?? 1;
    const x = d3.scaleLinear().domain([-extent, extent]).nice().range([0, innerWidth]);
    const y = d3.scaleBand<string>()
      .domain(data.map((d) => d.taxon))
      .range([0, innerHeight])
      .padding(0.18);

    g.append("line")
      .attr("x1", x(0))
      .attr("x2", x(0))
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .attr("stroke", "var(--gray)")
      .attr("stroke-dasharray", "4,4");

    g.selectAll("rect")
      .data(data)
      .join("rect")
      .attr("x", (d) => x(Math.min(0, d.log2fc)))
      .attr("y", (d) => y(d.taxon) ?? 0)
      .attr("width", (d) => Math.abs(x(d.log2fc) - x(0)))
      .attr("height", y.bandwidth())
      .attr("rx", 4)
      .attr("fill", (d) => phylumColor(d.phylum))
      .attr("opacity", 0.9)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) => renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span>{d.taxon}</span>
          <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{d.phylum}</span>
          <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
          <span>{locale === "zh" ? "均值 A" : "Mean A"}</span><span>{d.mean_a.toFixed(3)}%</span>
          <span>{locale === "zh" ? "均值 B" : "Mean B"}</span><span>{d.mean_b.toFixed(3)}%</span>
          <span>{locale === "zh" ? "流行率 A" : "Prev A"}</span><span>{(d.prevalence_a * 100).toFixed(1)}%</span>
          <span>{locale === "zh" ? "流行率 B" : "Prev B"}</span><span>{(d.prevalence_b * 100).toFixed(1)}%</span>
          <span>{locale === "zh" ? "校正 p 值" : "Adj. p"}</span><span>{d.adjusted_p.toExponential(2)}</span>
          <span>{locale === "zh" ? "效应量" : "Effect size"}</span><span>{d.effect_size.toFixed(3)}</span>
        </div>,
      ));

    g.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(d3.axisBottom(x).ticks(6))
      .attr("font-size", 12);

    g.append("g")
      .call(
        d3.axisLeft(y).tickFormat((value) => value.length > 38 ? `${value.slice(0, 36)}…` : value),
      )
      .attr("font-size", 12);

    svg.append("text")
      .attr("x", margin.left + innerWidth / 2)
      .attr("y", height - 8)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 12)
      .text(locale === "zh" ? "log2 差异倍数变化" : "log2 Fold Change");

    svg.append("text")
      .attr("x", margin.left + innerWidth / 2)
      .attr("y", 18)
      .attr("text-anchor", "middle")
      .attr("fill", "var(--light-gray)")
      .attr("font-size", 12)
      .text(locale === "zh" ? `Top 30 ${unitZh}，按门着色` : `Top 30 differential ${unitEn}, colored by phylum`);

    const legend = Array.from(new Set(data.map((d) => d.phylum))).slice(0, 6);
    legend.forEach((phylum, index) => {
      const x0 = width - margin.right + 10;
      const y0 = margin.top + index * 18;
      svg.append("rect")
        .attr("x", x0)
        .attr("y", y0 - 8)
        .attr("width", 10)
        .attr("height", 10)
        .attr("rx", 2)
        .attr("fill", phylumColor(phylum));
      svg.append("text")
        .attr("x", x0 + 16)
        .attr("y", y0)
        .attr("fill", "var(--light-gray)")
        .attr("font-size", 10)
        .text(phylum);
    });

    svg.attr("viewBox", `0 0 ${width} ${height}`);
  }, [locale, result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default DiffBarChart;
