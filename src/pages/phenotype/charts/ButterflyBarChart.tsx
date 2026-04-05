/**
 * ButterflyBarChart — upgraded butterfly chart with phylum colors and significance markers
 * 升级版蝴蝶对称条形图：门级颜色编码 + 显著性标记 + log2FC标注
 * Inspired by GMrepo v3 / ResMicroDb phenotype association visualizations
 */
import { useEffect, useRef } from "react";
import * as d3 from "d3";
import { renderToString } from "react-dom/server";
import { useI18n } from "@/i18n";
import type { PhenotypeAssociationResult } from "../types";
import { getPhylumColor, sigLabel } from "../types";

interface Props {
  results: PhenotypeAssociationResult[];
  groupA?: string;
  groupB?: string;
  labelA: string;
  labelB: string;
  onTaxonClick?: (taxon: string) => void;
  showOnlySig?: boolean;
}

export default function ButterflyBarChart({
  results, labelA, labelB, onTaxonClick, showOnlySig = false,
}: Props) {
  const ref = useRef<SVGSVGElement>(null);
  const { locale } = useI18n();

  useEffect(() => {
    if (!ref.current || results.length === 0) return;
    draw(ref.current, results, labelA, labelB, onTaxonClick, showOnlySig, locale);
  }, [results, labelA, labelB, showOnlySig, locale, onTaxonClick]);

  return (
    <svg ref={ref} style={{ width: "100%", maxWidth: 900, display: "block" }} />
  );
}

function draw(
  svgEl: SVGSVGElement,
  allResults: PhenotypeAssociationResult[],
  labelA: string,
  labelB: string,
  onTaxonClick?: (taxon: string) => void,
  showOnlySig = false,
  locale = "en",
) {
  const data = (showOnlySig ? allResults.filter(r => r.adjusted_p < 0.05) : allResults).slice(0, 40);
  if (data.length === 0) return;

  d3.select(svgEl).selectAll("*").remove();

  const margin = { top: 30, right: 120, bottom: 60, left: 10 };
  const barH = 20;
  const H = data.length * barH + margin.top + margin.bottom;
  const W = 900;
  const halfW = (W - margin.left - margin.right) / 2 - 30; // half bar area width
  const centerX = margin.left + halfW + 30; // center axis x

  d3.select(svgEl).attr("viewBox", `0 0 ${W} ${H}`);

  const svg = d3.select(svgEl)
    .append("g")
    .attr("transform", `translate(0,${margin.top})`);

  const xMax = d3.max(data, d => Math.max(d.mean_a, d.mean_b)) ?? 0.01;
  const xScaleA = d3.scaleLinear().domain([0, xMax]).range([0, halfW]);
  const xScaleB = d3.scaleLinear().domain([0, xMax]).range([0, halfW]);

  const yScale = d3.scaleBand()
    .domain(data.map(d => d.taxon))
    .range([0, data.length * barH])
    .padding(0.15);

  // Group A bars (left)
  svg.selectAll(".bar-a")
    .data(data)
    .join("rect")
    .attr("class", "bar-a")
    .attr("x", d => centerX - xScaleA(d.mean_a))
    .attr("y", d => yScale(d.taxon) ?? 0)
    .attr("width", d => xScaleA(d.mean_a))
    .attr("height", yScale.bandwidth())
    .attr("fill", d => getPhylumColor(d.phylum))
    .attr("opacity", 0.75)
    .attr("role", "graphics-symbol")
    .attr("style", "cursor:pointer")
    .attr("data-tooltip", d => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span>{d.taxon}</span>
        <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{d.phylum || "—"}</span>
        <span>{labelA}</span><span>{d.mean_a.toFixed(4)}%</span>
        <span>{locale === "zh" ? "流行率" : "Prevalence"}</span><span>{(d.prevalence_a * 100).toFixed(1)}%</span>
        <span>adj.p</span><span>{d.adjusted_p < 0.001 ? "<0.001" : d.adjusted_p.toFixed(3)}</span>
      </div>
    ))
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Group B bars (right)
  svg.selectAll(".bar-b")
    .data(data)
    .join("rect")
    .attr("class", "bar-b")
    .attr("x", centerX)
    .attr("y", d => yScale(d.taxon) ?? 0)
    .attr("width", d => xScaleB(d.mean_b))
    .attr("height", yScale.bandwidth())
    .attr("fill", d => getPhylumColor(d.phylum))
    .attr("opacity", 0.45)
    .attr("role", "graphics-symbol")
    .attr("style", "cursor:pointer")
    .attr("data-tooltip", d => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span>{d.taxon}</span>
        <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{d.phylum || "—"}</span>
        <span>{labelB}</span><span>{d.mean_b.toFixed(4)}%</span>
        <span>{locale === "zh" ? "流行率" : "Prevalence"}</span><span>{(d.prevalence_b * 100).toFixed(1)}%</span>
        <span>adj.p</span><span>{d.adjusted_p < 0.001 ? "<0.001" : d.adjusted_p.toFixed(3)}</span>
      </div>
    ))
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Taxon labels in center
  svg.selectAll(".taxon-label")
    .data(data)
    .join("text")
    .attr("class", "taxon-label")
    .attr("x", centerX)
    .attr("y", d => (yScale(d.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", Math.min(11, barH * 0.62))
    .attr("fill", "currentColor")
    .attr("font-style", "italic")
    .text(d => d.taxon)
    .attr("style", "cursor:pointer")
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Significance markers to right of center B bar
  svg.selectAll(".sig-marker")
    .data(data)
    .join("text")
    .attr("class", "sig-marker")
    .attr("x", d => centerX + xScaleB(d.mean_b) + 4)
    .attr("y", d => (yScale(d.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 9)
    .attr("fill", d => d.adjusted_p < 0.05 ? "#ff6b6b" : "var(--light-gray)")
    .text(d => sigLabel(d.adjusted_p));

  // log2FC labels on far right
  svg.selectAll(".fc-label")
    .data(data)
    .join("text")
    .attr("class", "fc-label")
    .attr("x", W - margin.right + 5)
    .attr("y", d => (yScale(d.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 9)
    .attr("fill", d => d.log2fc > 0 ? "#4dabf7" : "#ff6b6b")
    .text(d => `${d.log2fc > 0 ? "+" : ""}${d.log2fc.toFixed(2)}`);

  // Center axis line
  svg.append("line")
    .attr("x1", centerX).attr("x2", centerX)
    .attr("y1", 0).attr("y2", data.length * barH)
    .attr("stroke", "currentColor").attr("stroke-width", 0.8).attr("opacity", 0.4);

  // X axis A (left, reversed)
  const axisA = d3.axisTop(d3.scaleLinear().domain([xMax, 0]).range([0, halfW]))
    .ticks(3).tickFormat(v => `${Number(v).toFixed(2)}%`);
  svg.append("g")
    .attr("transform", `translate(${centerX - halfW},0)`)
    .call(axisA)
    .attr("font-size", 9);

  // X axis B (right)
  const axisB = d3.axisTop(xScaleB)
    .ticks(3).tickFormat(v => `${Number(v).toFixed(2)}%`);
  svg.append("g")
    .attr("transform", `translate(${centerX},0)`)
    .call(axisB)
    .attr("font-size", 9);

  // Group labels at top
  svg.append("text")
    .attr("x", centerX - halfW / 2).attr("y", -18)
    .attr("text-anchor", "middle")
    .attr("font-size", 11).attr("font-weight", 600)
    .attr("fill", "currentColor")
    .text(`← ${labelA}`);

  svg.append("text")
    .attr("x", centerX + halfW / 2).attr("y", -18)
    .attr("text-anchor", "middle")
    .attr("font-size", 11).attr("font-weight", 600)
    .attr("fill", "currentColor")
    .text(`${labelB} →`);

  // log2FC column header
  svg.append("text")
    .attr("x", W - margin.right + 5).attr("y", -18)
    .attr("font-size", 9).attr("fill", "var(--light-gray)")
    .text("log2FC");

  // Legend (bottom): phylum colors
  const uniquePhyla = [...new Set(data.map(d => d.phylum).filter(Boolean))].slice(0, 6);
  const legY = data.length * barH + 15;
  uniquePhyla.forEach((phylum, i) => {
    const lx = margin.left + i * 130;
    svg.append("rect").attr("x", lx).attr("y", legY).attr("width", 10).attr("height", 10)
      .attr("fill", getPhylumColor(phylum));
    svg.append("text").attr("x", lx + 14).attr("y", legY + 9)
      .attr("font-size", 9).attr("fill", "currentColor").text(phylum);
  });
}
