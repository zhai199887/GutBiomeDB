/**
 * LollipopChart — log2FC vs -log10(adj.p), size=prevalence, color=phylum
 * 效应量Lollipop图：参照GMrepo v3 lollipop可视化设计
 * X: log2FC, Y: -log10(adjusted_p), 点大小 = 流行率, 颜色 = 门
 */
import { useEffect, useRef } from "react";
import * as d3 from "d3";
import { renderToString } from "react-dom/server";
import { useI18n } from "@/i18n";
import type { PhenotypeAssociationResult } from "../types";
import { getPhylumColor } from "../types";

interface Props {
  results: PhenotypeAssociationResult[];
  labelA: string;
  labelB: string;
  onTaxonClick?: (taxon: string) => void;
}

export default function LollipopChart({ results, labelA, labelB, onTaxonClick }: Props) {
  const ref = useRef<SVGSVGElement>(null);
  const { locale } = useI18n();

  useEffect(() => {
    if (!ref.current || results.length === 0) return;
    draw(ref.current, results, labelA, labelB, onTaxonClick, locale);
  }, [results, labelA, labelB, locale, onTaxonClick]);

  return <svg ref={ref} style={{ width: "100%", maxWidth: 820, display: "block" }} />;
}

function draw(
  svgEl: SVGSVGElement,
  results: PhenotypeAssociationResult[],
  labelA: string,
  labelB: string,
  onTaxonClick?: (taxon: string) => void,
  locale = "en",
) {
  d3.select(svgEl).selectAll("*").remove();

  const margin = { top: 30, right: 30, bottom: 60, left: 60 };
  const W = 820, H = 480;
  d3.select(svgEl).attr("viewBox", `0 0 ${W} ${H}`);

  const inner_w = W - margin.left - margin.right;
  const inner_h = H - margin.top - margin.bottom;

  const svg = d3.select(svgEl).append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);

  const data = results
    .filter(r => isFinite(r.log2fc) && isFinite(r.p_value) && r.p_value > 0)
    .slice(0, 300);

  const xDomain = d3.extent(data, d => d.log2fc) as [number, number];
  const xPad = Math.max(Math.abs(xDomain[0]), Math.abs(xDomain[1])) * 0.15;
  const xScale = d3.scaleLinear()
    .domain([xDomain[0] - xPad, xDomain[1] + xPad])
    .range([0, inner_w]);

  const yMax = d3.max(data, d => -Math.log10(d.adjusted_p + 1e-300)) ?? 5;
  const yScale = d3.scaleLinear().domain([0, yMax * 1.1]).range([inner_h, 0]);

  const maxPrev = d3.max(data, d => Math.max(d.prevalence_a, d.prevalence_b)) ?? 1;
  const rScale = d3.scaleSqrt().domain([0, maxPrev]).range([2, 9]);

  // Reference lines
  // x=0 vertical
  svg.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", inner_h)
    .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3")
    .attr("stroke-width", 1).attr("opacity", 0.5);

  // x=±1 fold change reference
  [-1, 1].forEach(v => {
    if (xScale(v) >= 0 && xScale(v) <= inner_w) {
      svg.append("line")
        .attr("x1", xScale(v)).attr("x2", xScale(v))
        .attr("y1", 0).attr("y2", inner_h)
        .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "2,4")
        .attr("stroke-width", 0.7).attr("opacity", 0.35);
    }
  });

  // y = -log10(0.05) significance threshold
  const sigY = -Math.log10(0.05);
  if (sigY <= yMax * 1.1) {
    svg.append("line")
      .attr("x1", 0).attr("x2", inner_w)
      .attr("y1", yScale(sigY)).attr("y2", yScale(sigY))
      .attr("stroke", "#ff6b6b").attr("stroke-dasharray", "4,3")
      .attr("stroke-width", 1).attr("opacity", 0.5);
    svg.append("text")
      .attr("x", inner_w - 2).attr("y", yScale(sigY) - 4)
      .attr("text-anchor", "end").attr("font-size", 9)
      .attr("fill", "#ff6b6b").text("adj.p=0.05");
  }

  // Lollipop lines
  svg.selectAll(".lollipop-line")
    .data(data)
    .join("line")
    .attr("class", "lollipop-line")
    .attr("x1", d => xScale(d.log2fc))
    .attr("x2", d => xScale(d.log2fc))
    .attr("y1", yScale(0))
    .attr("y2", d => yScale(-Math.log10(d.adjusted_p + 1e-300)))
    .attr("stroke", d => getPhylumColor(d.phylum))
    .attr("stroke-width", 1.2)
    .attr("opacity", 0.45);

  // Points
  svg.selectAll(".lollipop-dot")
    .data(data)
    .join("circle")
    .attr("class", "lollipop-dot")
    .attr("cx", d => xScale(d.log2fc))
    .attr("cy", d => yScale(-Math.log10(d.adjusted_p + 1e-300)))
    .attr("r", d => rScale(Math.max(d.prevalence_a, d.prevalence_b)))
    .attr("fill", d => getPhylumColor(d.phylum))
    .attr("opacity", d => d.adjusted_p < 0.05 ? 0.9 : 0.4)
    .attr("stroke", d => d.adjusted_p < 0.05 ? "white" : "none")
    .attr("stroke-width", 0.8)
    .attr("role", "graphics-symbol")
    .attr("style", "cursor:pointer")
    .attr("data-tooltip", d => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span style={{ fontStyle: "italic" }}>{d.taxon}</span>
        <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{d.phylum || "—"}</span>
        <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
        <span>adj.p</span><span>{d.adjusted_p < 0.001 ? "<0.001" : d.adjusted_p.toFixed(4)}</span>
        <span>{locale === "zh" ? "流行率A" : "Prevalence A"}</span><span>{(d.prevalence_a * 100).toFixed(1)}%</span>
        <span>{locale === "zh" ? "流行率B" : "Prevalence B"}</span><span>{(d.prevalence_b * 100).toFixed(1)}%</span>
        <span>{locale === "zh" ? "效应量" : "Effect Size"}</span><span>{d.effect_size.toFixed(3)}</span>
      </div>
    ))
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Label top 5 significant points
  const top5 = [...data]
    .filter(d => d.adjusted_p < 0.05)
    .sort((a, b) => a.adjusted_p - b.adjusted_p)
    .slice(0, 5);

  svg.selectAll(".top-label")
    .data(top5)
    .join("text")
    .attr("class", "top-label")
    .attr("x", d => xScale(d.log2fc) + (d.log2fc > 0 ? 5 : -5))
    .attr("y", d => yScale(-Math.log10(d.adjusted_p + 1e-300)) - 6)
    .attr("text-anchor", d => d.log2fc > 0 ? "start" : "end")
    .attr("font-size", 9).attr("font-style", "italic")
    .attr("fill", "currentColor").text(d => d.taxon);

  // Summary annotation: enriched counts
  const nA = data.filter(d => d.adjusted_p < 0.05 && d.enriched_in === "a").length;
  const nB = data.filter(d => d.adjusted_p < 0.05 && d.enriched_in === "b").length;
  svg.append("text")
    .attr("x", inner_w - 4).attr("y", 12)
    .attr("text-anchor", "end").attr("font-size", 10)
    .attr("fill", "var(--light-gray)")
    .text(locale === "zh"
      ? `富集于${labelA}: ${nA} | 富集于${labelB}: ${nB}`
      : `Enriched in ${labelA}: ${nA} | Enriched in ${labelB}: ${nB}`);

  // Axes
  svg.append("g")
    .attr("transform", `translate(0,${inner_h})`)
    .call(d3.axisBottom(xScale).ticks(6).tickFormat(v => `${Number(v) > 0 ? "+" : ""}${Number(v).toFixed(1)}`))
    .attr("font-size", 10);

  svg.append("g")
    .call(d3.axisLeft(yScale).ticks(5))
    .attr("font-size", 10);

  // Axis labels
  svg.append("text")
    .attr("x", inner_w / 2).attr("y", inner_h + 44)
    .attr("text-anchor", "middle").attr("font-size", 11)
    .attr("fill", "currentColor")
    .text(locale === "zh" ? `log2FC（← 富集于${labelB} | 富集于${labelA} →）` : `log2FC (← Enriched in ${labelB} | Enriched in ${labelA} →)`);

  svg.append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", -inner_h / 2).attr("y", -45)
    .attr("text-anchor", "middle").attr("font-size", 11)
    .attr("fill", "currentColor")
    .text("-log₁₀(adj.p)");
}
