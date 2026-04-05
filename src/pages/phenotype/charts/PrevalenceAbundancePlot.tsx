/**
 * PrevalenceAbundancePlot — prevalence vs mean abundance scatter
 * 流行率-丰度散点图：每个分类群画两点（A/B组），细线连接，展示变化方向
 * Inspired by MicrobiomeDB prevalence-abundance visualization
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

export default function PrevalenceAbundancePlot({ results, labelA, labelB, onTaxonClick }: Props) {
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

  const margin = { top: 30, right: 130, bottom: 60, left: 70 };
  const W = 820, H = 500;
  d3.select(svgEl).attr("viewBox", `0 0 ${W} ${H}`);
  const inner_w = W - margin.left - margin.right;
  const inner_h = H - margin.top - margin.bottom;

  const svg = d3.select(svgEl).append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);

  // Filter to taxa with some variation in prevalence or abundance
  const data = results.filter(r => r.prevalence_a > 0 || r.prevalence_b > 0).slice(0, 150);

  // Scales (log for abundance to handle wide range)
  const xDomain: [number, number] = [0, 1];
  const xScale = d3.scaleLinear().domain(xDomain).range([0, inner_w]);

  const allAbund = data.flatMap(d => [d.mean_a, d.mean_b]).filter(v => v > 0);
  const yMin = Math.max(1e-4, d3.min(allAbund) ?? 1e-4);
  const yMax = d3.max(allAbund) ?? 1;
  const yScale = d3.scaleLog().domain([yMin * 0.5, yMax * 2]).range([inner_h, 0]).clamp(true);

  // Connection lines between A and B per taxon
  svg.selectAll(".connect-line")
    .data(data)
    .join("line")
    .attr("class", "connect-line")
    .attr("x1", d => xScale(d.prevalence_a))
    .attr("y1", d => yScale(Math.max(d.mean_a, yMin * 0.5)))
    .attr("x2", d => xScale(d.prevalence_b))
    .attr("y2", d => yScale(Math.max(d.mean_b, yMin * 0.5)))
    .attr("stroke", d => getPhylumColor(d.phylum))
    .attr("stroke-width", d => d.adjusted_p < 0.05 ? 1.2 : 0.5)
    .attr("opacity", d => d.adjusted_p < 0.05 ? 0.5 : 0.2);

  // Points Group A (filled circle)
  svg.selectAll(".dot-a")
    .data(data)
    .join("circle")
    .attr("class", "dot-a")
    .attr("cx", d => xScale(d.prevalence_a))
    .attr("cy", d => yScale(Math.max(d.mean_a, yMin * 0.5)))
    .attr("r", 4)
    .attr("fill", d => getPhylumColor(d.phylum))
    .attr("opacity", d => d.adjusted_p < 0.05 ? 0.85 : 0.3)
    .attr("stroke", "white").attr("stroke-width", 0.5)
    .attr("role", "graphics-symbol").attr("style", "cursor:pointer")
    .attr("data-tooltip", d => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span style={{ fontStyle: "italic" }}>{d.taxon}</span>
        <span>{labelA} {locale === "zh" ? "流行率" : "Prevalence"}</span><span>{(d.prevalence_a * 100).toFixed(1)}%</span>
        <span>{labelA} {locale === "zh" ? "平均丰度" : "Mean Abund."}</span><span>{d.mean_a.toFixed(4)}%</span>
        <span>adj.p</span><span>{d.adjusted_p < 0.001 ? "<0.001" : d.adjusted_p.toFixed(3)}</span>
      </div>
    ))
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Points Group B (open circle)
  svg.selectAll(".dot-b")
    .data(data)
    .join("circle")
    .attr("class", "dot-b")
    .attr("cx", d => xScale(d.prevalence_b))
    .attr("cy", d => yScale(Math.max(d.mean_b, yMin * 0.5)))
    .attr("r", 4)
    .attr("fill", "none")
    .attr("stroke", d => getPhylumColor(d.phylum))
    .attr("stroke-width", 1.5)
    .attr("opacity", d => d.adjusted_p < 0.05 ? 0.85 : 0.3)
    .attr("role", "graphics-symbol").attr("style", "cursor:pointer")
    .attr("data-tooltip", d => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span style={{ fontStyle: "italic" }}>{d.taxon}</span>
        <span>{labelB} {locale === "zh" ? "流行率" : "Prevalence"}</span><span>{(d.prevalence_b * 100).toFixed(1)}%</span>
        <span>{labelB} {locale === "zh" ? "平均丰度" : "Mean Abund."}</span><span>{d.mean_b.toFixed(4)}%</span>
        <span>adj.p</span><span>{d.adjusted_p < 0.001 ? "<0.001" : d.adjusted_p.toFixed(3)}</span>
      </div>
    ))
    .on("click", (_, d) => onTaxonClick?.(d.taxon));

  // Axes
  svg.append("g")
    .attr("transform", `translate(0,${inner_h})`)
    .call(d3.axisBottom(xScale).tickFormat(v => `${(Number(v) * 100).toFixed(0)}%`).ticks(6))
    .attr("font-size", 10);

  svg.append("g")
    .call(d3.axisLeft(yScale).ticks(5, ".2~s").tickFormat(v => `${Number(v).toFixed(3)}%`))
    .attr("font-size", 10);

  // Axis labels
  svg.append("text")
    .attr("x", inner_w / 2).attr("y", inner_h + 44)
    .attr("text-anchor", "middle").attr("font-size", 11).attr("fill", "currentColor")
    .text(locale === "zh" ? "流行率（样本检出比例）" : "Prevalence (% samples detected)");

  svg.append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", -inner_h / 2).attr("y", -52)
    .attr("text-anchor", "middle").attr("font-size", 11).attr("fill", "currentColor")
    .text(locale === "zh" ? "平均相对丰度（%，对数刻度）" : "Mean Relative Abundance (%, log scale)");

  // Legend
  const legX = inner_w + 10;
  svg.append("circle").attr("cx", legX + 5).attr("cy", 10).attr("r", 4)
    .attr("fill", "var(--light-gray)");
  svg.append("text").attr("x", legX + 13).attr("y", 14).attr("font-size", 9)
    .attr("fill", "currentColor").text(labelA);

  svg.append("circle").attr("cx", legX + 5).attr("cy", 28).attr("r", 4)
    .attr("fill", "none").attr("stroke", "var(--light-gray)").attr("stroke-width", 1.5);
  svg.append("text").attr("x", legX + 13).attr("y", 32).attr("font-size", 9)
    .attr("fill", "currentColor").text(labelB);

  svg.append("rect").attr("x", legX).attr("y", 46).attr("width", 20).attr("height", 1.5)
    .attr("fill", "var(--light-gray)").attr("opacity", 0.6);
  svg.append("text").attr("x", legX + 24).attr("y", 52).attr("font-size", 9)
    .attr("fill", "var(--light-gray)").text(locale === "zh" ? "连接线=同一菌属" : "line = same taxon");
}
