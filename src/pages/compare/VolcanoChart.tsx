import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";

import * as d3 from "d3";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { DiffResult, DiffTaxon, LefseFeature } from "./types";

const formatPValue = (pValue: number, negLog10P: number | undefined, locale: string) => {
  if (!Number.isFinite(pValue)) return locale === "zh" ? "不可用" : "Unavailable";
  if (pValue > 0) {
    return pValue < 0.001 ? pValue.toExponential(2) : pValue.toFixed(3);
  }
  if (Number.isFinite(negLog10P) && negLog10P! > 0) {
    return `< 1e-${Math.floor(negLog10P!)}`;
  }
  return "0";
};

const getNegLog10AdjustedP = (taxon: DiffTaxon) =>
  Number.isFinite(taxon.neg_log10_adjusted_p)
    ? Math.max(0, taxon.neg_log10_adjusted_p!)
    : -Math.log10(Math.max(taxon.adjusted_p, 1e-300));

type PlotPoint = {
  taxon: string;
  x: number;
  adjusted_p: number;
  neg_log10_p: number;
  enriched_in: "A" | "B";
};

const signedLda = (feature: LefseFeature) =>
  feature.signed_lda_score ?? (feature.enriched_group === "A" ? feature.lda_score : -feature.lda_score);

const VolcanoChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const isLefse = result.summary.method === "lefse" && Boolean(result.lefse_results?.length);
    const data: PlotPoint[] = isLefse
      ? (result.lefse_results ?? []).map((feature) => {
          const adjustedP = feature.adjusted_p ?? feature.p_value;
          const rawNegLogP = adjustedP > 0 && Number.isFinite(adjustedP) ? -Math.log10(adjustedP) : 50;
          return {
            taxon: feature.taxon,
            x: signedLda(feature),
            adjusted_p: adjustedP,
            // Keep zero/underflow p values visible without letting one point flatten the whole chart.
            neg_log10_p: Math.min(50, Math.max(0, rawNegLogP)),
            enriched_in: feature.enriched_group,
          };
        })
      : result.diff_taxa.map((taxon) => ({
          taxon: taxon.taxon,
          x: taxon.log2fc,
          adjusted_p: taxon.adjusted_p,
          neg_log10_p: getNegLog10AdjustedP(taxon),
          enriched_in: taxon.enriched_in,
        }));
    const xThreshold = isLefse ? 2 : 1;
    const xLabel = isLefse ? "LDA score (log10)" : "log2 Fold Change";
    const margin = { top: 34, right: 120, bottom: 72, left: 74 };
    const width = 980;
    const height = 560;
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const group = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const xExtent = Math.max(d3.max(data, (point) => Math.abs(point.x)) ?? 0, xThreshold) * 1.08;
    const negLogP = data.map((point) => point.neg_log10_p);
    const pThreshold = -Math.log10(0.05);
    const observedYMax = d3.max(negLogP) ?? 0;
    const yMax = Math.max(Math.min(observedYMax, isLefse ? 50 : observedYMax), pThreshold, 4) * 1.08;

    const xScale = d3.scaleLinear().domain([-xExtent, xExtent]).range([0, innerWidth]);
    // Compress extreme adjusted-p values while keeping the significance region readable.
    const yScale = d3.scalePow().exponent(1 / 3).domain([0, yMax]).range([innerHeight, 0]);

    const getColor = (point: PlotPoint) => {
      const significant = point.adjusted_p < 0.05 && Math.abs(point.x) > xThreshold;
      if (!significant) return "var(--gray)";
      return point.enriched_in === "A" ? "var(--secondary)" : "var(--primary)";
    };

    group.append("line")
      .attr("x1", 0)
      .attr("x2", innerWidth)
      .attr("y1", yScale(pThreshold))
      .attr("y2", yScale(pThreshold))
      .attr("stroke", "var(--light-gray)")
      .attr("stroke-dasharray", "4,3")
      .attr("opacity", 0.6);

    group.append("text")
      .attr("x", innerWidth - 2)
      .attr("y", yScale(pThreshold) - 4)
      .attr("text-anchor", "end")
      .attr("font-size", 9)
      .attr("fill", "var(--light-gray)")
      .text("adj.p=0.05");

    [-xThreshold, xThreshold].forEach((threshold) => {
      group.append("line")
        .attr("x1", xScale(threshold))
        .attr("x2", xScale(threshold))
        .attr("y1", 0)
        .attr("y2", innerHeight)
        .attr("stroke", "var(--light-gray)")
        .attr("stroke-dasharray", "4,3")
        .attr("opacity", 0.6);

      group.append("text")
        .attr("x", xScale(threshold) + (threshold < 0 ? -3 : 3))
        .attr("y", 16)
        .attr("text-anchor", threshold < 0 ? "end" : "start")
        .attr("font-size", 9)
        .attr("fill", "var(--light-gray)")
        .text(isLefse ? `|LDA|=${xThreshold}` : "|log2FC|=1");
    });

    group.selectAll(".dot")
      .data(data)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (point) => xScale(point.x))
      .attr("cy", (point) => yScale(Math.min(point.neg_log10_p, yMax)))
      .attr("r", (point) => point.adjusted_p < 0.05 && Math.abs(point.x) > xThreshold ? 5 : 3)
      .attr("fill", getColor)
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (point) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "属" : "Genus"}</span><span>{point.taxon}</span>
            <span>{xLabel}</span><span>{point.x.toFixed(3)}</span>
            <span>-log10(adj.p)</span><span>{point.neg_log10_p.toFixed(2)}</span>
            <span>{locale === "zh" ? "校正 p 值" : "adj.p"}</span>
            <span>{formatPValue(point.adjusted_p, point.neg_log10_p, locale)}</span>
          </div>,
        )
      );

    const topSignificant = data
      .filter((point) => point.adjusted_p < 0.05 && Math.abs(point.x) > xThreshold)
      .sort((left, right) => right.neg_log10_p - left.neg_log10_p)
      .slice(0, 8);

    group.selectAll(".label")
      .data(topSignificant)
      .join("text")
      .attr("class", "label")
      .attr("x", (point) => xScale(point.x) + 6)
      .attr("y", (point) => Math.max(10, yScale(Math.min(point.neg_log10_p, yMax)) - 4))
      .attr("font-size", 10)
      .attr("fill", "var(--white)")
      .text((point) => (point.taxon.length > 22 ? `${point.taxon.slice(0, 20)}...` : point.taxon));

    group.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(d3.axisBottom(xScale).ticks(6))
      .attr("font-size", 12);

    group.append("g")
      .call(d3.axisLeft(yScale).ticks(6))
      .attr("font-size", 12);

    const legend = svg.append("g").attr("transform", `translate(${width - 178},${margin.top + 6})`);
    [
      { label: locale === "zh" ? "疾病组富集" : "Disease enriched", color: "var(--secondary)" },
      { label: locale === "zh" ? "对照组富集" : "Control enriched", color: "var(--primary)" },
      { label: locale === "zh" ? "未达阈值" : "Below threshold", color: "var(--gray)" },
    ].forEach((item, index) => {
      const y = index * 18;
      legend.append("circle").attr("cx", 0).attr("cy", y).attr("r", 4).attr("fill", item.color);
      legend.append("text").attr("x", 10).attr("y", y + 4).attr("fill", "currentColor").attr("font-size", 11).text(item.label);
    });

    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height - 10)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 13)
      .text(locale === "zh" ? (isLefse ? "LDA 得分 (log10)" : "log2 差异倍数") : xLabel);

    svg.append("text")
      .attr("transform", `translate(14,${height / 2}) rotate(-90)`)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 13)
      .text(locale === "zh" ? "-log10(校正 p)" : "-log10(adj. p)");
  }, [result, locale]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default VolcanoChart;
