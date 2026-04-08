import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";

import * as d3 from "d3";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { DiffResult, DiffTaxon } from "./types";

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

const VolcanoChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const data = result.diff_taxa;
    const margin = { top: 34, right: 120, bottom: 72, left: 74 };
    const width = 980;
    const height = 560;
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const group = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const xExtent = d3.max(data, (taxon) => Math.abs(taxon.log2fc)) ?? 1;
    const negLogP = data.map(getNegLog10AdjustedP);
    const sortedNegLogP = [...negLogP].sort(d3.ascending);
    const pThreshold = -Math.log10(0.05);
    const yMax = Math.max(
      d3.quantile(sortedNegLogP, 0.98) ?? 5,
      pThreshold * 1.7,
      4,
    );

    const xScale = d3.scaleLinear().domain([-xExtent, xExtent]).range([0, innerWidth]);
    const yScale = d3.scaleLinear().domain([0, yMax * 1.05]).range([innerHeight, 0]);

    const getColor = (taxon: DiffTaxon) => {
      const significant = taxon.adjusted_p < 0.05 && Math.abs(taxon.log2fc) > 1;
      if (!significant) return "var(--gray)";
      return taxon.log2fc > 0 ? "var(--secondary)" : "var(--primary)";
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

    [-1, 1].forEach((threshold) => {
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
        .text("|log2FC|=1");
    });

    group.selectAll(".dot")
      .data(data)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (taxon) => xScale(taxon.log2fc))
      .attr("cy", (_, index) => yScale(Math.min(negLogP[index]!, yMax)))
      .attr("r", (taxon) => taxon.adjusted_p < 0.05 && Math.abs(taxon.log2fc) > 1 ? 5 : 3)
      .attr("fill", getColor)
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (taxon, index) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "分类群" : "Taxon"}</span><span>{taxon.taxon}</span>
            <span>log2FC</span><span>{taxon.log2fc.toFixed(3)}</span>
            <span>-log10(adj.p)</span><span>{negLogP[index]!.toFixed(2)}</span>
            <span>{locale === "zh" ? "校正 p 值" : "adj.p"}</span>
            <span>{formatPValue(taxon.adjusted_p, taxon.neg_log10_adjusted_p, locale)}</span>
          </div>,
        )
      );

    const topSignificant = data
      .filter((taxon) => taxon.adjusted_p < 0.05 && Math.abs(taxon.log2fc) > 1)
      .sort((left, right) => {
        const leftScore = left.neg_log10_adjusted_p ?? getNegLog10AdjustedP(left);
        const rightScore = right.neg_log10_adjusted_p ?? getNegLog10AdjustedP(right);
        return rightScore - leftScore;
      })
      .slice(0, 8);

    group.selectAll(".label")
      .data(topSignificant)
      .join("text")
      .attr("class", "label")
      .attr("x", (taxon) => xScale(taxon.log2fc) + 6)
      .attr("y", (taxon) => yScale(Math.min(getNegLog10AdjustedP(taxon), yMax)) - 4)
      .attr("font-size", 10)
      .attr("fill", "var(--white)")
      .text((taxon) => (taxon.taxon.length > 22 ? `${taxon.taxon.slice(0, 20)}...` : taxon.taxon));

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
      .text(locale === "zh" ? "log2 差异倍数" : "log2 Fold Change");

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
