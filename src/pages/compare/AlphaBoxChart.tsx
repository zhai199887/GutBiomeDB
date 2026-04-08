import { useEffect, useRef } from "react";

import * as d3 from "d3";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { DiffResult } from "./types";

const PANELS = [
  { key: "shannon", titleEn: "Shannon", titleZh: "Shannon 指数" },
  { key: "simpson", titleEn: "Simpson (1-D)", titleZh: "Simpson 指数" },
  { key: "chao1", titleEn: "Chao1", titleZh: "Chao1 丰富度" },
] as const;

const formatP = (pValue: number | undefined, locale: string) => {
  if (pValue == null || Number.isNaN(pValue)) return locale === "zh" ? "p 不可用" : "p unavailable";
  if (pValue < 0.001) return "p < 0.001";
  return `p = ${pValue.toFixed(3)}`;
};

const safeSeries = (values: unknown): number[] => {
  if (!Array.isArray(values)) return [];
  return values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
};

const shortenLabel = (value: unknown, fallback: string) => {
  if (typeof value !== "string" || value.trim().length === 0) return fallback;
  return value.length > 12 ? `${value.slice(0, 11)}...` : value;
};

const AlphaBoxChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const width = 920;
    const height = 400;
    const margin = { top: 52, right: 20, bottom: 60, left: 56 };
    const panelWidth = (width - margin.left - margin.right) / 3;
    const panelHeight = height - margin.top - margin.bottom;
    const noDataLabel = locale === "zh" ? "无数据" : "No data";
    const groupAName = shortenLabel(result.summary?.group_a_name, locale === "zh" ? "A 组" : "Group A");
    const groupBName = shortenLabel(result.summary?.group_b_name, locale === "zh" ? "B 组" : "Group B");

    const alphaGroupA = result.alpha_diversity?.group_a;
    const alphaGroupB = result.alpha_diversity?.group_b;
    const hasAnyValue = PANELS.some((panel) =>
      safeSeries(alphaGroupA?.[panel.key]).length > 0 || safeSeries(alphaGroupB?.[panel.key]).length > 0,
    );

    if (!hasAnyValue) {
      svg.attr("viewBox", "0 0 900 120");
      svg.append("text")
        .attr("x", 20)
        .attr("y", 60)
        .attr("fill", "currentColor")
        .attr("font-size", 14)
        .text(locale === "zh" ? "暂无 Alpha 多样性数据" : "No alpha-diversity data available");
      return;
    }

    const drawBox = (
      panelGroup: d3.Selection<SVGGElement, unknown, null, undefined>,
      yScale: d3.ScaleLinear<number, number>,
      xCenter: number,
      points: number[],
      color: string,
    ) => {
      if (!points.length) return;
      const sorted = [...points].sort((left, right) => left - right);
      const q1 = d3.quantile(sorted, 0.25) ?? 0;
      const median = d3.quantile(sorted, 0.5) ?? 0;
      const q3 = d3.quantile(sorted, 0.75) ?? 0;
      const iqr = q3 - q1;
      const lower = Math.max(sorted[0] ?? 0, q1 - 1.5 * iqr);
      const upper = Math.min(sorted[sorted.length - 1] ?? 0, q3 + 1.5 * iqr);
      const boxWidth = 56;

      panelGroup.append("line")
        .attr("x1", xCenter)
        .attr("x2", xCenter)
        .attr("y1", yScale(lower))
        .attr("y2", yScale(upper))
        .attr("stroke", color)
        .attr("stroke-width", 1.4)
        .attr("stroke-dasharray", "3,3");

      panelGroup.append("rect")
        .attr("x", xCenter - boxWidth / 2)
        .attr("y", yScale(q3))
        .attr("width", boxWidth)
        .attr("height", Math.max(1, yScale(q1) - yScale(q3)))
        .attr("rx", 4)
        .attr("fill", color)
        .attr("opacity", 0.24)
        .attr("stroke", color)
        .attr("stroke-width", 1.3);

      panelGroup.append("line")
        .attr("x1", xCenter - boxWidth / 2)
        .attr("x2", xCenter + boxWidth / 2)
        .attr("y1", yScale(median))
        .attr("y2", yScale(median))
        .attr("stroke", color)
        .attr("stroke-width", 2.2);

      const outliers = sorted.filter((value) => value < lower || value > upper);
      panelGroup.append("g")
        .selectAll("circle")
        .data(outliers)
        .join("circle")
        .attr("cx", (_, index) => xCenter + ((index % 3) - 1) * 4)
        .attr("cy", (value) => yScale(value))
        .attr("r", 2.6)
        .attr("fill", color)
        .attr("opacity", 0.55);
    };

    PANELS.forEach((panel, panelIndex) => {
      const panelGroup = svg.append("g")
        .attr("transform", `translate(${margin.left + panelIndex * panelWidth},${margin.top})`);

      const groupAValues = safeSeries(alphaGroupA?.[panel.key]);
      const groupBValues = safeSeries(alphaGroupB?.[panel.key]);
      const panelValues = [...groupAValues, ...groupBValues];
      const panelMax = Math.max(d3.max(panelValues) ?? 1, 1);
      const yScale = d3.scaleLinear()
        .domain([0, panelMax])
        .nice()
        .range([panelHeight, 0]);

      const xA = panelWidth * 0.3;
      const xB = panelWidth * 0.7;

      panelGroup.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);

      if (groupAValues.length > 0) {
        drawBox(panelGroup, yScale, xA, groupAValues, "var(--secondary)");
      }
      if (groupBValues.length > 0) {
        drawBox(panelGroup, yScale, xB, groupBValues, "var(--primary)");
      }

      panelGroup.append("text")
        .attr("x", panelWidth / 2)
        .attr("y", -18)
        .attr("text-anchor", "middle")
        .attr("fill", "currentColor")
        .attr("font-size", 13)
        .attr("font-weight", 600)
        .text(locale === "zh" ? panel.titleZh : panel.titleEn);

      panelGroup.append("text")
        .attr("x", xA)
        .attr("y", panelHeight + 26)
        .attr("text-anchor", "middle")
        .attr("fill", groupAValues.length > 0 ? "var(--secondary)" : "var(--light-gray)")
        .attr("font-size", 10)
        .text(groupAValues.length > 0 ? groupAName : noDataLabel);

      panelGroup.append("text")
        .attr("x", xB)
        .attr("y", panelHeight + 26)
        .attr("text-anchor", "middle")
        .attr("fill", groupBValues.length > 0 ? "var(--primary)" : "var(--light-gray)")
        .attr("font-size", 10)
        .text(groupBValues.length > 0 ? groupBName : noDataLabel);

      const bracketY = 18;
      panelGroup.append("path")
        .attr("d", `M ${xA} ${bracketY} V ${bracketY - 8} H ${xB} V ${bracketY}`)
        .attr("fill", "none")
        .attr("stroke", "var(--light-gray)")
        .attr("stroke-width", 1.1);

      panelGroup.append("text")
        .attr("x", panelWidth / 2)
        .attr("y", bracketY - 12)
        .attr("text-anchor", "middle")
        .attr("fill", "var(--light-gray)")
        .attr("font-size", 10)
        .text(formatP(result.alpha_pvalues?.[panel.key], locale));

      if (panelIndex < PANELS.length - 1) {
        svg.append("line")
          .attr("x1", margin.left + (panelIndex + 1) * panelWidth)
          .attr("x2", margin.left + (panelIndex + 1) * panelWidth)
          .attr("y1", margin.top)
          .attr("y2", height - margin.bottom)
          .attr("stroke", "var(--dark-gray)")
          .attr("stroke-dasharray", "4,4");
      }
    });

    svg.append("text")
      .attr("transform", `translate(16,${height / 2}) rotate(-90)`)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 12)
      .text(locale === "zh" ? "多样性指数" : "Diversity Index");

    svg.attr("viewBox", `0 0 ${width} ${height}`);
  }, [locale, result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default AlphaBoxChart;
