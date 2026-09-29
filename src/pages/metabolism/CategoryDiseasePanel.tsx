import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import type { CategoryProfileResult, MetabolismCategory } from "./types";
import classes from "../MetabolismPage.module.css";

const MAX_ROWS = 20;

const formatDiseaseLabel = (disease: string, locale: string, maxLen = 56) => {
  const full = diseaseDisplayNameI18n(disease, locale);
  return full.length > maxLen ? `${full.slice(0, maxLen - 3)}...` : full;
};

interface Props {
  category: MetabolismCategory;
  result: CategoryProfileResult;
}

const CategoryDiseasePanel = ({ category, result }: Props) => {
  const { t, locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  const rows = [...result.disease_profiles]
    .sort((a, b) => Math.abs(b.log2fc) - Math.abs(a.log2fc) || a.adjusted_p - b.adjusted_p)
    .slice(0, MAX_ROWS);

  useEffect(() => {
    if (!svgRef.current || rows.length === 0) {
      return;
    }

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const rootStyles = getComputedStyle(document.documentElement);
    const diseaseColor = rootStyles.getPropertyValue("--primary").trim() || "#ef6b6b";
    const controlColor = rootStyles.getPropertyValue("--secondary").trim() || "#55b6ff";
    const textColor = rootStyles.getPropertyValue("--white").trim() || "#f4f7fb";
    const mutedColor = rootStyles.getPropertyValue("--gray").trim() || "#7d8699";
    const gridColor = rootStyles.getPropertyValue("--dark-gray").trim() || "#2c3344";

    const width = 1260;
    const margin = { top: 24, right: 96, bottom: 40, left: 430 };
    const rowHeight = 32;
    const height = margin.top + margin.bottom + rows.length * rowHeight;
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const maxAbs = Math.max(0.25, d3.max(rows, (row) => Math.abs(row.log2fc)) ?? 0.25);

    const xScale = d3.scaleLinear()
      .domain([-maxAbs, maxAbs])
      .range([0, innerWidth]);
    const yScale = d3.scaleBand()
      .domain(rows.map((row) => row.disease))
      .range([0, innerHeight])
      .padding(0.35);

    const zeroX = xScale(0);

    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const chart = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    chart.append("line")
      .attr("x1", zeroX)
      .attr("x2", zeroX)
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .attr("stroke", gridColor)
      .attr("stroke-width", 1)
      .attr("stroke-dasharray", "4 4");

    chart.selectAll(".row-guide")
      .data(rows)
      .join("line")
      .attr("x1", 0)
      .attr("x2", innerWidth)
      .attr("y1", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("y2", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("stroke", gridColor)
      .attr("stroke-width", 1)
      .attr("opacity", 0.35);

    chart.selectAll(".stem")
      .data(rows)
      .join("line")
      .attr("x1", zeroX)
      .attr("x2", (row) => xScale(row.log2fc))
      .attr("y1", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("y2", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("stroke", (row) => (row.log2fc >= 0 ? diseaseColor : controlColor))
      .attr("stroke-width", 3)
      .attr("stroke-linecap", "round");

    chart.selectAll(".dot")
      .data(rows)
      .join("circle")
      .attr("cx", (row) => xScale(row.log2fc))
      .attr("cy", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("r", 6.5)
      .attr("fill", (row) => (row.log2fc >= 0 ? diseaseColor : controlColor))
      .attr("stroke", textColor)
      .attr("stroke-width", 1.5);

    chart.selectAll(".overlay")
      .data(rows)
      .join("rect")
      .attr("x", 0)
      .attr("y", (row) => yScale(row.disease) ?? 0)
      .attr("width", innerWidth)
      .attr("height", yScale.bandwidth())
      .attr("fill", "transparent")
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (row) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "疾病" : "Disease"}</span>
            <span>{diseaseDisplayNameI18n(row.disease, locale)}</span>
            <span>{locale === "zh" ? "log2FC" : "log2FC"}</span>
            <span>{row.log2fc.toFixed(3)}</span>
            <span>{locale === "zh" ? "疾病组均值" : "Disease mean"}</span>
            <span>{row.mean_disease.toFixed(4)}</span>
            <span>{locale === "zh" ? "严格 NC 均值" : "Strict NC mean"}</span>
            <span>{row.mean_nc.toFixed(4)}</span>
            <span>{locale === "zh" ? "校正 p 值" : "Adjusted p"}</span>
            <span>{row.adjusted_p.toExponential(2)}</span>
            <span>{locale === "zh" ? "效应量" : "Effect size"}</span>
            <span>{row.effect_size.toFixed(3)}</span>
          </div>,
        )
      );

    chart.append("g")
      .call(
        d3.axisBottom(xScale)
          .ticks(6)
          .tickFormat((tick) => Number(tick).toFixed(1)),
      )
      .attr("transform", `translate(0,${innerHeight})`)
      .attr("font-size", 11)
      .attr("color", mutedColor);

    chart.selectAll(".disease-label")
      .data(rows)
      .join("text")
      .attr("class", "disease-label")
      .attr("x", -12)
      .attr("y", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("dy", "0.35em")
      .attr("text-anchor", "end")
      .attr("font-size", 12)
      .attr("fill", textColor)
      .text((row) => formatDiseaseLabel(row.disease, locale, 56));

    chart.selectAll(".count-label")
      .data(rows)
      .join("text")
      .attr("x", innerWidth + 10)
      .attr("y", (row) => (yScale(row.disease) ?? 0) + yScale.bandwidth() / 2)
      .attr("dy", "0.35em")
      .attr("font-size", 11)
      .attr("fill", mutedColor)
      .text((row) => `n=${row.sample_count}`);
  }, [locale, rows]);

  if (rows.length === 0) {
    return (
      <div className={classes.emptyState}>
        {t("metabolism.noDiseaseContext")}
      </div>
    );
  }

  const significantCount = rows.filter((row) => row.adjusted_p < 0.05).length;

  return (
    <div className={classes.chartBlock}>
      <div className={classes.chartHeaderRow}>
        <div>
          <h4>{t("metabolism.diseaseContext")}</h4>
          <p className={classes.subtleText}>
            {locale === "zh"
              ? `${category.name_zh} 在样本量最高的疾病中相对严格 NC 的差异`
              : `${category.name_en} compared against strict NC across the highest-coverage diseases`}
          </p>
        </div>
      </div>

      <div className={classes.summaryRow}>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.matchedGenera")}</span>
          <strong>{result.n_matched}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.testedDiseases")}</span>
          <strong>{result.disease_profiles.length}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.strictNc")}</span>
          <strong>{result.control_count}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{locale === "zh" ? "显著疾病" : "Significant diseases"}</span>
          <strong>{significantCount}</strong>
        </div>
      </div>

      <div className={classes.plotLegend}>
        <span className={classes.legendItem}>
          <span className={classes.legendSwatch} data-tone="disease" />
          {t("metabolism.enrichedDisease")}
        </span>
        <span className={classes.legendItem}>
          <span className={classes.legendSwatch} data-tone="control" />
          {t("metabolism.enrichedNc")}
        </span>
      </div>

      <svg ref={svgRef} className={classes.metricChart} />
      <p className={classes.subtleText}>{t("metabolism.scoreNote")}</p>
    </div>
  );
};

export default CategoryDiseasePanel;
