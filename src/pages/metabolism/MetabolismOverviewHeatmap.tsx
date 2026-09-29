import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import type { MetabolismOverviewResult } from "./types";
import classes from "../MetabolismPage.module.css";

interface Props {
  data: MetabolismOverviewResult;
  onSelectCategory: (categoryId: string) => void;
}

const formatDiseaseLabel = (disease: string, locale: string, maxLen = 32) => {
  const full = diseaseDisplayNameI18n(disease, locale);
  return full.length > maxLen ? `${full.slice(0, maxLen - 3)}...` : full;
};

const MetabolismOverviewHeatmap = ({ data, onSelectCategory }: Props) => {
  const { t, locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current || data.categories.length === 0 || data.diseases.length === 0) {
      return;
    }

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const rootStyles = getComputedStyle(document.documentElement);
    const textColor = rootStyles.getPropertyValue("--white").trim() || "#f4f7fb";
    const mutedColor = rootStyles.getPropertyValue("--gray").trim() || "#7d8699";
    const gridColor = rootStyles.getPropertyValue("--dark-gray").trim() || "#2c3344";

    const cellWidth = 56;
    const cellHeight = 28;
    const width = 1440;
    const margin = { top: 186, right: 44, bottom: 34, left: 340 };
    const height = margin.top + margin.bottom + data.categories.length * cellHeight;
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const maxAbs = Math.max(
      0.5,
      d3.max(
        data.categories.flatMap((category) => category.values.filter((value): value is number => typeof value === "number")),
        (value) => Math.abs(value),
      ) ?? 0.5,
    );

    const colorScale = d3.scaleDiverging((t) => d3.interpolateRdBu(1 - t))
      .domain([-maxAbs, 0, maxAbs]);
    const xScale = d3.scaleBand()
      .domain(data.diseases.map((disease) => disease.name))
      .range([0, cellWidth * data.diseases.length])
      .padding(0.08);
    const yScale = d3.scaleBand()
      .domain(data.categories.map((category) => category.category_id))
      .range([0, innerHeight])
      .padding(0.08);

    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const chart = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const cells: Array<{
      categoryId: string;
      categoryName: string;
      matched: number;
      disease: string;
      value: number | null;
      x: string;
      y: string;
    }> = [];

    for (const category of data.categories) {
      for (let index = 0; index < data.diseases.length; index += 1) {
        const disease = data.diseases[index];
        cells.push({
          categoryId: category.category_id,
          categoryName: locale === "zh" ? category.name_zh : category.name_en,
          matched: category.n_matched,
          disease: disease.name,
          value: category.values[index] ?? null,
          x: disease.name,
          y: category.category_id,
        });
      }
    }

    chart.selectAll(".cell")
      .data(cells)
      .join("rect")
      .attr("class", "cell")
      .attr("x", (cell) => xScale(cell.x) ?? 0)
      .attr("y", (cell) => yScale(cell.y) ?? 0)
      .attr("width", xScale.bandwidth())
      .attr("height", yScale.bandwidth())
      .attr("rx", 4)
      .attr("fill", (cell) => (cell.value === null ? gridColor : colorScale(cell.value)))
      .attr("stroke", gridColor)
      .attr("stroke-width", 1)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (cell) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "类别" : "Category"}</span>
            <span>{cell.categoryName}</span>
            <span>{locale === "zh" ? "疾病" : "Disease"}</span>
            <span>{diseaseDisplayNameI18n(cell.disease, locale)}</span>
            <span>{locale === "zh" ? "log2FC" : "log2FC"}</span>
            <span>{cell.value === null ? "NA" : cell.value.toFixed(3)}</span>
            <span>{locale === "zh" ? "匹配属数" : "Matched genera"}</span>
            <span>{cell.matched}</span>
          </div>,
        )
      )
      .style("cursor", "pointer")
      .on("click", (_, cell) => onSelectCategory(cell.categoryId));

    chart.append("g")
      .call(
        d3.axisTop(xScale)
          .tickFormat((tick) => formatDiseaseLabel(String(tick), locale, 32)),
      )
      .attr("font-size", 11)
      .attr("color", mutedColor)
      .selectAll("text")
      .attr("transform", "rotate(-52)")
      .attr("text-anchor", "start");

    chart.selectAll(".row-label")
      .data(data.categories)
      .join("text")
      .attr("x", -12)
      .attr("y", (category) => (yScale(category.category_id) ?? 0) + yScale.bandwidth() / 2)
      .attr("dy", "0.35em")
      .attr("text-anchor", "end")
      .attr("font-size", 12)
      .attr("fill", textColor)
      .style("cursor", "pointer")
      .text((category) => `${category.icon} ${locale === "zh" ? category.name_zh : category.name_en}`)
      .on("click", (_, category) => onSelectCategory(category.category_id));

    const legendWidth = Math.min(innerWidth, 240);
    const legendY = innerHeight + 14;
    const defs = svg.append("defs");
    const gradient = defs.append("linearGradient")
      .attr("id", "metabolismOverviewGradient")
      .attr("x1", "0%")
      .attr("x2", "100%");
    gradient.append("stop").attr("offset", "0%").attr("stop-color", colorScale(-maxAbs));
    gradient.append("stop").attr("offset", "50%").attr("stop-color", colorScale(0));
    gradient.append("stop").attr("offset", "100%").attr("stop-color", colorScale(maxAbs));

    chart.append("rect")
      .attr("x", 0)
      .attr("y", legendY)
      .attr("width", legendWidth)
      .attr("height", 12)
      .attr("rx", 4)
      .attr("fill", "url(#metabolismOverviewGradient)");
    chart.append("text")
      .attr("x", 0)
      .attr("y", legendY + 28)
      .attr("font-size", 10)
      .attr("fill", mutedColor)
      .text(t("metabolism.enrichedNc"));
    chart.append("text")
      .attr("x", legendWidth)
      .attr("y", legendY + 28)
      .attr("text-anchor", "end")
      .attr("font-size", 10)
      .attr("fill", mutedColor)
      .text(t("metabolism.enrichedDisease"));
  }, [data.categories, data.diseases, locale, onSelectCategory, t]);

  if (data.categories.length === 0 || data.diseases.length === 0) {
    return (
      <div className={classes.emptyState}>
        {t("metabolism.noOverview")}
      </div>
    );
  }

  return (
    <div className={classes.chartBlock}>
      <div className={classes.chartHeaderRow}>
        <div>
          <h4>{t("metabolism.globalOverview")}</h4>
          <p className={classes.subtleText}>{t("metabolism.overviewNote")}</p>
        </div>
      </div>
      <svg ref={svgRef} className={classes.metricChart} />
    </div>
  );
};

export default MetabolismOverviewHeatmap;
