/**
 * SpeciesPage.tsx
 * Species detail page: abundance by disease, country, metabolic roles
 * 物种详情页：显示该物种在不同疾病和国家中的丰度分布
 */
import React, { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link, useParams } from "react-router-dom";
import * as d3 from "d3";
import { useData } from "@/data";
import "@/components/tooltip";
import classes from "./SpeciesPage.module.css";

const SpeciesPage = () => {
  const { taxon } = useParams<{ taxon: string }>();
  const abundance = useData((s) => s.abundance);
  const summary = useData((s) => s.summary);

  const decodedTaxon = decodeURIComponent(taxon ?? "");

  if (!abundance || !summary) {
    return (
      <div className={classes.page}>
        <div className={classes.loading}>Loading data…</div>
      </div>
    );
  }

  // Check if genus exists in abundance data / 检查属名是否在丰度数据中
  const genusList = abundance.genera ?? [];
  const matchedGenus = genusList.find(
    (g) => g.toLowerCase() === decodedTaxon.toLowerCase(),
  );

  return (
    <div className={classes.page}>
      <div className={classes.header}>
        <Link to="/" className={classes.back}>← Back to Atlas</Link>
        <h1><i>{decodedTaxon}</i></h1>
        {!matchedGenus && (
          <p className={classes.notFound}>
            Genus not found in abundance dataset. Showing available information only.
          </p>
        )}
      </div>

      {matchedGenus && (
        <div className={classes.content}>
          <DiseaseBarChart genus={matchedGenus} abundance={abundance} summary={summary} />
          <AgeGroupBarChart genus={matchedGenus} abundance={abundance} summary={summary} />
          <MetabolismTags genus={matchedGenus} />
        </div>
      )}
    </div>
  );
};

// ── Disease abundance boxplot / 疾病丰度箱线图 ───────────────────────────────

const DiseaseBarChart = ({
  genus,
  abundance,
  summary,
}: {
  genus: string;
  abundance: NonNullable<ReturnType<typeof useData.getState>["abundance"]>;
  summary: NonNullable<ReturnType<typeof useData.getState>["summary"]>;
}) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Top 15 diseases by sample count / 取前15种疾病
    const topDiseases = summary.top20_diseases.slice(0, 15);

    // For each disease, get abundance value (stored as mean in abundance summary)
    // 对每种疾病，从丰度摘要中获取平均丰度
    const data = topDiseases
      .map((disease) => ({
        disease,
        value: abundance.by_disease[disease]?.[genus] ?? 0,
      }))
      .sort((a, b) => b.value - a.value);

    const margin = { top: 20, right: 30, bottom: 80, left: 60 };
    const W = 700, H = 320;
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xScale = d3.scaleBand().domain(data.map((d) => d.disease)).range([0, iW]).padding(0.3);
    const yMax = d3.max(data, (d) => d.value) ?? 0.01;
    const yScale = d3.scaleLinear().domain([0, yMax]).range([iH, 0]);

    // Color scale by value / 按丰度值上色
    const colorScale = d3.scaleSequential()
      .domain([0, yMax])
      .interpolator(d3.interpolate("var(--dark-gray)", "var(--secondary)"));

    g.selectAll(".bar")
      .data(data)
      .join("rect")
      .attr("class", "bar")
      .attr("x", (d) => xScale(d.disease) ?? 0)
      .attr("y", (d) => yScale(d.value))
      .attr("width", xScale.bandwidth())
      .attr("height", (d) => iH - yScale(d.value))
      .attr("fill", (d) => colorScale(d.value))
      .attr("opacity", 0.85)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Disease</span><span>{d.disease}</span>
            <span>Mean Abundance</span><span>{(d.value * 100).toFixed(4)}%</span>
          </div>
        )
      );

    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).tickFormat((d) => d.length > 10 ? d.slice(0, 9) + "…" : d))
      .attr("font-size", 10)
      .selectAll("text")
      .attr("transform", "rotate(-35)")
      .attr("text-anchor", "end");

    g.append("g")
      .call(d3.axisLeft(yScale).ticks(4).tickFormat((d) => `${(Number(d) * 100).toFixed(2)}%`))
      .attr("font-size", 10);

    // Y-axis label / Y轴标签
    svg.append("text")
      .attr("transform", "translate(15," + (margin.top + iH / 2) + ") rotate(-90)")
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 11).text("Relative Abundance (%)");
  }, [genus, abundance, summary]);

  return (
    <div className={classes.chartCard}>
      <h3>Abundance by Disease (Top 15)</h3>
      <svg ref={svgRef} className={classes.chart} />
    </div>
  );
};

// ── Age-group bar chart (country-level data not in abundance summary)
// 年龄段丰度柱状图（丰度摘要中无按国家的聚合数据）

const AgeGroupBarChart = ({
  genus,
  abundance,
  summary: _summary,
}: {
  genus: string;
  abundance: NonNullable<ReturnType<typeof useData.getState>["abundance"]>;
  summary: NonNullable<ReturnType<typeof useData.getState>["summary"]>;
}) => {
  // abundance.by_age_group holds mean abundance per age group
  // by_age_group 存储每个年龄段的平均丰度
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Sort by biological life-course order, not by abundance value
    // 按生命周期顺序排序，保留年龄段的生物学意义
    const AGE_ORDER = [
      "infant", "child", "schoolage", "school_age", "adolescent",
      "adult", "middle_aged", "senior", "elderly", "aged",
    ];
    const ageRank = (age: string) => {
      const idx = AGE_ORDER.findIndex((k) => age.toLowerCase().includes(k));
      return idx === -1 ? 99 : idx;
    };

    const ageData = Object.entries(abundance.by_age_group)
      .map(([age, vals]) => ({ age, value: vals[genus] ?? 0 }))
      .filter((d) => d.value > 0)
      .sort((a, b) => ageRank(a.age) - ageRank(b.age));

    if (ageData.length === 0) {
      svg.attr("viewBox", "0 0 480 60");
      svg.append("text").attr("x", 10).attr("y", 30)
        .text("No age-stratified data available")
        .attr("fill", "currentColor").attr("font-size", 12);
      return;
    }

    const margin = { top: 20, right: 20, bottom: 60, left: 100 };
    const W = 480, H = 240;
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xScale = d3.scaleBand().domain(ageData.map((d) => d.age)).range([0, iW]).padding(0.25);
    const yMax = d3.max(ageData, (d) => d.value) ?? 0.01;
    const yScale = d3.scaleLinear().domain([0, yMax]).range([iH, 0]);

    g.selectAll(".bar")
      .data(ageData)
      .join("rect")
      .attr("x", (d) => xScale(d.age) ?? 0)
      .attr("y", (d) => yScale(d.value))
      .attr("width", xScale.bandwidth())
      .attr("height", (d) => iH - yScale(d.value))
      .attr("fill", "var(--primary)")
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Age Group</span><span>{d.age.replace(/_/g, " ")}</span>
            <span>Mean Abundance</span><span>{(d.value * 100).toFixed(4)}%</span>
          </div>
        )
      );

    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).tickFormat((d) => d.replace(/_/g, " ")))
      .attr("font-size", 10)
      .selectAll("text")
      .attr("transform", "rotate(-25)")
      .attr("text-anchor", "end");

    g.append("g")
      .call(d3.axisLeft(yScale).ticks(4).tickFormat((d) => `${(Number(d) * 100).toFixed(2)}%`))
      .attr("font-size", 10);

    // Y-axis label / Y轴标签
    svg.append("text")
      .attr("transform", "translate(15," + (margin.top + iH / 2) + ") rotate(-90)")
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 11).text("Relative Abundance (%)");
  }, [genus, abundance]);

  return (
    <div className={classes.chartCard}>
      <h3>Abundance by Age Group</h3>
      <svg ref={svgRef} className={classes.chart} />
    </div>
  );
};

// ── Metabolic function tags / 代谢功能标签 ───────────────────────────────────

const MetabolismTags = ({ genus }: { genus: string }) => {
  const [categories, setCategories] = useState<
    { id: string; name_en: string; icon: string }[]
  >([]);

  useEffect(() => {
    fetch("/data/metabolism_mapping.json")
      .then((r) => r.json())
      .then((data: { categories: { id: string; name_en: string; icon: string; taxa: string[] }[] }) => {
        const matches = data.categories.filter((c) =>
          c.taxa.some((t) => t.toLowerCase().includes(genus.toLowerCase()) ||
            genus.toLowerCase().includes(t.toLowerCase()))
        );
        setCategories(matches);
      })
      .catch(() => { /* mapping file unavailable, show no tags */ });
  }, [genus]);

  if (categories.length === 0) return null;

  return (
    <div className={classes.chartCard}>
      <h3>Metabolic Functions</h3>
      <div className={classes.tagList}>
        {categories.map((c) => (
          <Link
            key={c.id}
            to="/metabolism"
            className={classes.metaTag}
          >
            {c.icon} {c.name_en}
          </Link>
        ))}
      </div>
    </div>
  );
};

export default SpeciesPage;
