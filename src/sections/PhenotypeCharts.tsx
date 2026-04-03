import { useEffect } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import Placeholder from "@/components/Placeholder";
import { useData } from "@/data";
import { getCssVariable } from "@/util/dom";
import { formatNumber } from "@/util/string";
import classes from "./PhenotypeCharts.module.css";

const AGE_ORDER = [
  "Infant",
  "Child",
  "Adolescent",
  "Adult",
  "Older_Adult",
  "Oldest_Old",
  "Centenarian",
  "Unknown",
];

const PhenotypeCharts = () => {
  const summary = useData((s) => s.summary);
  const filters = useData((s) => s.filters);

  useEffect(() => {
    if (!summary) return;
    drawAgeChart(summary.age_counts, summary.age_sex_cross, filters.age_groups);
    drawDiseaseChart(summary.disease_counts, filters.diseases);
    drawHeatmap(
      summary.age_disease_cross,
      summary.top20_diseases.slice(0, 15),
    );
  }, [summary, filters]);

  if (!summary)
    return <Placeholder height={300}>Loading charts...</Placeholder>;

  return (
    <section>
      <h2>Phenotype Overview</h2>

      <div className={classes.grid}>
        <div className="sub-section">
          <h3>Age Group Distribution</h3>
          <svg id="age-chart" className="chart" viewBox="-80 -40 480 380" />
        </div>

        <div className="sub-section">
          <h3>Top 20 Diseases</h3>
          <svg id="disease-chart" className="chart" viewBox="-200 -20 700 560" />
        </div>
      </div>

      <div className="sub-section" style={{ marginTop: "1.5rem" }}>
        <h3>Age × Disease Heatmap</h3>
        <svg id="heatmap" className="chart" viewBox="-160 -80 960 490" />
      </div>
    </section>
  );
};

export default PhenotypeCharts;

/** Age group bar chart, stacked by sex, with optional highlighting
 * 年龄组柱状图，按性别堆叠，支持筛选高亮 */
const drawAgeChart = (
  ageCounts: Record<string, number>,
  ageSex: { age_group: string; sex: string; count: number }[],
  activeAgeGroups: string[] = [],
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#age-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  const W = 400;
  const H = 340;
  const primary = getCssVariable("--primary");
  const secondary = getCssVariable("--secondary");

  const groups = AGE_ORDER.filter((g) => ageCounts[g] > 0);

  const sexColors: Record<string, string> = {
    female: secondary,
    male: primary,
    unknown: getCssVariable("--gray"),
  };

  /** stack data per age group */
  const data = groups.map((g) => {
    const bySex = Object.fromEntries(
      ageSex.filter((r) => r.age_group === g).map((r) => [r.sex, r.count]),
    );
    return {
      group: g,
      female: bySex["female"] ?? 0,
      male: bySex["male"] ?? 0,
      unknown: bySex["unknown"] ?? 0,
      total: ageCounts[g] ?? 0,
    };
  });

  const xMax = d3.max(data, (d) => d.total) ?? 1;
  const xScale = d3.scaleLinear().domain([0, xMax]).range([0, W]);
  const yScale = d3
    .scaleBand()
    .domain(groups)
    .range([0, H])
    .padding(0.2);

  /** stacked horizontal bars */
  const sexKeys = ["female", "male", "unknown"] as const;
  for (const sex of sexKeys) {
    svg
      .selectAll(`.bar-${sex}`)
      .data(data)
      .join("rect")
      .attr("class", `bar-${sex}`)
      .attr("x", (d) => {
        const offset =
          sex === "male"
            ? xScale(d.female)
            : sex === "unknown"
              ? xScale(d.female + d.male)
              : 0;
        return offset;
      })
      .attr("y", (d) => yScale(d.group) ?? 0)
      .attr("width", (d) => xScale(d[sex]))
      .attr("height", yScale.bandwidth())
      .attr("fill", sexColors[sex])
      .attr("opacity", (d) => {
        const baseOp = sex === "unknown" ? 0.4 : 0.85;
        if (activeAgeGroups.length === 0) return baseOp;
        return activeAgeGroups.includes(d.group) ? baseOp : 0.15;
      })
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Age Group</span>
            <span>{d.group.replace(/_/g, " ")}</span>
            <span>Total</span>
            <span>{formatNumber(d.total, false)}</span>
            <span>Female</span>
            <span>{formatNumber(d.female, false)}</span>
            <span>Male</span>
            <span>{formatNumber(d.male, false)}</span>
            <span>Unknown sex</span>
            <span>{formatNumber(d.unknown, false)}</span>
          </div>,
        ),
      );
  }

  /** y axis */
  svg
    .append("g")
    .call(
      d3
        .axisLeft(yScale)
        .tickFormat((d) => d.replace(/_/g, " ")),
    )
    .attr("font-size", "14px");

  /** x axis */
  svg
    .append("g")
    .attr("transform", `translate(0,${H})`)
    .call(
      d3
        .axisBottom(xScale)
        .ticks(4)
        .tickFormat((d) => formatNumber(Number(d))),
    )
    .attr("font-size", "12px");

  /** legend */
  const legendX = W + 10;
  sexKeys.forEach((s, i) => {
    svg
      .append("rect")
      .attr("x", legendX)
      .attr("y", i * 20)
      .attr("width", 14)
      .attr("height", 14)
      .attr("fill", sexColors[s])
      .attr("opacity", s === "unknown" ? 0.4 : 0.85);
    svg
      .append("text")
      .attr("x", legendX + 18)
      .attr("y", i * 20 + 11)
      .text(s)
      .attr("font-size", "13px")
      .attr("fill", "currentColor");
  });
};

/** Disease horizontal bar chart with optional highlighting
 * 疾病柱状图，支持筛选高亮 */
const drawDiseaseChart = (diseaseCounts: Record<string, number>, activeDiseases: string[] = []) => {
  const svg = d3.select<SVGSVGElement, unknown>("#disease-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  const W = 420;
  const H = 530;
  const primary = getCssVariable("--primary");
  const secondary = getCssVariable("--secondary");

  const data = Object.entries(diseaseCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);

  const xScale = d3
    .scaleLog()
    .domain([1, d3.max(data, (d) => d[1]) ?? 1])
    .range([0, W]);

  const yScale = d3
    .scaleBand()
    .domain(data.map((d) => d[0]))
    .range([0, H])
    .padding(0.15);

  const highlight = (name: string) =>
    name === "NC" ? secondary : name === "unknown" ? getCssVariable("--gray") : primary;

  svg
    .selectAll(".bar")
    .data(data)
    .join("rect")
    .attr("class", "bar")
    .attr("x", 0)
    .attr("y", (d) => yScale(d[0]) ?? 0)
    .attr("width", (d) => xScale(d[1]))
    .attr("height", yScale.bandwidth())
    .attr("fill", (d) => highlight(d[0]))
    .attr("opacity", (d) => {
      if (activeDiseases.length === 0) return 0.85;
      return activeDiseases.includes(d[0]) ? 1 : 0.15;
    })
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", ([name, count]) =>
      renderToString(
        <div className="tooltip-table">
          <span>Disease</span>
          <span>{name}</span>
          <span>Samples</span>
          <span>{formatNumber(count, false)}</span>
        </div>,
      ),
    );

  svg
    .append("g")
    .call(
      d3.axisLeft(yScale).tickFormat((d) =>
        d.length > 22 ? d.slice(0, 20) + "…" : d,
      ),
    )
    .attr("font-size", "13px");

  svg
    .append("g")
    .attr("transform", `translate(0,${H})`)
    .call(
      d3
        .axisBottom(xScale)
        .ticks(4)
        .tickFormat((d) => formatNumber(Number(d))),
    )
    .attr("font-size", "12px");
};

/** Age × Disease heatmap – interactive with row/column highlighting
 * 年龄×疾病热图 – 支持行/列高亮和点击交互 */
const drawHeatmap = (
  crossData: { age_group: string; disease: string; count: number }[],
  diseases: string[],
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#heatmap");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  const W = 780;
  const H = 320;
  const primary = getCssVariable("--primary");

  const ages = AGE_ORDER.filter((a) =>
    crossData.some((r) => r.age_group === a),
  );

  const filteredData = crossData.filter((d) => diseases.includes(d.disease));

  const xScale = d3
    .scaleBand()
    .domain(diseases)
    .range([0, W])
    .padding(0.05);

  const yScale = d3.scaleBand().domain(ages).range([0, H]).padding(0.05);

  const maxVal = d3.max(filteredData, (d) => d.count) ?? 1;
  const color = d3
    .scaleSequential()
    .domain([0, maxVal])
    .interpolator(d3.interpolate("#1a1a2e", primary));

  // State for highlighting / 高亮状态
  let highlightAge: string | null = null;
  let highlightDisease: string | null = null;

  const updateHighlight = () => {
    cells
      .attr("opacity", (d) => {
        if (!highlightAge && !highlightDisease) return 1;
        const matchAge = !highlightAge || d.age_group === highlightAge;
        const matchDisease = !highlightDisease || d.disease === highlightDisease;
        return matchAge && matchDisease ? 1 : 0.2;
      })
      .attr("stroke", (d) => {
        if (!highlightAge && !highlightDisease) return "none";
        const matchAge = !highlightAge || d.age_group === highlightAge;
        const matchDisease = !highlightDisease || d.disease === highlightDisease;
        return matchAge && matchDisease ? "var(--white)" : "none";
      })
      .attr("stroke-width", 1.5);

    // Show value labels on highlighted cells / 高亮单元格显示数值
    svg.selectAll(".cell-label").remove();
    if (highlightAge || highlightDisease) {
      svg.selectAll(".cell-label")
        .data(filteredData.filter((d) => {
          const matchAge = !highlightAge || d.age_group === highlightAge;
          const matchDisease = !highlightDisease || d.disease === highlightDisease;
          return matchAge && matchDisease && d.count > 0;
        }))
        .join("text")
        .attr("class", "cell-label")
        .attr("x", (d) => (xScale(d.disease) ?? 0) + xScale.bandwidth() / 2)
        .attr("y", (d) => (yScale(d.age_group) ?? 0) + yScale.bandwidth() / 2)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("fill", (d) => d.count > maxVal * 0.5 ? "#fff" : "var(--light-gray)")
        .attr("font-size", 9)
        .attr("font-weight", 600)
        .attr("pointer-events", "none")
        .text((d) => formatNumber(d.count));
    }
  };

  // Draw cells / 绘制单元格
  const cells = svg
    .selectAll(".cell")
    .data(filteredData)
    .join("rect")
    .attr("class", "cell")
    .attr("x", (d) => xScale(d.disease) ?? 0)
    .attr("y", (d) => yScale(d.age_group) ?? 0)
    .attr("width", xScale.bandwidth())
    .attr("height", yScale.bandwidth())
    .attr("fill", (d) => color(d.count))
    .attr("rx", 2)
    .style("cursor", "pointer")
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>Age Group</span>
          <span>{d.age_group.replace(/_/g, " ")}</span>
          <span>Disease</span>
          <span>{d.disease}</span>
          <span>Samples</span>
          <span>{formatNumber(d.count, false)}</span>
        </div>,
      ),
    )
    .on("mouseenter", (_, d) => {
      highlightAge = d.age_group;
      highlightDisease = d.disease;
      updateHighlight();
    })
    .on("mouseleave", () => {
      highlightAge = null;
      highlightDisease = null;
      updateHighlight();
    });

  /** x axis */
  svg
    .append("g")
    .attr("transform", `translate(0,${H})`)
    .call(d3.axisBottom(xScale).tickFormat((d) =>
      d.length > 12 ? d.slice(0, 11) + "…" : d,
    ))
    .attr("font-size", "11px")
    .selectAll("text")
    .attr("transform", "rotate(-30)")
    .attr("text-anchor", "end")
    .style("cursor", "pointer")
    .on("click", (_, d) => {
      highlightDisease = highlightDisease === d ? null : (d as string);
      highlightAge = null;
      updateHighlight();
    });

  /** y axis */
  svg
    .append("g")
    .call(d3.axisLeft(yScale).tickFormat((d) => d.replace(/_/g, " ")))
    .attr("font-size", "13px")
    .selectAll("text")
    .style("cursor", "pointer")
    .on("click", (_, d) => {
      highlightAge = highlightAge === d ? null : (d as string);
      highlightDisease = null;
      updateHighlight();
    });

  // Color legend / 色阶图例
  const legendW = 200, legendH = 10, legendY = H + 55;
  const defs = svg.append("defs");
  const gradId = "heatmap-gradient";
  const grad = defs.append("linearGradient").attr("id", gradId)
    .attr("x1", "0%").attr("x2", "100%");
  grad.append("stop").attr("offset", "0%").attr("stop-color", "#1a1a2e");
  grad.append("stop").attr("offset", "100%").attr("stop-color", primary);

  svg.append("rect")
    .attr("x", W / 2 - legendW / 2).attr("y", legendY)
    .attr("width", legendW).attr("height", legendH)
    .attr("fill", `url(#${gradId})`).attr("rx", 3);
  svg.append("text").attr("x", W / 2 - legendW / 2).attr("y", legendY + legendH + 14)
    .attr("font-size", 10).attr("fill", "var(--light-gray)").text("0");
  svg.append("text").attr("x", W / 2 + legendW / 2).attr("y", legendY + legendH + 14)
    .attr("text-anchor", "end").attr("font-size", 10).attr("fill", "var(--light-gray)")
    .text(formatNumber(maxVal));
  svg.append("text").attr("x", W / 2).attr("y", legendY + legendH + 14)
    .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", "var(--light-gray)")
    .text("Samples");
};
