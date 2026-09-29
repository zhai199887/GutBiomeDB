/**
 * AlphaDiversityChart.tsx — Shannon/Simpson diversity trend across life stages
 */
import { useEffect, useRef } from "react";

import * as d3 from "d3";

import type { LifecycleRow } from "../LifecyclePage";

interface Props {
  data: LifecycleRow[];
  locale: string;
  metric: "shannon" | "simpson";
}

const AGE_ZH: Record<string, string> = {
  Infant: "婴儿",
  Child: "儿童",
  Adolescent: "青少年",
  Adult: "成人",
  Older_Adult: "老年人",
  Oldest_Old: "高龄老人",
  Centenarian: "百岁老人",
  Unknown: "未知",
};

export function AlphaDiversityChart({ data, locale, metric }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current || data.length === 0) return;
    drawDiversityLine(svgRef.current, data, locale, metric);
  }, [data, locale, metric]);

  return <svg ref={svgRef} style={{ width: "100%", height: 220 }} />;
}

function drawDiversityLine(
  svgEl: SVGSVGElement,
  data: LifecycleRow[],
  locale: string,
  metric: "shannon" | "simpson",
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const margin = { top: 16, right: 20, bottom: 52, left: 52 };
  const width = 840;
  const height = 220;
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const meanKey = `${metric}_mean` as keyof LifecycleRow;
  const sdKey = `${metric}_sd` as keyof LifecycleRow;
  const ageGroups = data.map((row) => row.age_group);
  const agLabel = (ageGroup: string) => locale === "zh" ? (AGE_ZH[ageGroup] ?? ageGroup.replace(/_/g, " ")) : ageGroup.replace(/_/g, " ");

  const xScale = d3.scalePoint<string>()
    .domain(ageGroups)
    .range([0, innerWidth])
    .padding(0.2);

  const means = data.map((row) => Number(row[meanKey] ?? 0));
  const sds = data.map((row) => Number(row[sdKey] ?? 0));
  const minValue = d3.min(means.map((value, index) => value - sds[index])) ?? 0;
  const maxValue = d3.max(means.map((value, index) => value + sds[index])) ?? 4;
  const yScale = d3.scaleLinear()
    .domain([Math.max(0, minValue * 0.92), maxValue * 1.08 || 1])
    .range([innerHeight, 0]);

  const color = metric === "shannon" ? "#3b82f6" : "#10b981";

  root.selectAll(".grid-line")
    .data(yScale.ticks(4))
    .join("line")
    .attr("x1", 0)
    .attr("x2", innerWidth)
    .attr("y1", (tick) => yScale(tick))
    .attr("y2", (tick) => yScale(tick))
    .attr("stroke", "rgba(148, 163, 184, 0.18)")
    .attr("stroke-dasharray", "4,4");

  root.selectAll(".err-bar")
    .data(data)
    .join("line")
    .attr("class", "err-bar")
    .attr("x1", (row) => xScale(row.age_group) ?? 0)
    .attr("x2", (row) => xScale(row.age_group) ?? 0)
    .attr("y1", (row) => yScale(Number(row[meanKey] ?? 0) - Number(row[sdKey] ?? 0)))
    .attr("y2", (row) => yScale(Number(row[meanKey] ?? 0) + Number(row[sdKey] ?? 0)))
    .attr("stroke", "rgba(148, 163, 184, 0.9)")
    .attr("stroke-width", 1.3);

  const line = d3.line<LifecycleRow>()
    .x((row) => xScale(row.age_group) ?? 0)
    .y((row) => yScale(Number(row[meanKey] ?? 0)))
    .curve(d3.curveMonotoneX);

  root.append("path")
    .datum(data)
    .attr("fill", "none")
    .attr("stroke", color)
    .attr("stroke-width", 2.6)
    .attr("d", line);

  const tooltip = d3.select("body")
    .selectAll(".alpha-tooltip")
    .data([null])
    .join("div")
    .attr("class", "alpha-tooltip")
    .style("position", "absolute")
    .style("pointer-events", "none")
    .style("opacity", 0)
    .style("background", "rgba(255,255,255,0.98)")
    .style("border", "1px solid #dbe3ef")
    .style("border-radius", "10px")
    .style("padding", "8px 10px")
    .style("font-size", "0.8rem")
    .style("line-height", "1.45")
    .style("color", "#0f172a")
    .style("box-shadow", "0 10px 24px rgba(15, 23, 42, 0.18)")
    .style("z-index", "1000");

  root.selectAll(".dot")
    .data(data)
    .join("circle")
    .attr("class", "dot")
    .attr("cx", (row) => xScale(row.age_group) ?? 0)
    .attr("cy", (row) => yScale(Number(row[meanKey] ?? 0)))
    .attr("r", 5)
    .attr("fill", color)
    .attr("stroke", "#ffffff")
    .attr("stroke-width", 1.5)
    .on("mouseover", function onHover(event, row) {
      const meanVal = Number(row[meanKey] ?? 0);
      const sdVal = Number(row[sdKey] ?? 0);
      tooltip
        .html([
          `<strong>${agLabel(row.age_group)}</strong>`,
          `${metric === "shannon" ? "Shannon" : "Simpson"}: ${meanVal.toFixed(3)} ± ${sdVal.toFixed(3)}`,
          `n = ${row.sample_count.toLocaleString()}`,
        ].join("<br/>"))
        .style("left", `${event.pageX + 12}px`)
        .style("top", `${event.pageY - 22}px`)
        .style("opacity", 1);
    })
    .on("mouseout", () => tooltip.style("opacity", 0));

  root.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(xScale).tickFormat((value) => agLabel(value)))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("transform", "rotate(-20)")
    .style("text-anchor", "end");

  root.append("g")
    .call(d3.axisLeft(yScale).ticks(4))
    .attr("font-size", 10);

  root.append("text")
    .attr("transform", `translate(-40,${innerHeight / 2}) rotate(-90)`)
    .attr("fill", "currentColor")
    .attr("font-size", 11)
    .attr("text-anchor", "middle")
    .text(
      metric === "shannon"
        ? (locale === "zh" ? "Shannon 多样性指数" : "Shannon Diversity Index")
        : (locale === "zh" ? "Simpson 多样性指数 (1-D)" : "Simpson Diversity Index (1-D)"),
    );
}
