import { useEffect, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { loadAbundance, loadSummary, useData } from "@/data";
import { getCssVariable } from "@/util/dom";
import { formatNumber } from "@/util/string";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import "@/components/tooltip";

const AGE_GROUPS = [
  "Infant",
  "Child",
  "Adolescent",
  "Adult",
  "Older_Adult",
  "Oldest_Old",
  "Centenarian",
];

type DimType = "age" | "sex" | "disease";

const PhenotypePage = () => {
  const { t, locale } = useI18n();
  const abundance = useData((s) => s.abundance);
  const summary = useData((s) => s.summary);

  const [dimType, setDimType] = useState<DimType>("sex");
  const [groupA, setGroupA] = useState("female");
  const [groupB, setGroupB] = useState("male");

  useEffect(() => {
    if (!abundance) loadAbundance();
    if (!summary) loadSummary();
  }, [abundance, summary]);

  const dLabel = (g: string) =>
    dimType === "disease" ? diseaseDisplayNameI18n(g, locale) : g.replace(/_/g, " ");

  useEffect(() => {
    if (!abundance) return;
    drawChart(abundance, dimType, groupA, groupB, dLabel);
  }, [abundance, dimType, groupA, groupB, locale]);

  const groupOptions = (): string[] => {
    if (dimType === "age") return AGE_GROUPS;
    if (dimType === "sex") return ["female", "male", "unknown"];
    return summary?.top20_diseases ?? [];
  };

  const handleDimChange = (d: DimType) => {
    setDimType(d);
    const opts = d === "age"
      ? AGE_GROUPS
      : d === "sex"
        ? ["female", "male", "unknown"]
        : summary?.top20_diseases ?? [];
    setGroupA(opts[0] ?? "");
    setGroupB(opts[1] ?? "");
  };

  return (
    <div style={{ padding: "2rem", maxWidth: "1100px", margin: "0 auto" }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <Link
          to="/"
          style={{ color: "var(--primary)", textDecoration: "none" }}
        >
          {t("phenotype.back")}
        </Link>
      </div>

      <h1 style={{ fontSize: "1.6rem", marginBottom: "0.5rem" }}>
        {t("phenotype.title")}
      </h1>
      <p style={{ color: "var(--light-gray)", marginBottom: "2rem" }}>
        {t("phenotype.subtitle")}
      </p>

      <div
        style={{
          display: "flex",
          gap: "1.5rem",
          flexWrap: "wrap",
          marginBottom: "2rem",
          alignItems: "flex-end",
        }}
      >
        {/* Dimension selector */}
        <div>
          <div
            style={{
              fontSize: "0.8rem",
              textTransform: "uppercase",
              color: "var(--light-gray)",
              marginBottom: "0.4rem",
            }}
          >
            {t("phenotype.compareBy")}
          </div>
          <div style={{ display: "flex", gap: "0.4rem" }}>
            {(["age", "sex", "disease"] as DimType[]).map((d) => (
              <button
                key={d}
                onClick={() => handleDimChange(d)}
                style={{
                  background: dimType === d ? "var(--primary)" : "none",
                  border: "1px solid var(--gray)",
                  color:
                    dimType === d ? "var(--black)" : "var(--light-gray)",
                  borderRadius: "4px",
                  padding: "0.3rem 0.8rem",
                  cursor: "pointer",
                }}
              >
                {d.charAt(0).toUpperCase() + d.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Group A */}
        <div>
          <div
            style={{
              fontSize: "0.8rem",
              textTransform: "uppercase",
              color: "var(--light-gray)",
              marginBottom: "0.4rem",
            }}
          >
            Group A
          </div>
          <select
            value={groupA}
            onChange={(e) => setGroupA(e.target.value)}
            style={{
              background: "var(--dark-gray)",
              border: "1px solid var(--gray)",
              color: "var(--light-gray)",
              borderRadius: "4px",
              padding: "0.3rem 0.6rem",
            }}
          >
            {groupOptions().map((g) => (
              <option key={g} value={g}>
                {dLabel(g)}
              </option>
            ))}
          </select>
        </div>

        <span style={{ color: "var(--light-gray)", paddingBottom: "0.3rem" }}>
          vs
        </span>

        {/* Group B */}
        <div>
          <div
            style={{
              fontSize: "0.8rem",
              textTransform: "uppercase",
              color: "var(--light-gray)",
              marginBottom: "0.4rem",
            }}
          >
            Group B
          </div>
          <select
            value={groupB}
            onChange={(e) => setGroupB(e.target.value)}
            style={{
              background: "var(--dark-gray)",
              border: "1px solid var(--gray)",
              color: "var(--light-gray)",
              borderRadius: "4px",
              padding: "0.3rem 0.6rem",
            }}
          >
            {groupOptions().map((g) => (
              <option key={g} value={g}>
                {dLabel(g)}
              </option>
            ))}
          </select>
        </div>
      </div>

      {!abundance ? (
        <p style={{ color: "var(--light-gray)" }}>{t("phenotype.loading")}</p>
      ) : (
        <svg
          id="phenotype-chart"
          viewBox="-350 -40 1050 560"
          style={{ width: "100%", maxWidth: 1050 }}
        />
      )}
    </div>
  );
};

export default PhenotypePage;

const drawChart = (
  abundance: NonNullable<ReturnType<typeof useData.getState>["abundance"]>,
  dimType: string,
  groupA: string,
  groupB: string,
  labelFn: (g: string) => string = (g) => g.replace(/_/g, " "),
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#phenotype-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  const byKey =
    dimType === "age"
      ? abundance.by_age_group
      : dimType === "sex"
        ? abundance.by_sex
        : abundance.by_disease;

  const dataA = byKey[groupA];
  const dataB = byKey[groupB];

  if (!dataA || !dataB) {
    svg
      .append("text")
      .text("No data for selected groups")
      .attr("fill", "currentColor")
      .attr("font-size", 18);
    return;
  }

  /** top 20 genera by max abundance across both groups */
  const genera = abundance.genera.slice(0, 20);

  const primary = getCssVariable("--primary");
  const secondary = getCssVariable("--secondary");

  type Row = { genus: string; a: number; b: number; diff: number };
  const data: Row[] = genera
    .map((g) => ({
      genus: g,
      a: dataA[g] ?? 0,
      b: dataB[g] ?? 0,
      diff: (dataA[g] ?? 0) - (dataB[g] ?? 0),
    }))
    .sort((x, y) => Math.abs(y.diff) - Math.abs(x.diff));

  const W = 650;
  const H = 500;
  const barH = H / data.length;

  const xMax = d3.max(data, (d) => Math.max(d.a, d.b)) ?? 0.01;
  const xScale = d3.scaleLinear().domain([0, xMax]).range([0, W / 2 - 10]);

  const yScale = d3
    .scaleBand()
    .domain(data.map((d) => d.genus))
    .range([0, H])
    .padding(0.2);

  /** Group A bars (left-facing) */
  svg
    .selectAll(".bar-a")
    .data(data)
    .join("rect")
    .attr("class", "bar-a")
    .attr("x", (d) => -xScale(d.a))
    .attr("y", (d) => yScale(d.genus) ?? 0)
    .attr("width", (d) => xScale(d.a))
    .attr("height", yScale.bandwidth())
    .attr("fill", secondary)
    .attr("opacity", 0.85)
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>Genus</span>
          <span>{d.genus}</span>
          <span>{labelFn(groupA)}</span>
          <span>{(d.a * 100).toFixed(3)}%</span>
        </div>,
      ),
    );

  /** Group B bars (right-facing) */
  svg
    .selectAll(".bar-b")
    .data(data)
    .join("rect")
    .attr("class", "bar-b")
    .attr("x", 0)
    .attr("y", (d) => yScale(d.genus) ?? 0)
    .attr("width", (d) => xScale(d.b))
    .attr("height", yScale.bandwidth())
    .attr("fill", primary)
    .attr("opacity", 0.85)
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>Genus</span>
          <span>{d.genus}</span>
          <span>{labelFn(groupB)}</span>
          <span>{(d.b * 100).toFixed(3)}%</span>
        </div>,
      ),
    );

  /** genus labels in center */
  svg
    .selectAll(".genus-label")
    .data(data)
    .join("text")
    .attr("class", "genus-label")
    .attr("x", 0)
    .attr("y", (d) => (yScale(d.genus) ?? 0) + yScale.bandwidth() / 2 + 1)
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", Math.max(10, barH * 0.55))
    .attr("fill", "currentColor")
    .text((d) => d.genus);

  /** center axis */
  svg
    .append("line")
    .attr("x1", 0)
    .attr("x2", 0)
    .attr("y1", 0)
    .attr("y2", H)
    .attr("stroke", "currentColor")
    .attr("stroke-width", 1);

  /** legend */
  const ly = H + 30;
  svg
    .append("rect")
    .attr("x", -W / 2)
    .attr("y", ly)
    .attr("width", 14)
    .attr("height", 14)
    .attr("fill", secondary);
  svg
    .append("text")
    .attr("x", -W / 2 + 18)
    .attr("y", ly + 11)
    .text(labelFn(groupA))
    .attr("font-size", 14)
    .attr("fill", "currentColor");

  svg
    .append("rect")
    .attr("x", 20)
    .attr("y", ly)
    .attr("width", 14)
    .attr("height", 14)
    .attr("fill", primary);
  svg
    .append("text")
    .attr("x", 38)
    .attr("y", ly + 11)
    .text(labelFn(groupB))
    .attr("font-size", 14)
    .attr("fill", "currentColor");

  /** x axis labels */
  const xAxisA = d3
    .axisTop(
      d3.scaleLinear().domain([xMax, 0]).range([0, W / 2 - 10]),
    )
    .ticks(3)
    .tickFormat((d) => (Number(d) * 100).toFixed(1) + "%");

  const xAxisB = d3
    .axisTop(xScale)
    .ticks(3)
    .tickFormat((d) => (Number(d) * 100).toFixed(1) + "%");

  svg
    .append("g")
    .attr("transform", "translate(-" + (W / 2 - 10) + ",0)")
    .call(xAxisA)
    .attr("font-size", 11);

  svg
    .append("g")
    .call(xAxisB)
    .attr("font-size", 11);
};
