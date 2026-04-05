import { useEffect } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import Placeholder from "@/components/Placeholder";
import type { AbundanceSummary } from "@/data";
import { useI18n } from "@/i18n";
import { useData } from "@/data";
import { diseaseShortNameI18n } from "@/util/diseaseNames";
import { AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
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

const PHYLUM_MAP: Record<string, string> = {
  Bacteroides: "Bacteroidota",
  Parabacteroides: "Bacteroidota",
  Alistipes: "Bacteroidota",
  Segatella: "Bacteroidota",
  Bifidobacterium: "Actinomycetota",
  Collinsella: "Actinomycetota",
  Faecalibacterium: "Bacillota",
  Blautia: "Bacillota",
  Roseburia: "Bacillota",
  Ruminococcus: "Bacillota",
  Agathobacter: "Bacillota",
  Gemmiger: "Bacillota",
  Anaerostipes: "Bacillota",
  Dialister: "Bacillota",
  Clostridium: "Bacillota",
  Enterococcus: "Bacillota",
  Streptococcus: "Bacillota",
  Staphylococcus: "Bacillota",
  Veillonella: "Bacillota",
  Mediterraneibacter: "Bacillota",
  Fusicatenibacter: "Bacillota",
  Dorea: "Bacillota",
  Thomasclavelia: "Bacillota",
  Anaerobutyricum: "Bacillota",
  Enterocloster: "Bacillota",
  Phascolarctobacterium: "Bacillota",
  Shigella: "Pseudomonadota",
  Klebsiella: "Pseudomonadota",
  Haemophilus: "Pseudomonadota",
  Akkermansia: "Verrucomicrobiota",
};

const PHYLUM_COLORS: Record<string, string> = {
  Bacillota: "#e23fff",
  Bacteroidota: "#6ec1e4",
  Pseudomonadota: "#ff6b6b",
  Actinomycetota: "#ffd93d",
  Verrucomicrobiota: "#6bcb77",
};

const PhenotypeCharts = () => {
  const summary = useData((s) => s.summary);
  const abundance = useData((s) => s.abundance);
  const filters = useData((s) => s.filters);
  const { t, locale } = useI18n();

  useEffect(() => {
    if (!summary || !abundance) return;
    drawAgeChart(summary.age_counts, summary.age_sex_cross, filters.age_groups, locale);
    drawDiseaseChart(summary.disease_counts, filters.diseases, locale);
    drawHeatmap(summary.age_disease_cross, summary.top20_diseases.slice(0, 15), locale);
    drawGenusRankChart(abundance, locale);
  }, [summary, abundance, filters, locale]);

  if (!summary || !abundance)
    return <Placeholder height={300}>Loading charts...</Placeholder>;

  return (
    <section>
      <h2>{t("home.phenotypeOverview")}</h2>
      <div className={classes.grid}>
        <div className="sub-section">
          <h3>{t("home.ageDistribution")}</h3>
          <svg id="age-chart" className="chart" viewBox="-80 -40 480 380" style={{ minHeight: 360 }} />
        </div>
        <div className="sub-section">
          <h3>{t("home.top20Diseases")}</h3>
          <svg id="disease-chart" className="chart" viewBox="-240 -20 760 560" style={{ minHeight: 420 }} />
        </div>
      </div>
      <div className="sub-section" style={{ marginTop: "1.5rem" }}>
        <h3>{t("home.heatmapTitle")}</h3>
        {/* viewBox: left=-160(y轴标签), top=-80(列头), width=1100(图+图例), height=580(含旋转标签) */}
        <svg id="heatmap" className="chart" viewBox="-160 -80 1100 580" />
      </div>
      <div className="sub-section" style={{ marginTop: "1.5rem" }}>
        <h3>{t("home.topGeneraTitle")}</h3>
        <p style={{ color: "var(--light-gray)", fontSize: "0.85rem" }}>
          {t("home.topGeneraDesc")}
        </p>
        <svg id="genus-rank-chart" className="chart" viewBox="-180 -20 860 600" />
      </div>
    </section>
  );
};

export default PhenotypeCharts;

const ageName = (key: string, locale: string) =>
  locale === "zh" ? (AGE_GROUP_ZH[key] ?? key.replace(/_/g, " ")) : key.replace(/_/g, " ");
const sexName = (key: string, locale: string) =>
  locale === "zh" ? (SEX_ZH[key] ?? key) : key;
const dName = (key: string, locale: string, maxLen = 22) =>
  diseaseShortNameI18n(key, locale, maxLen);

const drawAgeChart = (
  ageCounts: Record<string, number>,
  ageSex: { age_group: string; sex: string; count: number }[],
  activeAgeGroups: string[] = [],
  locale: string = "en",
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#age-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();
  const W = 400, H = 340;
  const primary = getCssVariable("--primary");
  const secondary = getCssVariable("--secondary");
  const groups = AGE_ORDER.filter((g) => ageCounts[g] > 0);
  const sexColors: Record<string, string> = { female: secondary, male: primary, unknown: getCssVariable("--gray") };
  const data = groups.map((g) => {
    const bySex = Object.fromEntries(ageSex.filter((r) => r.age_group === g).map((r) => [r.sex, r.count]));
    return { group: g, female: bySex["female"] ?? 0, male: bySex["male"] ?? 0, unknown: bySex["unknown"] ?? 0, total: ageCounts[g] ?? 0 };
  });
  const xMax = d3.max(data, (d) => d.total) ?? 1;
  const xScale = d3.scaleLinear().domain([0, xMax]).range([0, W]);
  const yScale = d3.scaleBand().domain(groups).range([0, H]).padding(0.2);
  const sexKeys = ["female", "male", "unknown"] as const;
  for (const sex of sexKeys) {
    svg.selectAll(`.bar-${sex}`).data(data).join("rect")
      .attr("class", `bar-${sex}`)
      .attr("x", (d) => sex === "male" ? xScale(d.female) : sex === "unknown" ? xScale(d.female + d.male) : 0)
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
      .attr("data-tooltip", (d) => renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "\u5e74\u9f84\u7ec4" : "Age Group"}</span><span>{ageName(d.group, locale)}</span>
          <span>{locale === "zh" ? "\u603b\u8ba1" : "Total"}</span><span>{formatNumber(d.total, false)}</span>
          <span>{sexName("female", locale)}</span><span>{formatNumber(d.female, false)}</span>
          <span>{sexName("male", locale)}</span><span>{formatNumber(d.male, false)}</span>
          <span>{locale === "zh" ? "\u6027\u522b\u672a\u77e5" : "Unknown sex"}</span><span>{formatNumber(d.unknown, false)}</span>
        </div>,
      ));
  }
  svg.append("g").call(d3.axisLeft(yScale).tickFormat((d) => ageName(d, locale))).attr("font-size", "14px");
  svg.append("g").attr("transform", `translate(0,${H})`).call(d3.axisBottom(xScale).ticks(4).tickFormat((d) => formatNumber(Number(d)))).attr("font-size", "12px");
  const legendX = W + 10;
  sexKeys.forEach((s, i) => {
    svg.append("rect").attr("x", legendX).attr("y", i * 20).attr("width", 14).attr("height", 14).attr("fill", sexColors[s]).attr("opacity", s === "unknown" ? 0.4 : 0.85);
    svg.append("text").attr("x", legendX + 18).attr("y", i * 20 + 11).text(sexName(s, locale)).attr("font-size", "13px").attr("fill", "currentColor");
  });
};

const drawDiseaseChart = (diseaseCounts: Record<string, number>, activeDiseases: string[] = [], locale: string = "en") => {
  const svg = d3.select<SVGSVGElement, unknown>("#disease-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();
  const W = 420, H = 530;
  const primary = getCssVariable("--primary");
  const secondary = getCssVariable("--secondary");
  const data = Object.entries(diseaseCounts).sort((a, b) => b[1] - a[1]).slice(0, 20);
  const xScale = d3.scaleLog().domain([1, d3.max(data, (d) => d[1]) ?? 1]).range([0, W]);
  const yScale = d3.scaleBand().domain(data.map((d) => d[0])).range([0, H]).padding(0.15);
  const highlight = (name: string) => name === "NC" ? secondary : name === "unknown" ? getCssVariable("--gray") : primary;
  svg.selectAll(".bar").data(data).join("rect")
    .attr("class", "bar").attr("x", 0)
    .attr("y", (d) => yScale(d[0]) ?? 0)
    .attr("width", (d) => xScale(d[1]))
    .attr("height", yScale.bandwidth())
    .attr("fill", (d) => highlight(d[0]))
    .attr("opacity", (d) => {
      if (activeDiseases.length === 0) return 0.85;
      return activeDiseases.includes(d[0]) ? 1 : 0.15;
    })
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", ([name, count]) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "\u75be\u75c5" : "Disease"}</span><span>{dName(name, locale, 30)}</span>
        <span>{locale === "zh" ? "\u6837\u672c\u6570" : "Samples"}</span><span>{formatNumber(count, false)}</span>
      </div>,
    ));
  svg.append("g").call(d3.axisLeft(yScale).tickFormat((d) => dName(d, locale, 30))).attr("font-size", "14px");
  svg.append("g").attr("transform", `translate(0,${H})`).call(d3.axisBottom(xScale).ticks(4).tickFormat((d) => formatNumber(Number(d)))).attr("font-size", "14px");
};

const drawHeatmap = (
  crossData: { age_group: string; disease: string; count: number }[],
  diseases: string[],
  locale: string = "en",
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#heatmap");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  // Layout constants / 布局常量
  const W = 800, H = 320;
  const LEGEND_W = 14;
  const LEGEND_GAP = 30;   // gap between heatmap and legend
  const primary = getCssVariable("--primary");

  const ages = AGE_ORDER.filter((a) => crossData.some((r) => r.age_group === a));
  const filteredData = crossData.filter((d) => diseases.includes(d.disease));
  const xScale = d3.scaleBand().domain(diseases).range([0, W]).padding(0.05);
  const yScale = d3.scaleBand().domain(ages).range([0, H]).padding(0.05);
  const maxVal = d3.max(filteredData, (d) => d.count) ?? 1;
  const color = d3.scaleSequential().domain([0, maxVal]).interpolator(d3.interpolate("#1a1a2e", primary));
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
    svg.selectAll(".cell-label").remove();
    if (highlightAge || highlightDisease) {
      svg.selectAll(".cell-label")
        .data(filteredData.filter((d) => {
          const matchAge = !highlightAge || d.age_group === highlightAge;
          const matchDisease = !highlightDisease || d.disease === highlightDisease;
          return matchAge && matchDisease && d.count > 0;
        }))
        .join("text").attr("class", "cell-label")
        .attr("x", (d) => (xScale(d.disease) ?? 0) + xScale.bandwidth() / 2)
        .attr("y", (d) => (yScale(d.age_group) ?? 0) + yScale.bandwidth() / 2)
        .attr("text-anchor", "middle").attr("dominant-baseline", "middle")
        .attr("fill", (d) => d.count > maxVal * 0.5 ? "#fff" : "var(--light-gray)")
        .attr("font-size", 9).attr("font-weight", 600).attr("pointer-events", "none")
        .text((d) => formatNumber(d.count));
    }
  };

  const cells = svg.selectAll(".cell").data(filteredData).join("rect")
    .attr("class", "cell")
    .attr("x", (d) => xScale(d.disease) ?? 0)
    .attr("y", (d) => yScale(d.age_group) ?? 0)
    .attr("width", xScale.bandwidth())
    .attr("height", yScale.bandwidth())
    .attr("fill", (d) => color(d.count))
    .attr("rx", 2)
    .style("cursor", "pointer")
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", (d) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "年龄组" : "Age Group"}</span><span>{ageName(d.age_group, locale)}</span>
        <span>{locale === "zh" ? "疾病" : "Disease"}</span><span>{dName(d.disease, locale, 50)}</span>
        <span>{locale === "zh" ? "样本数" : "Samples"}</span><span>{formatNumber(d.count, false)}</span>
      </div>,
    ))
    .on("mouseenter", (_, d) => { highlightAge = d.age_group; highlightDisease = d.disease; updateHighlight(); })
    .on("mouseleave", () => { highlightAge = null; highlightDisease = null; updateHighlight(); });

  // X axis — full disease name, rotated -45°, anchor "end"
  // 疾病名不截断（最长40字符），旋转-45°便于阅读
  svg.append("g")
    .attr("transform", `translate(0,${H})`)
    .call(d3.axisBottom(xScale).tickFormat((d) => dName(d, locale, 40)))
    .attr("font-size", "11px")
    .selectAll("text")
    .attr("transform", "rotate(-45)")
    .attr("text-anchor", "end")
    .attr("dx", "-0.5em")
    .attr("dy", "0.1em")
    .style("cursor", "pointer")
    .on("click", (_, d) => {
      highlightDisease = highlightDisease === d ? null : (d as string);
      highlightAge = null;
      updateHighlight();
    });

  // Y axis — age group labels
  svg.append("g")
    .call(d3.axisLeft(yScale).tickFormat((d) => ageName(d, locale)))
    .attr("font-size", "14px")
    .selectAll("text").style("cursor", "pointer")
    .on("click", (_, d) => {
      highlightAge = highlightAge === d ? null : (d as string);
      highlightDisease = null;
      updateHighlight();
    });

  // ── Legend: vertical gradient bar on the RIGHT side ─────────────────────
  // 图例：垂直渐变色条，放在热图右侧 LEGEND_GAP 像素处
  const legendX = W + LEGEND_GAP;
  const defs = svg.append("defs");
  const grad = defs.append("linearGradient")
    .attr("id", "heatmap-gradient")
    .attr("x1", "0%").attr("x2", "0%")
    .attr("y1", "100%").attr("y2", "0%");
  grad.append("stop").attr("offset", "0%").attr("stop-color", "#1a1a2e");
  grad.append("stop").attr("offset", "100%").attr("stop-color", primary);

  svg.append("rect")
    .attr("x", legendX).attr("y", 0)
    .attr("width", LEGEND_W).attr("height", H)
    .attr("fill", "url(#heatmap-gradient)").attr("rx", 3);

  // Legend labels: max at top, 0 at bottom, "Samples" label in middle
  svg.append("text")
    .attr("x", legendX + LEGEND_W + 6).attr("y", 8)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 11).attr("fill", "var(--light-gray)")
    .text(formatNumber(maxVal));

  svg.append("text")
    .attr("x", legendX + LEGEND_W + 6).attr("y", H / 2)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 11).attr("fill", "var(--light-gray)")
    .attr("transform", `rotate(-90,${legendX + LEGEND_W + 40},${H / 2})`)
    .text(locale === "zh" ? "样本数" : "Samples");

  svg.append("text")
    .attr("x", legendX + LEGEND_W + 6).attr("y", H)
    .attr("dominant-baseline", "auto")
    .attr("font-size", 11).attr("fill", "var(--light-gray)")
    .text("0");
};

const drawGenusRankChart = (
  abundance: AbundanceSummary,
  locale: string = "en",
) => {
  const svg = d3.select<SVGSVGElement, unknown>("#genus-rank-chart");
  if (!svg.node()) return;
  svg.selectAll("*").remove();

  const W = 520;
  const H = 540;
  const genusAbundance: Record<string, number> = {};
  const phylumMap = abundance.phylum_map ?? PHYLUM_MAP;

  for (const genus of abundance.genera) {
    const values = Object.values(abundance.by_disease).map((entry) => entry[genus] ?? 0);
    genusAbundance[genus] = d3.mean(values) ?? 0;
  }

  const top30 = [...abundance.genera]
    .sort((a, b) => (genusAbundance[b] ?? 0) - (genusAbundance[a] ?? 0))
    .slice(0, 30);

  const xScale = d3.scaleLinear()
    .domain([0, d3.max(top30, (genus) => genusAbundance[genus]) ?? 0.1])
    .range([0, W]);
  const yScale = d3.scaleBand().domain(top30).range([0, H]).padding(0.15);

  svg.selectAll(".bar")
    .data(top30)
    .join("rect")
    .attr("class", "bar")
    .attr("x", 0)
    .attr("y", (genus) => yScale(genus) ?? 0)
    .attr("width", (genus) => xScale(genusAbundance[genus] ?? 0))
    .attr("height", yScale.bandwidth())
    .attr("fill", (genus) => PHYLUM_COLORS[phylumMap[genus] ?? ""] ?? getCssVariable("--primary"))
    .attr("opacity", 0.85)
    .attr("rx", 2)
    .attr("role", "graphics-symbol")
    .attr("data-tooltip", (genus) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "菌属" : "Genus"}</span>
        <span><i>{genus}</i></span>
        <span>{locale === "zh" ? "所属门" : "Phylum"}</span>
        <span>{phylumMap[genus] ?? "Unknown"}</span>
        <span>{locale === "zh" ? "平均丰度" : "Mean Abundance"}</span>
        <span>{((genusAbundance[genus] ?? 0) * 100).toFixed(3)}%</span>
      </div>,
    ));

  svg.append("g")
    .call(d3.axisLeft(yScale))
    .attr("font-size", "12px")
    .selectAll("text")
    .attr("font-style", "italic");

  svg.append("g")
    .attr("transform", `translate(0,${H})`)
    .call(d3.axisBottom(xScale).tickFormat((value) => `${(Number(value) * 100).toFixed(2)}%`))
    .attr("font-size", "11px");
};
