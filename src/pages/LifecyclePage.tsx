/**
 * LifecyclePage.tsx — Lifecycle Microbiome Atlas workspace
 * 全生命周期微生物组图谱工作台
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";

import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { API_BASE } from "@/util/apiBase";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";
import { countryName, AGE_GROUP_ZH } from "@/util/countries";
import { cachedFetch } from "@/util/apiCache";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { exportTable } from "@/util/export";
import { phylumColor } from "@/util/phylumColors";

import { AlphaDiversityChart } from "./lifecycle/AlphaDiversityChart";
import { TransitionPanel } from "./lifecycle/TransitionPanel";
import classes from "./LifecyclePage.module.css";

export interface LifecycleRow {
  age_group: string;
  sample_count: number;
  shannon_mean: number;
  shannon_sd: number;
  simpson_mean: number;
  simpson_sd: number;
  [genus: string]: number | string;
}

export interface LifecycleTopChange {
  genus: string;
  change: number;
  direction: "increase" | "decrease";
  pvalue?: number | null;
  adjusted_p?: number | null;
}

export interface LifecycleTransition {
  from: string;
  to: string;
  top_changes: LifecycleTopChange[];
}

export interface LifecycleKruskal {
  genus: string;
  kruskal_h: number;
  kruskal_p: number;
  adjusted_p: number;
  significant: boolean;
}

export interface LifecycleData {
  disease: string;
  country: string;
  total_samples: number;
  genera: string[];
  phylum_map: Record<string, string>;
  data: LifecycleRow[];
  transitions: LifecycleTransition[];
  kruskal_results: LifecycleKruskal[];
}

export interface LifecycleDualData {
  disease_data: LifecycleData;
  nc_data: LifecycleData;
}

interface DiseaseItem {
  name: string;
  sample_count: number;
}

const AGE_GROUP_ZH_MAP: Record<string, string> = {
  Infant: "婴儿",
  Child: "儿童",
  Adolescent: "青少年",
  Adult: "成人",
  Older_Adult: "老年人",
  Oldest_Old: "高龄老人",
  Centenarian: "百岁老人",
  Unknown: "未知",
};

const fmtP = (value?: number | null) => {
  if (value == null || Number.isNaN(value)) return "NA";
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const LifecyclePage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [countries, setCountries] = useState<string[]>([]);
  const [disease, setDisease] = useState("");
  const [country, setCountry] = useState("");
  const [topN, setTopN] = useState(15);
  const [viewMode, setViewMode] = useState<"area" | "compare">("area");
  const [diversityMetric, setDiversityMetric] = useState<"shannon" | "simpson">("shannon");
  const [isolatedGenus, setIsolatedGenus] = useState<string | null>(null);
  const [data, setData] = useState<LifecycleData | null>(null);
  const [dualData, setDualData] = useState<LifecycleDualData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const singleSvgRef = useRef<SVGSVGElement>(null);
  const compareDiseaseSvgRef = useRef<SVGSVGElement>(null);
  const compareNcSvgRef = useRef<SVGSVGElement>(null);

  const diseaseLabel = (name: string) => {
    if (name === "Healthy (NC)" || name === "NC") {
      return locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)";
    }
    return (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : diseaseDisplayNameI18n(name, locale);
  };

  const ageLabel = (name: string) => (
    locale === "zh"
      ? (AGE_GROUP_ZH_MAP[name] ?? AGE_GROUP_ZH[name] ?? name.replace(/_/g, " "))
      : name.replace(/_/g, " ")
  );

  useEffect(() => {
    cachedFetch<{ diseases: DiseaseItem[] }>(`${API_BASE}/api/disease-list`)
      .then((payload) => setDiseases(payload.diseases ?? []))
      .catch(() => {});
    cachedFetch<Record<string, string>>(`${API_BASE}/api/disease-names-zh`)
      .then(setDiseaseZh)
      .catch(() => {});
    cachedFetch<{ countries: string[] }>(`${API_BASE}/api/filter-options`)
      .then((payload) => setCountries(payload.countries ?? []))
      .catch(() => {});
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  useEffect(() => {
    if (!disease && viewMode === "compare") {
      setViewMode("area");
    }
  }, [disease, viewMode]);

  useEffect(() => {
    setLoading(true);
    setError("");
    setIsolatedGenus(null);

    const params = new URLSearchParams();
    if (disease) params.set("disease", disease);
    if (country) params.set("country", country);
    params.set("top_genera", topN.toString());

    const run = async () => {
      if (viewMode === "compare" && disease) {
        const response = await cachedFetch<LifecycleDualData>(`${API_BASE}/api/lifecycle-compare?${params.toString()}`);
        setDualData(response);
        setData(null);
        return;
      }

      const response = await cachedFetch<LifecycleData>(`${API_BASE}/api/lifecycle?${params.toString()}`);
      setData(response);
      setDualData(null);
    };

    run()
      .catch((err) => {
        setData(null);
        setDualData(null);
        setError(locale === "zh" ? "后端未启动或生命周期接口不可用" : `Lifecycle API error: ${(err as Error).message}`);
      })
      .finally(() => setLoading(false));
  }, [country, disease, locale, topN, viewMode]);

  useEffect(() => {
    if (viewMode !== "area" || !singleSvgRef.current || !data || data.data.length === 0) return;
    drawStackedArea(singleSvgRef.current, data, locale, isolatedGenus, ageLabel);
  }, [ageLabel, data, isolatedGenus, locale, viewMode]);

  useEffect(() => {
    if (viewMode !== "compare" || !dualData) return;
    if (compareDiseaseSvgRef.current && dualData.disease_data.data.length > 0) {
      drawStackedArea(compareDiseaseSvgRef.current, dualData.disease_data, locale, isolatedGenus, ageLabel);
    }
    if (compareNcSvgRef.current && dualData.nc_data.data.length > 0) {
      drawStackedArea(compareNcSvgRef.current, dualData.nc_data, locale, isolatedGenus, ageLabel);
    }
  }, [ageLabel, dualData, isolatedGenus, locale, viewMode]);

  const legendData = viewMode === "compare" ? dualData?.disease_data : data;

  const exportLifecycleTable = (payload: LifecycleData, fileName: string) => {
    exportTable(
      payload.data.map((row) => {
        const result: Record<string, number | string> = {
          age_group: row.age_group,
          sample_count: row.sample_count,
          shannon_mean: row.shannon_mean,
          shannon_sd: row.shannon_sd,
          simpson_mean: row.simpson_mean,
          simpson_sd: row.simpson_sd,
        };
        payload.genera.forEach((genus) => {
          result[genus] = row[genus];
        });
        return result;
      }),
      fileName,
    );
  };

  const renderLegend = (payload: LifecycleData) => (
    <div className={classes.legend}>
      {payload.genera.map((genus) => {
        const active = isolatedGenus === genus;
        const dimmed = isolatedGenus != null && !active;
        const swatch = genus === "Other" ? "#cbd5e1" : phylumColor(payload.phylum_map[genus] ?? "Unknown");
        return (
          <button
            key={genus}
            type="button"
            className={`${classes.legendItem} ${active ? classes.legendItemActive : ""} ${dimmed ? classes.legendItemDimmed : ""}`}
            onClick={() => setIsolatedGenus((prev) => (prev === genus ? null : genus))}
          >
            <span className={classes.legendDot} style={{ background: swatch }} />
            <span className={classes.legendName} style={{ fontStyle: genus === "Other" ? "normal" : "italic" }}>
              {genus === "Other" ? (locale === "zh" ? "其他" : "Other") : genus}
            </span>
            {genus !== "Other" ? (
              <span className={classes.legendPhylum}>({payload.phylum_map[genus] ?? "Unknown"})</span>
            ) : null}
          </button>
        );
      })}
    </div>
  );

  const renderSignificantCard = (payload: LifecycleData, label: string) => {
    const rows = payload.kruskal_results
      .filter((item) => item.significant)
      .sort((a, b) => a.adjusted_p - b.adjusted_p);

    return (
      <div className={classes.chartCard}>
        <div className={classes.cardHeader}>
          <div>
            <h3>{t("lifecycle.kruskalSig")}</h3>
            <p>{label}</p>
          </div>
          <div className={classes.statBadge}>{rows.length}</div>
        </div>
        {rows.length === 0 ? (
          <div className={classes.emptyHint}>
            {locale === "zh" ? "当前筛选下没有 FDR < 0.05 的显著年龄变化属。" : "No genus reaches FDR < 0.05 for age variation under the current filters."}
          </div>
        ) : (
          <div className={classes.sigChips}>
            {rows.map((row) => (
              <button
                key={row.genus}
                type="button"
                className={classes.sigChip}
                onClick={() => setIsolatedGenus((prev) => (prev === row.genus ? null : row.genus))}
                title={`H=${row.kruskal_h.toFixed(2)}, adj.p=${fmtP(row.adjusted_p)}`}
              >
                <span className={classes.sigChipName}>{row.genus}</span>
                <span className={classes.sigChipMeta}>
                  H={row.kruskal_h.toFixed(1)}, adj.p={fmtP(row.adjusted_p)}
                </span>
              </button>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("lifecycle.back")}</Link>
        <h1>{t("lifecycle.title")}</h1>
        <p>{t("lifecycle.subtitle")}</p>
      </div>

      <div className={classes.notice}>
        {locale === "zh"
          ? "该模块展示按年龄分层的分类学丰度轨迹与非参数统计，反映群体级年龄梯度，不等同于个体纵向随访。"
          : "This module shows age-stratified taxonomic trajectories and non-parametric statistics at the cohort level. It is not an individual longitudinal follow-up trace."}
      </div>

      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("lifecycle.filterDisease")}</label>
          <select className={classes.select} value={disease} onChange={(e) => setDisease(e.target.value)}>
            <option value="">{t("lifecycle.allDiseases")}</option>
            {sortedDiseases.map((item) => (
              <option key={item.name} value={item.name}>
                {`${diseaseLabel(item.name)} (${item.sample_count.toLocaleString()})`}
              </option>
            ))}
          </select>
        </div>

        <div className={classes.field}>
          <label>{t("lifecycle.filterCountry")}</label>
          <select className={classes.select} value={country} onChange={(e) => setCountry(e.target.value)}>
            <option value="">{t("lifecycle.allCountries")}</option>
            {countries.map((code) => {
              const display = countryName(code, locale);
              const showCode = display !== code && code !== "unknown" && code.length <= 3;
              return (
                <option key={code} value={code}>
                  {showCode ? `${display} (${code})` : display}
                </option>
              );
            })}
          </select>
        </div>

        <div className={classes.field}>
          <label>{`${t("lifecycle.topN")}: ${topN}`}</label>
          <input
            className={classes.slider}
            type="range"
            min={5}
            max={30}
            step={5}
            value={topN}
            onChange={(e) => setTopN(parseInt(e.target.value, 10))}
          />
        </div>

        <div className={classes.field}>
          <label>{t("lifecycle.viewMode")}</label>
          <select className={classes.select} value={viewMode} onChange={(e) => setViewMode(e.target.value as "area" | "compare")}>
            <option value="area">{t("lifecycle.modeNormal")}</option>
            <option value="compare" disabled={!disease}>{t("lifecycle.modeCompare")}</option>
          </select>
        </div>
      </div>

      {loading ? <div className={classes.loading}>{t("search.searching")}</div> : null}
      {error ? <div className={classes.error}>{error}</div> : null}

      {!loading && !error && legendData ? (
        <>
          <div className={classes.summaryRow}>
            <div className={classes.summaryCard}>
              <span className={classes.summaryLabel}>{t("lifecycle.sampleCount")}</span>
              <strong>{legendData.total_samples.toLocaleString()}</strong>
            </div>
            <div className={classes.summaryCard}>
              <span className={classes.summaryLabel}>{t("lifecycle.topN")}</span>
              <strong>{legendData.genera.filter((genus) => genus !== "Other").length}</strong>
            </div>
            <div className={classes.summaryCard}>
              <span className={classes.summaryLabel}>{t("lifecycle.kruskalSig")}</span>
              <strong>{legendData.kruskal_results.filter((row) => row.significant).length}</strong>
            </div>
            <div className={classes.summaryCard}>
              <span className={classes.summaryLabel}>{t("lifecycle.viewMode")}</span>
              <strong>{viewMode === "compare" ? t("lifecycle.modeCompare") : t("lifecycle.modeNormal")}</strong>
            </div>
          </div>

          {viewMode === "area" && data ? (
            <>
              <div className={classes.chartCard}>
                <div className={classes.cardHeader}>
                  <div>
                    <h3>{t("lifecycle.stackedArea")}</h3>
                    <p>
                      {`${diseaseLabel(data.disease)} · ${country ? countryName(country, locale) : t("lifecycle.allCountries")} · ${data.total_samples.toLocaleString()} ${locale === "zh" ? "个样本" : "samples"}`}
                    </p>
                  </div>
                  <div className={classes.actionRow}>
                    <button type="button" onClick={() => exportLifecycleTable(data, `lifecycle_${Date.now()}`)}>{t("export.csv")}</button>
                    <button type="button" onClick={() => singleSvgRef.current && exportSVG(singleSvgRef.current, `lifecycle_${Date.now()}`)}>{t("export.svg")}</button>
                    <button type="button" onClick={() => singleSvgRef.current && exportPNG(singleSvgRef.current, `lifecycle_${Date.now()}`)}>{t("export.png")}</button>
                  </div>
                </div>
                <svg ref={singleSvgRef} className={classes.chart} />
                {renderLegend(data)}
              </div>

              <div className={classes.chartCard}>
                <div className={classes.cardHeader}>
                  <div>
                    <h3>{t("lifecycle.alphaDiversity")}</h3>
                    <p>{locale === "zh" ? "按年龄段展示 Shannon / Simpson 均值与标准差。" : "Age-group mean diversity with one-standard-deviation error bars."}</p>
                  </div>
                  <div className={classes.metricToggle}>
                    <button
                      type="button"
                      className={diversityMetric === "shannon" ? classes.metricButtonActive : classes.metricButton}
                      onClick={() => setDiversityMetric("shannon")}
                    >
                      Shannon
                    </button>
                    <button
                      type="button"
                      className={diversityMetric === "simpson" ? classes.metricButtonActive : classes.metricButton}
                      onClick={() => setDiversityMetric("simpson")}
                    >
                      Simpson
                    </button>
                  </div>
                </div>
                <AlphaDiversityChart data={data.data} locale={locale} metric={diversityMetric} />
              </div>

              <TransitionPanel transitions={data.transitions} locale={locale} />
              {renderSignificantCard(data, diseaseLabel(data.disease))}
            </>
          ) : null}

          {viewMode === "compare" && dualData ? (
            <>
              <div className={classes.compareGrid}>
                <div className={classes.chartCard}>
                  <div className={classes.cardHeader}>
                    <div>
                      <h3>{diseaseLabel(dualData.disease_data.disease)}</h3>
                      <p>{`${dualData.disease_data.total_samples.toLocaleString()} ${locale === "zh" ? "个样本" : "samples"}`}</p>
                    </div>
                    <div className={classes.actionRow}>
                      <button type="button" onClick={() => exportLifecycleTable(dualData.disease_data, `lifecycle_disease_${Date.now()}`)}>{t("export.csv")}</button>
                      <button type="button" onClick={() => compareDiseaseSvgRef.current && exportSVG(compareDiseaseSvgRef.current, `lifecycle_disease_${Date.now()}`)}>{t("export.svg")}</button>
                      <button type="button" onClick={() => compareDiseaseSvgRef.current && exportPNG(compareDiseaseSvgRef.current, `lifecycle_disease_${Date.now()}`)}>{t("export.png")}</button>
                    </div>
                  </div>
                  <svg ref={compareDiseaseSvgRef} className={classes.chart} />
                </div>

                <div className={classes.chartCard}>
                  <div className={classes.cardHeader}>
                    <div>
                      <h3>{locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)"}</h3>
                      <p>{`${dualData.nc_data.total_samples.toLocaleString()} ${locale === "zh" ? "个样本" : "samples"}`}</p>
                    </div>
                    <div className={classes.actionRow}>
                      <button type="button" onClick={() => exportLifecycleTable(dualData.nc_data, `lifecycle_nc_${Date.now()}`)}>{t("export.csv")}</button>
                      <button type="button" onClick={() => compareNcSvgRef.current && exportSVG(compareNcSvgRef.current, `lifecycle_nc_${Date.now()}`)}>{t("export.svg")}</button>
                      <button type="button" onClick={() => compareNcSvgRef.current && exportPNG(compareNcSvgRef.current, `lifecycle_nc_${Date.now()}`)}>{t("export.png")}</button>
                    </div>
                  </div>
                  <svg ref={compareNcSvgRef} className={classes.chart} />
                </div>
              </div>

              {renderLegend(dualData.disease_data)}

              <div className={classes.compareGrid}>
                <div className={classes.chartCard}>
                  <div className={classes.cardHeader}>
                    <div>
                      <h3>{`${diseaseLabel(dualData.disease_data.disease)} · ${t("lifecycle.alphaDiversity")}`}</h3>
                    </div>
                    <div className={classes.metricToggle}>
                      <button
                        type="button"
                        className={diversityMetric === "shannon" ? classes.metricButtonActive : classes.metricButton}
                        onClick={() => setDiversityMetric("shannon")}
                      >
                        Shannon
                      </button>
                      <button
                        type="button"
                        className={diversityMetric === "simpson" ? classes.metricButtonActive : classes.metricButton}
                        onClick={() => setDiversityMetric("simpson")}
                      >
                        Simpson
                      </button>
                    </div>
                  </div>
                  <AlphaDiversityChart data={dualData.disease_data.data} locale={locale} metric={diversityMetric} />
                </div>

                <div className={classes.chartCard}>
                  <div className={classes.cardHeader}>
                    <div>
                      <h3>{`${locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)"} · ${t("lifecycle.alphaDiversity")}`}</h3>
                    </div>
                  </div>
                  <AlphaDiversityChart data={dualData.nc_data.data} locale={locale} metric={diversityMetric} />
                </div>
              </div>

              <div className={classes.compareGrid}>
                <TransitionPanel transitions={dualData.disease_data.transitions} locale={locale} heading={diseaseLabel(dualData.disease_data.disease)} />
                <TransitionPanel transitions={dualData.nc_data.transitions} locale={locale} heading={locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)"} />
              </div>

              <div className={classes.compareGrid}>
                {renderSignificantCard(dualData.disease_data, diseaseLabel(dualData.disease_data.disease))}
                {renderSignificantCard(dualData.nc_data, locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)")}
              </div>
            </>
          ) : null}
        </>
      ) : null}
    </div>
  );
};

export default LifecyclePage;

function drawStackedArea(
  svgEl: SVGSVGElement,
  lifecycle: LifecycleData,
  locale: string,
  isolatedGenus: string | null,
  ageLabel: (name: string) => string,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  if (!lifecycle.data.length) {
    svg.attr("viewBox", "0 0 840 200");
    svg.append("text")
      .attr("x", 24)
      .attr("y", 96)
      .attr("fill", "currentColor")
      .attr("font-size", 14)
      .text(locale === "zh" ? "暂无生命周期轨迹数据" : "No lifecycle trajectory data available");
    return;
  }

  const margin = { top: 58, right: 20, bottom: 72, left: 56 };
  const width = 860;
  const height = 430;
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const rows = lifecycle.data;
  const genera = lifecycle.genera;
  const ageGroups = rows.map((row) => row.age_group);
  const xScale = d3.scalePoint<string>()
    .domain(ageGroups)
    .range([0, innerWidth])
    .padding(0.28);
  const yScale = d3.scaleLinear()
    .domain([0, 100])
    .range([innerHeight, 0]);

  const stack = d3.stack<LifecycleRow>()
    .keys(genera)
    .value((row, key) => Number(row[key] ?? 0));

  const series = stack(rows);
  const tooltip = d3.select("body")
    .selectAll(".lifecycle-tooltip")
    .data([null])
    .join("div")
    .attr("class", "lifecycle-tooltip")
    .style("position", "absolute")
    .style("pointer-events", "none")
    .style("opacity", 0)
    .style("background", "rgba(255,255,255,0.98)")
    .style("border", "1px solid #dbe3ef")
    .style("border-radius", "10px")
    .style("padding", "10px 12px")
    .style("font-size", "0.8rem")
    .style("line-height", "1.45")
    .style("color", "#0f172a")
    .style("box-shadow", "0 12px 28px rgba(15, 23, 42, 0.18)")
    .style("z-index", "1000");

  const area = d3.area<d3.SeriesPoint<LifecycleRow>>()
    .x((point) => xScale(point.data.age_group) ?? 0)
    .y0((point) => yScale(point[0]))
    .y1((point) => yScale(point[1]))
    .curve(d3.curveMonotoneX);

  const chartRoot = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  chartRoot.append("g")
    .selectAll(".grid-line")
    .data([0, 20, 40, 60, 80, 100])
    .join("line")
    .attr("x1", 0)
    .attr("x2", innerWidth)
    .attr("y1", (tick) => yScale(tick))
    .attr("y2", (tick) => yScale(tick))
    .attr("stroke", "rgba(148, 163, 184, 0.18)")
    .attr("stroke-dasharray", "4,4");

  const nearestAgeGroup = (mouseX: number) => ageGroups.reduce((best, current) => {
    const bestDist = Math.abs((xScale(best) ?? 0) - mouseX);
    const currentDist = Math.abs((xScale(current) ?? 0) - mouseX);
    return currentDist < bestDist ? current : best;
  }, ageGroups[0]);

  chartRoot.selectAll(".layer")
    .data(series)
    .join("path")
    .attr("class", "layer")
    .attr("d", area)
    .attr("fill", (layer) => layer.key === "Other" ? "#cbd5e1" : phylumColor(lifecycle.phylum_map[layer.key] ?? "Unknown"))
    .attr("opacity", (layer) => {
      if (!isolatedGenus) return layer.key === "Other" ? 0.68 : 0.9;
      return isolatedGenus === layer.key ? 0.98 : 0.12;
    })
    .attr("stroke", (layer) => isolatedGenus === layer.key ? "#f8fafc" : "transparent")
    .attr("stroke-width", (layer) => isolatedGenus === layer.key ? 1.2 : 0)
    .on("mousemove", function onMove(event, layer) {
      const [pointerX] = d3.pointer(event, svgEl);
      const localX = Math.max(0, Math.min(innerWidth, pointerX - margin.left));
      const nearest = nearestAgeGroup(localX);
      const row = rows.find((item) => item.age_group === nearest);
      if (!row) return;

      const abundance = Number(row[layer.key] ?? 0);
      const phylum = layer.key === "Other" ? "Other" : (lifecycle.phylum_map[layer.key] ?? "Unknown");
      tooltip
        .html([
          `<strong>${ageLabel(nearest)}</strong>`,
          layer.key === "Other" ? (locale === "zh" ? "其他属汇总" : "Other genera aggregate") : `<i>${layer.key}</i>`,
          `${locale === "zh" ? "门" : "Phylum"}: ${phylum}`,
          `${locale === "zh" ? "相对丰度" : "Relative abundance"}: ${abundance.toFixed(2)}%`,
          `${locale === "zh" ? "样本量" : "Sample count"}: ${row.sample_count.toLocaleString()}`,
          `${locale === "zh" ? "Shannon 均值" : "Mean Shannon"}: ${row.shannon_mean.toFixed(3)}`,
        ].join("<br/>"))
        .style("left", `${event.pageX + 14}px`)
        .style("top", `${event.pageY - 24}px`)
        .style("opacity", 1);
    })
    .on("mouseout", () => {
      tooltip.style("opacity", 0);
    });

  const sampleBadges = chartRoot.selectAll(".sample-badge")
    .data(rows)
    .join("g")
    .attr("class", "sample-badge")
    .attr("transform", (row) => `translate(${xScale(row.age_group) ?? 0},-18)`);

  sampleBadges.each(function appendBadge(row) {
    const group = d3.select(this);
    const label = `n=${Number(row.sample_count).toLocaleString()}`;
    const badgeWidth = Math.max(40, label.length * 6.3 + 12);
    group.append("rect")
      .attr("x", -badgeWidth / 2)
      .attr("y", -11)
      .attr("width", badgeWidth)
      .attr("height", 18)
      .attr("rx", 9)
      .attr("fill", "rgba(15, 23, 42, 0.9)")
      .attr("stroke", "rgba(148, 163, 184, 0.45)");
    group.append("text")
      .attr("text-anchor", "middle")
      .attr("y", 2)
      .attr("fill", "#f8fafc")
      .attr("font-size", 9)
      .text(label);
  });

  chartRoot.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(xScale).tickFormat((value) => ageLabel(value)))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("transform", "rotate(-22)")
    .style("text-anchor", "end");

  chartRoot.append("g")
    .call(d3.axisLeft(yScale).ticks(5).tickFormat((value) => `${value}%`))
    .attr("font-size", 10);

  chartRoot.append("text")
    .attr("x", innerWidth / 2)
    .attr("y", innerHeight + 58)
    .attr("fill", "currentColor")
    .attr("font-size", 11)
    .attr("text-anchor", "middle")
    .text(locale === "zh" ? "年龄阶段" : "Age group");

  chartRoot.append("text")
    .attr("transform", `translate(-42,${innerHeight / 2}) rotate(-90)`)
    .attr("fill", "currentColor")
    .attr("font-size", 11)
    .attr("text-anchor", "middle")
    .text(locale === "zh" ? "相对丰度 (%)" : "Relative abundance (%)");
}
