/**
 * HealthIndexPanel.tsx
 * Gut Microbiome Health Index (GMHI) — population distribution + user scoring workbench
 */
import { useEffect, useRef, useState, type DragEvent } from "react";

import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { exportElementPNG } from "@/util/chartExport";
import { exportTable } from "@/util/export";

import ContributionChart from "./ContributionChart";
import classes from "./HealthIndexPanel.module.css";

interface HistBin {
  bin_start: number;
  bin_end: number;
  nc_count: number;
  disease_count: number;
}

interface PopStats {
  n: number;
  mean: number;
  median: number;
  std: number;
  p10: number;
  p25: number;
  p75: number;
  p90: number;
}

interface GenusEntry {
  genus: string;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  weight: number;
  mean_nc: number;
  mean_disease: number;
}

interface PopulationData {
  histogram: HistBin[];
  nc_stats: PopStats;
  disease_stats: PopStats;
}

interface ReferenceData {
  health_genera: GenusEntry[];
  disease_genera: GenusEntry[];
  n_nc_samples: number;
  n_disease_samples: number;
  population: PopulationData;
}

interface HealthDetail {
  genus: string;
  abundance: number;
  weight: number;
  contribution: number;
}

interface DeviationEntry {
  genus: string;
  user_abundance: number;
  nc_mean: number;
  nc_median: number;
  nc_p10: number;
  nc_p25: number;
  nc_p75: number;
  nc_p90: number;
  status: string;
}

// Multinomial softmax GBHI response (10-class classifier)
// Backend endpoint: POST /api/health_score
interface GbhiResult {
  p_nc: number;
  p_cdi: number;
  p_cd: number;
  p_uc: number;
  p_ra: number;
  p_hiv: number;
  p_adenoma: number;
  p_obesity: number;
  p_ibs: number;
  p_crc: number;
  health_score: number; // p_nc * 100, 0-100
  tier: "high" | "moderate" | "low";
}

// Ordered disease class labels for the 10-class softmax output
const GBHI_DISEASE_CLASSES: Array<{ key: keyof GbhiResult; label: string }> = [
  { key: "p_cdi", label: "CDI" },
  { key: "p_cd", label: "CD" },
  { key: "p_uc", label: "UC" },
  { key: "p_ra", label: "RA" },
  { key: "p_hiv", label: "HIV" },
  { key: "p_adenoma", label: "Adenoma" },
  { key: "p_obesity", label: "Obesity" },
  { key: "p_ibs", label: "IBS" },
  { key: "p_crc", label: "CRC" },
];

interface HealthResult {
  score: number;
  raw_score: number;
  raw_score_weighted: number;
  category: string;
  population_percentile: number;
  health_genera_matched: number;
  disease_genera_matched: number;
  health_genera_sum: number;
  disease_genera_sum: number;
  health_genera_detail: HealthDetail[];
  disease_genera_detail: HealthDetail[];
  per_genus_deviation: DeviationEntry[];
  reference: {
    n_nc_samples: number;
    n_disease_samples: number;
    health_genera_total: number;
    disease_genera_total: number;
  };
}

function parseAbundanceText(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  const lines = text.trim().split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const lower = trimmed.toLowerCase();
    if (lower.startsWith("genus") || lower.startsWith("taxon") || lower.startsWith("name")) continue;
    const parts = trimmed.split(/[,\t]+/);
    const genus = parts[0]?.trim();
    const rawValue = parts[1]?.trim();
    if (!genus || rawValue == null) continue;
    const value = parseFloat(rawValue);
    if (genus && !Number.isNaN(value)) {
      result[genus] = value;
    }
  }
  return result;
}

const formatP = (value: number) => {
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const PopHistogram = ({
  data,
  xLabel,
  yLabel,
}: {
  data: HistBin[];
  xLabel: string;
  yLabel: string;
}) => {
  const ref = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!ref.current || !data.length) return;
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();

    const margin = { top: 24, right: 20, bottom: 40, left: 48 };
    const width = 560;
    const height = 220;
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;

    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const group = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const maxValue = d3.max(data, (item) => Math.max(item.nc_count, item.disease_count)) ?? 1;
    const x = d3.scaleBand().domain(data.map((item) => String(item.bin_start))).range([0, innerWidth]).padding(0.15);
    const y = d3.scaleLinear().domain([0, maxValue * 1.1]).range([innerHeight, 0]);

    group.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(d3.axisBottom(x).tickValues(data.filter((_, index) => index % 2 === 0).map((item) => String(item.bin_start))))
      .selectAll("text")
      .attr("fill", "#999")
      .style("font-size", "10px");

    group.append("g")
      .call(d3.axisLeft(y).ticks(5))
      .selectAll("text")
      .attr("fill", "#999")
      .style("font-size", "10px");

    group.append("text")
      .attr("x", innerWidth / 2)
      .attr("y", innerHeight + 34)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .style("font-size", "11px")
      .text(xLabel);

    group.append("text")
      .attr("x", -innerHeight / 2)
      .attr("y", -34)
      .attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .style("font-size", "11px")
      .text(yLabel);

    const bandHalf = x.bandwidth() / 2;

    group.selectAll(".nc-bar")
      .data(data)
      .join("rect")
      .attr("x", (item) => x(String(item.bin_start))!)
      .attr("y", (item) => y(item.nc_count))
      .attr("width", bandHalf)
      .attr("height", (item) => innerHeight - y(item.nc_count))
      .attr("fill", "#44cc88")
      .attr("opacity", 0.8);

    group.selectAll(".disease-bar")
      .data(data)
      .join("rect")
      .attr("x", (item) => x(String(item.bin_start))! + bandHalf)
      .attr("y", (item) => y(item.disease_count))
      .attr("width", bandHalf)
      .attr("height", (item) => innerHeight - y(item.disease_count))
      .attr("fill", "#ff6666")
      .attr("opacity", 0.8);

    const legend = svg.append("g").attr("transform", `translate(${width - 180}, 8)`);
    legend.append("rect").attr("width", 12).attr("height", 12).attr("fill", "#44cc88").attr("rx", 2);
    legend.append("text").attr("x", 16).attr("y", 10).attr("fill", "#ccc").style("font-size", "11px").text("NC");
    legend.append("rect").attr("x", 50).attr("width", 12).attr("height", 12).attr("fill", "#ff6666").attr("rx", 2);
    legend.append("text").attr("x", 66).attr("y", 10).attr("fill", "#ccc").style("font-size", "11px").text("Disease");
  }, [data, xLabel, yLabel]);

  return <svg ref={ref} style={{ width: "100%", maxWidth: 560, height: "auto" }} role="img" aria-label="Population GMHI distribution histogram" />;
};

const HealthIndexPanel = () => {
  const { t, locale } = useI18n();
  const [refData, setRefData] = useState<ReferenceData | null>(null);
  const [refLoading, setRefLoading] = useState(true);
  const [referenceError, setReferenceError] = useState("");
  const [pasteText, setPasteText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<HealthResult | null>(null);
  const [gbhi, setGbhi] = useState<GbhiResult | null>(null);
  const [sortMode, setSortMode] = useState<"diff" | "user" | "nc">("diff");

  const fileInputRef = useRef<HTMLInputElement>(null);
  const gaugeRef = useRef<SVGSVGElement>(null);
  const summaryExportRef = useRef<HTMLDivElement>(null);
  const contributionExportRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cancelled = false;

    cachedFetch<ReferenceData>(`${API_BASE}/api/health-index/reference`)
      .then((data) => {
        if (cancelled) return;
        setRefData(data);
        setReferenceError("");
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setRefData(null);
        setReferenceError(err instanceof Error ? err.message : "");
      })
      .finally(() => {
        if (!cancelled) setRefLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const readFile = (candidate: File) =>
    new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = () => reject(new Error("Failed to read file"));
      reader.readAsText(candidate);
    });

  const calculate = async () => {
    setError("");
    setResult(null);
    setGbhi(null);

    let abundances: Record<string, number> = {};
    try {
      if (file) {
        const text = await readFile(file);
        abundances = parseAbundanceText(text);
      } else if (pasteText.trim()) {
        abundances = parseAbundanceText(pasteText);
      }
    } catch {
      setError(locale === "zh" ? "输入解析失败" : "Failed to parse input");
      return;
    }

    if (Object.keys(abundances).length === 0) {
      setError(locale === "zh" ? "未识别到有效的属名-丰度对" : "No valid genus-abundance pairs found");
      return;
    }

    setLoading(true);
    try {
      const resp = await fetch(`${API_BASE}/api/health-index`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ abundances }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${resp.status}`);
      }
      const payload: HealthResult = await resp.json();
      setResult(payload);

      // Also call the universal multinomial softmax GBHI endpoint
      try {
        const gResp = await fetch(`${API_BASE}/api/health_score`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ abundances }),
        });
        if (gResp.ok) {
          const gPayload: GbhiResult = await gResp.json();
          setGbhi(gPayload);
        }
      } catch {
        // non-fatal: GBHI endpoint optional
      }
    } catch (err: unknown) {
      const detail = err instanceof Error ? err.message : "";
      setError(detail ? `${t("healthIndex.requestFailed")}: ${detail}` : t("healthIndex.calculateFailed"));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!result || !gaugeRef.current) return;
    // Prefer the multinomial softmax GBHI score when available
    const displayScore = gbhi ? gbhi.health_score : result.score;
    const svg = d3.select(gaugeRef.current);
    svg.selectAll("*").remove();

    const width = 300;
    const height = 190;
    const centerX = width / 2;
    const centerY = 152;
    const radius = 112;
    const startAngle = -Math.PI * 0.75;
    const endAngle = Math.PI * 0.75;

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const segmentDefs = [
      { from: 0, to: 0.33, color: "#ff5454" },
      { from: 0.33, to: 0.66, color: "#ffb02e" },
      { from: 0.66, to: 1, color: "#44cc88" },
    ];

    segmentDefs.forEach((segment) => {
      const arc = d3.arc<unknown>()
        .innerRadius(radius - 16)
        .outerRadius(radius)
        .startAngle(startAngle + segment.from * (endAngle - startAngle))
        .endAngle(startAngle + segment.to * (endAngle - startAngle));

      svg.append("path")
        .attr("d", arc({} as never) ?? "")
        .attr("transform", `translate(${centerX},${centerY})`)
        .attr("fill", segment.color)
        .attr("opacity", 0.3);
    });

    const trackArc = d3.arc<unknown>()
      .innerRadius(radius - 4)
      .outerRadius(radius - 1)
      .startAngle(startAngle)
      .endAngle(endAngle);

    svg.append("path")
      .attr("d", trackArc({} as never) ?? "")
      .attr("transform", `translate(${centerX},${centerY})`)
      .attr("fill", "rgba(255,255,255,0.08)");

    const needle = svg.append("line")
      .attr("stroke", "#ffffff")
      .attr("stroke-width", 2.5)
      .attr("x1", centerX)
      .attr("y1", centerY);

    svg.append("circle")
      .attr("cx", centerX)
      .attr("cy", centerY)
      .attr("r", 6)
      .attr("fill", "#ffffff");

    const valueText = svg.append("text")
      .attr("x", centerX)
      .attr("y", centerY - 34)
      .attr("text-anchor", "middle")
      .attr("fill", "#ffffff")
      .attr("font-size", 34)
      .attr("font-weight", 700)
      .text("0");

    const categoryKey = result.category === "good" || result.category === "moderate" || result.category === "attention"
      ? result.category
      : "attention";

    const categoryText = svg.append("text")
      .attr("x", centerX)
      .attr("y", centerY - 8)
      .attr("text-anchor", "middle")
      .attr("fill", result.category === "good" ? "#44cc88" : result.category === "moderate" ? "#ffb02e" : "#ff6666")
      .attr("font-size", 13)
      .text(t(`healthIndex.category.${categoryKey}` as const));

    svg.append("text")
      .attr("x", 36)
      .attr("y", centerY + 4)
      .attr("fill", "#888")
      .attr("font-size", 11)
      .text("0");

    svg.append("text")
      .attr("x", centerX)
      .attr("y", 34)
      .attr("text-anchor", "middle")
      .attr("fill", "#888")
      .attr("font-size", 11)
      .text("50");

    svg.append("text")
      .attr("x", width - 42)
      .attr("y", centerY + 4)
      .attr("fill", "#888")
      .attr("font-size", 11)
      .text("100");

    const updateNeedle = (score: number) => {
      const clamped = Math.max(0, Math.min(100, score));
      const angle = startAngle + (clamped / 100) * (endAngle - startAngle) - Math.PI / 2;
      const length = radius - 24;
      const x2 = centerX + length * Math.cos(angle);
      const y2 = centerY + length * Math.sin(angle);
      needle.attr("x2", x2).attr("y2", y2);
    };

    updateNeedle(0);
    const transition = d3.transition().duration(900).ease(d3.easeCubicOut);
    svg.transition(transition).tween("gmhi", () => {
      const interpolate = d3.interpolateNumber(0, displayScore);
      return (time) => {
        const current = interpolate(time);
        valueText.text(current.toFixed(1));
        updateNeedle(current);
      };
    });

    categoryText.raise();
  }, [result, gbhi, t]);

  const handleDrop = (event: DragEvent) => {
    event.preventDefault();
    const dropped = event.dataTransfer.files?.[0] ?? null;
    if (dropped) setFile(dropped);
  };

  const exportDeviations = (rows: DeviationEntry[]) => {
    exportTable(
      rows.map((item) => ({
        Genus: item.genus,
        User_Abundance: item.user_abundance,
        NC_Mean: item.nc_mean,
        NC_Median: item.nc_median,
        NC_P10: item.nc_p10,
        NC_P25: item.nc_p25,
        NC_P75: item.nc_p75,
        NC_P90: item.nc_p90,
        Status: item.status,
      })),
      `health_index_deviation_${Date.now()}`,
    );
  };

  const sortedDeviation = result ? [...result.per_genus_deviation].sort((a, b) => {
    if (sortMode === "user") return b.user_abundance - a.user_abundance;
    if (sortMode === "nc") return b.nc_mean - a.nc_mean;
    return Math.abs(b.user_abundance - b.nc_mean) - Math.abs(a.user_abundance - a.nc_mean);
  }) : [];

  const hasInput = !!file || pasteText.trim().length > 0;
  const population = refData?.population;
  const exportSummaryLabel = locale === "zh" ? "下载 GMHI 结果图" : "Export GMHI summary";
  const exportContributionLabel = locale === "zh" ? "下载菌群贡献图" : "Export genus contribution chart";

  return (
    <div className={classes.panel}>
      <h2 className={classes.title}>{t("healthIndex.title")}</h2>
      <p className={classes.subtitle}>{t("healthIndex.subtitle")}</p>

      {refLoading && (
        <div className={classes.popLoading}>
          <div className="loading-spinner" style={{ width: 24, height: 24 }} />
          <span>{t("healthIndex.referenceLoading")}</span>
        </div>
      )}

      {!refLoading && !refData && (
        <div className={classes.error}>
          <strong>{t("healthIndex.referenceError")}</strong>
          <div>{t("healthIndex.referenceErrorDetail")}</div>
          {referenceError ? <div><code>{referenceError}</code></div> : null}
        </div>
      )}

      {!refLoading && refData && population && (
        <div className={classes.popSection}>
          <h3 className={classes.popTitle}>{t("healthIndex.popTitle")}</h3>
          <p className={classes.popSubtitle}>{t("healthIndex.popSubtitle")}</p>

          <div className={classes.statsGrid}>
            <div className={classes.statCard} data-accent="green">
              <div className={classes.statCardVal}>{refData.n_nc_samples.toLocaleString()}</div>
              <div className={classes.statCardLbl}>{t("healthIndex.ncSamples")}</div>
              <div className={classes.statCardSub}>
                {t("healthIndex.meanScore")}: <strong>{population.nc_stats.mean}</strong> · {t("healthIndex.medianScore")}: <strong>{population.nc_stats.median}</strong>
              </div>
              <div className={classes.statCardSub}>
                P10–P90: {population.nc_stats.p10} — {population.nc_stats.p90}
              </div>
            </div>

            <div className={classes.statCard} data-accent="red">
              <div className={classes.statCardVal}>{refData.n_disease_samples.toLocaleString()}</div>
              <div className={classes.statCardLbl}>{t("healthIndex.diseaseSamples")}</div>
              <div className={classes.statCardSub}>
                {t("healthIndex.meanScore")}: <strong>{population.disease_stats.mean}</strong> · {t("healthIndex.medianScore")}: <strong>{population.disease_stats.median}</strong>
              </div>
              <div className={classes.statCardSub}>
                P10–P90: {population.disease_stats.p10} — {population.disease_stats.p90}
              </div>
            </div>
          </div>

          <PopHistogram data={population.histogram} xLabel={t("healthIndex.score")} yLabel={t("col.samples")} />

          <div className={classes.generaRow}>
            <div className={classes.generaCol}>
              <h4 className={classes.generaTitle} style={{ color: "#44cc88" }}>{t("healthIndex.topHealthGenera")}</h4>
              <div className={classes.tableWrap} style={{ maxHeight: 320 }}>
                <table className={classes.table}>
                  <thead>
                    <tr>
                      <th>{t("healthIndex.col.genus")}</th>
                      <th>{t("healthIndex.log2fc")}</th>
                      <th>{t("healthIndex.col.adjustedP")}</th>
                      <th>{t("healthIndex.col.weight")}</th>
                      <th>{t("healthIndex.meanNC")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {refData.health_genera.map((item) => (
                      <tr key={`health:${item.genus}`}>
                        <td><em>{item.genus}</em></td>
                        <td style={{ color: "#44cc88" }}>{item.log2fc.toFixed(2)}</td>
                        <td>{formatP(item.adjusted_p)}</td>
                        <td>{item.weight.toFixed(2)}</td>
                        <td>{item.mean_nc.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className={classes.generaCol}>
              <h4 className={classes.generaTitle} style={{ color: "#ff7a7a" }}>{t("healthIndex.topDiseaseGenera")}</h4>
              <div className={classes.tableWrap} style={{ maxHeight: 320 }}>
                <table className={classes.table}>
                  <thead>
                    <tr>
                      <th>{t("healthIndex.col.genus")}</th>
                      <th>{t("healthIndex.log2fc")}</th>
                      <th>{t("healthIndex.col.adjustedP")}</th>
                      <th>{t("healthIndex.col.weight")}</th>
                      <th>{t("healthIndex.meanDis")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {refData.disease_genera.map((item) => (
                      <tr key={`disease:${item.genus}`}>
                        <td><em>{item.genus}</em></td>
                        <td style={{ color: "#ff7a7a" }}>{item.log2fc.toFixed(2)}</td>
                        <td>{formatP(item.adjusted_p)}</td>
                        <td>{item.weight.toFixed(2)}</td>
                        <td>{item.mean_disease.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className={classes.uploadSection}>
        <h3 className={classes.uploadTitle}>{t("healthIndex.tryYourOwn")}</h3>
      </div>

      <div className={classes.inputRow}>
        <div
          className={`${classes.dropZone} ${file ? classes.dropZoneActive : ""}`}
          onClick={() => fileInputRef.current?.click()}
          onDrop={handleDrop}
          onDragOver={(event) => event.preventDefault()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,.tsv,.txt"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
            style={{ display: "none" }}
          />
          <p>{file ? file.name : "CSV / TSV"}</p>
          <p style={{ fontSize: "0.75rem", color: "#888" }}>genus_name, abundance</p>
        </div>

        <textarea
          className={classes.textarea}
          placeholder={"Bacteroides,0.25\nFaecalibacterium,0.18\nPrevotella,0.12"}
          value={pasteText}
          onChange={(event) => setPasteText(event.target.value)}
        />

        <button className={classes.calcBtn} onClick={calculate} disabled={!hasInput || loading}>
          {loading ? t("healthIndex.calculating") : t("healthIndex.calculate")}
        </button>
      </div>

      {error && <div className={classes.error}>{error}</div>}

      {result && (
        <div className={classes.results}>
          <div className={classes.resultToolbar}>
            <button
              className={classes.exportBtn}
              type="button"
              onClick={() => summaryExportRef.current && exportElementPNG(summaryExportRef.current, `gmhi_summary_${Date.now()}`)}
            >
              {exportSummaryLabel}
            </button>
            <button
              className={classes.exportBtn}
              type="button"
              onClick={() => contributionExportRef.current && exportElementPNG(contributionExportRef.current, `gmhi_contribution_${Date.now()}`)}
            >
              {exportContributionLabel}
            </button>
          </div>

          <div ref={summaryExportRef} className={classes.exportCard}>
            {/* Tier pill */}
            {gbhi && (
              <div style={{ marginBottom: 8 }}>
                <span
                  style={{
                    display: "inline-block",
                    padding: "3px 10px",
                    borderRadius: 10,
                    fontSize: 12,
                    fontWeight: 600,
                    color:
                      gbhi.tier === "high"
                        ? "#44cc88"
                        : gbhi.tier === "moderate"
                          ? "#ffb02e"
                          : "#ff6666",
                    background:
                      gbhi.tier === "high"
                        ? "rgba(68, 204, 136, 0.15)"
                        : gbhi.tier === "moderate"
                          ? "rgba(255, 176, 46, 0.15)"
                          : "rgba(255, 102, 102, 0.15)",
                    border: `1px solid ${
                      gbhi.tier === "high"
                        ? "rgba(68, 204, 136, 0.5)"
                        : gbhi.tier === "moderate"
                          ? "rgba(255, 176, 46, 0.5)"
                          : "rgba(255, 102, 102, 0.5)"
                    }`,
                  }}
                >
                  Tier: {gbhi.tier.toUpperCase()}
                </span>
              </div>
            )}

            <div className={classes.percentileBanner}>
              <span className={classes.percentileValue}>{result.population_percentile.toFixed(1)}%</span>
              <span className={classes.percentileLabel}>{t("healthIndex.populationPct")}</span>
            </div>

            <div className={classes.gaugeRow}>
              <svg ref={gaugeRef} className={classes.gauge} />

              <div className={classes.resultMetrics}>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.score.toFixed(1)}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.score")}</span>
                </div>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.raw_score.toFixed(3)}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.rawScore")}</span>
                </div>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.raw_score_weighted.toFixed(3)}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.weightedScore")}</span>
                </div>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.health_genera_matched}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.healthGenera")}</span>
                </div>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.disease_genera_matched}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.diseaseGenera")}</span>
                </div>
                <div className={classes.metricCard}>
                  <span className={classes.metricValue}>{result.reference.n_nc_samples.toLocaleString()}</span>
                  <span className={classes.metricLabel}>{t("healthIndex.ncSamples")}</span>
                </div>
              </div>
            </div>
          </div>

          {gbhi && (
            <div
              style={{
                marginTop: 16,
                padding: 16,
                background: "rgba(255,255,255,0.03)",
                border: "1px solid rgba(255,255,255,0.08)",
                borderRadius: 8,
              }}
            >
              <h3 style={{ margin: "0 0 4px", fontSize: 14, color: "#eee" }}>
                10-Class Softmax Probabilities
              </h3>
              <p style={{ margin: "0 0 12px", fontSize: 11, color: "#888" }}>
                Order: NC + 9 diseases
              </p>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {[{ key: "p_nc" as const, label: "NC (健康对照)" }, ...GBHI_DISEASE_CLASSES].map(
                  (cls) => {
                    const value = gbhi[cls.key] as number;
                    const pct = Math.max(0, Math.min(1, value)) * 100;
                    const isNc = cls.key === "p_nc";
                    return (
                      <div
                        key={cls.key}
                        style={{ display: "flex", alignItems: "center", gap: 10 }}
                      >
                        <span style={{ width: 110, fontSize: 11, color: "#bbb" }}>
                          {cls.label}
                        </span>
                        <div
                          style={{
                            flex: 1,
                            height: 12,
                            background: "rgba(255,255,255,0.06)",
                            borderRadius: 6,
                            overflow: "hidden",
                          }}
                        >
                          <div
                            style={{
                              width: `${pct}%`,
                              height: "100%",
                              background: isNc ? "#44cc88" : "#ff7a7a",
                              transition: "width 0.6s ease-out",
                            }}
                          />
                        </div>
                        <span
                          style={{
                            width: 60,
                            fontSize: 11,
                            color: "#ddd",
                            textAlign: "right",
                            fontVariantNumeric: "tabular-nums",
                          }}
                        >
                          {(value * 100).toFixed(2)}%
                        </span>
                      </div>
                    );
                  },
                )}
              </div>
            </div>
          )}

          <div ref={contributionExportRef}>
            <ContributionChart
              title={t("healthIndex.contribution")}
              positiveLabel={t("healthIndex.topHealthGenera")}
              negativeLabel={t("healthIndex.topDiseaseGenera")}
              health={result.health_genera_detail}
              disease={result.disease_genera_detail}
            />
          </div>

          <div className={classes.deviationSection}>
            <div className={classes.tableToolbar}>
              <h3>{t("healthIndex.deviation")}</h3>
              <div className={classes.toolbarActions}>
                <label className={classes.sortField}>
                  <span>{t("healthIndex.sortBy")}</span>
                  <select value={sortMode} onChange={(event) => setSortMode(event.target.value as "diff" | "user" | "nc")}>
                    <option value="diff">{t("healthIndex.sortDiff")}</option>
                    <option value="user">{t("healthIndex.sortUser")}</option>
                    <option value="nc">{t("healthIndex.sortNC")}</option>
                  </select>
                </label>
                <button className={classes.exportBtn} onClick={() => exportDeviations(sortedDeviation)}>{t("export.csv")}</button>
              </div>
            </div>

            <div className={classes.tableWrap}>
              <table className={classes.table}>
                <thead>
                  <tr>
                    <th>{t("healthIndex.col.genus")}</th>
                    <th>{t("healthIndex.col.yours")}</th>
                    <th>{t("healthIndex.col.ncMean")}</th>
                    <th>{t("healthIndex.col.ncMedian")}</th>
                    <th>P10</th>
                    <th>P90</th>
                    <th>{t("healthIndex.col.status")}</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedDeviation.map((item) => (
                    <tr key={item.genus}>
                      <td>{item.genus}</td>
                      <td>{item.user_abundance.toFixed(4)}</td>
                      <td>{item.nc_mean.toFixed(4)}</td>
                      <td>{item.nc_median.toFixed(4)}</td>
                      <td>{item.nc_p10.toFixed(4)}</td>
                      <td>{item.nc_p90.toFixed(4)}</td>
                      <td>
                        <span className={classes.statusBadge} data-status={item.status}>
                          {item.status === "high" ? "↑" : item.status === "low" ? "↓" : "—"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default HealthIndexPanel;
