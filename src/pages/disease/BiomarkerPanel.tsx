/**
 * BiomarkerPanel.tsx — 标志物发现面板（嵌入 DiseasePage Tab）
 * Wilcoxon + BH FDR + LDA + 森林图
 */
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import classes from "../DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface Marker {
  taxon: string;
  mean_disease: number;
  mean_control: number;
  log2fc: number;
  lda_score: number;
  p_value: number;
  adjusted_p: number;
  prevalence_disease: number;
  prevalence_control: number;
  enriched_in: string;
  ci_low: number;
  ci_high: number;
}

interface BiomarkerResult {
  disease: string;
  n_disease: number;
  n_control: number;
  n_markers: number;
  markers: Marker[];
}

interface Props {
  disease: string;
}

const BiomarkerPanel = ({ disease }: Props) => {
  const { t, locale } = useI18n();
  const [ldaThreshold, setLdaThreshold] = useState(2.0);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<BiomarkerResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const forestRef = useRef<SVGSVGElement>(null);
  const ldaRef = useRef<SVGSVGElement>(null);

  // 切换疾病时重置结果
  useEffect(() => {
    setResult(null);
    setError(null);
  }, [disease]);

  const runAnalysis = () => {
    if (!disease) return;
    setLoading(true);
    setResult(null);
    setError(null);
    fetch(`${API_BASE}/api/biomarker-discovery?disease=${encodeURIComponent(disease)}&lda_threshold=${ldaThreshold}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: BiomarkerResult) => setResult(data))
      .catch((err) => {
        console.error("Biomarker API error:", err);
        setError(locale === "zh" ? "后端未启动或连接失败" : "Backend not available or connection failed");
      })
      .finally(() => setLoading(false));
  };

  // 绘制森林图
  useEffect(() => {
    if (!forestRef.current || !result || result.markers.length === 0) return;
    drawForestPlot(forestRef.current, result.markers.slice(0, 30));
  }, [result]);

  // 绘制 LDA 柱状图
  useEffect(() => {
    if (!ldaRef.current || !result || result.markers.length === 0) return;
    drawLDAChart(ldaRef.current, result.markers.slice(0, 30));
  }, [result]);

  const exportBiomarkerCsv = () => {
    if (!result) return;
    exportTable(
      result.markers.map((m) => ({
        Taxon: m.taxon,
        Log2FC: m.log2fc,
        LDA_Score: m.lda_score,
        P_value: m.p_value,
        Adjusted_P: m.adjusted_p,
        Enriched_In: m.enriched_in,
        Prevalence_Disease: m.prevalence_disease,
        Prevalence_Control: m.prevalence_control,
      })),
      `biomarker_${disease}_${Date.now()}`,
    );
  };

  const exportBiomarkerChart = (ref: React.RefObject<SVGSVGElement | null>, name: string, type: "svg" | "png") => {
    const svg = ref.current;
    if (!svg) return;
    type === "svg"
      ? exportSVG(svg, `${name}_${disease}_${Date.now()}`)
      : exportPNG(svg, `${name}_${disease}_${Date.now()}`);
  };

  return (
    <div>
      {/* 控制栏：LDA 阈值 + 运行按钮 */}
      <div className={classes.biomarkerControls}>
        <div className={classes.field}>
          <label>{t("biomarker.ldaThreshold")}</label>
          <select className={classes.inlineSelect} value={ldaThreshold} onChange={e => setLdaThreshold(Number(e.target.value))}>
            <option value={1.5}>1.5</option>
            <option value={2.0}>2.0</option>
            <option value={2.5}>2.5</option>
            <option value={3.0}>3.0</option>
          </select>
        </div>
        <button className={classes.runBtn} onClick={runAnalysis} disabled={loading}>
          {loading ? t("biomarker.running") : t("biomarker.runAnalysis")}
        </button>
      </div>

      {/* 加载状态 */}
      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}

      {/* 错误信息 */}
      {error && <div className={classes.errorMsg}>{error}</div>}

      {/* 结果展示 */}
      {result && (
        <>
          {/* 统计卡片 */}
          <div className={classes.profileHeader}>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_markers}</span>
              <span className={classes.statLabel}>{t("biomarker.markerCount")}</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_disease.toLocaleString()}</span>
              <span className={classes.statLabel}>{t("biomarker.diseaseSamples")}</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_control.toLocaleString()}</span>
              <span className={classes.statLabel}>{t("biomarker.controlSamples")}</span>
            </div>
          </div>

          {result.markers.length === 0 ? (
            <div className={classes.selectHint}>{t("biomarker.noResults")}</div>
          ) : (
            <>
              {/* 导出按钮 */}
              <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.5rem", padding: "0 1rem" }}>
                <button onClick={exportBiomarkerCsv} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
                <button onClick={() => exportBiomarkerChart(ldaRef, "lda", "svg")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
                <button onClick={() => exportBiomarkerChart(ldaRef, "lda", "png")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
              </div>

              {/* 森林图 */}
              <div className={classes.chartCard}>
                <h3>{t("biomarker.forestPlot")}</h3>
                <svg ref={forestRef} className={classes.chart} />
              </div>

              {/* LDA 柱状图 */}
              <div className={classes.chartCard}>
                <h3>{t("biomarker.ldaPlot")}</h3>
                <svg ref={ldaRef} className={classes.chart} />
              </div>

              {/* 标志物表格 */}
              <div className={classes.chartCard}>
                <h3>{t("biomarker.table")}</h3>
                <table className={classes.generaTable}>
                  <thead>
                    <tr>
                      <th>{t("biomarker.taxon")}</th>
                      <th>{t("biomarker.log2fc")}</th>
                      <th>{t("biomarker.lda")}</th>
                      <th>{t("biomarker.pValue")}</th>
                      <th>{t("biomarker.enrichedIn")}</th>
                      <th>{t("biomarker.prevalence")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.markers.slice(0, 50).map(m => (
                      <tr key={m.taxon}>
                        <td><i>{m.taxon}</i></td>
                        <td className={m.log2fc > 0 ? classes.enriched : classes.depleted}>
                          {m.log2fc > 0 ? "+" : ""}{m.log2fc.toFixed(2)}
                        </td>
                        <td>{m.lda_score.toFixed(2)}</td>
                        <td>{m.adjusted_p < 0.001 ? m.adjusted_p.toExponential(2) : m.adjusted_p.toFixed(4)}</td>
                        <td>{locale === "zh" ? (m.enriched_in === "disease" ? "疾病组" : "对照组") : m.enriched_in}</td>
                        <td>{(m.prevalence_disease * 100).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
};

export default BiomarkerPanel;

// ── Forest Plot / 森林图 ──────────────────────────────────────────────────────

function drawForestPlot(svgEl: SVGSVGElement, markers: Marker[]) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const margin = { top: 20, right: 40, bottom: 30, left: 140 };
  const W = 700, H = Math.max(300, markers.length * 22 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const diffs = markers.map(m => m.mean_disease - m.mean_control);
  const cis = markers.map(m => [m.ci_low, m.ci_high]);
  const allVals = [...diffs, ...cis.flat()];
  const xMin = Math.min(0, d3.min(allVals) ?? 0) * 1.1;
  const xMax = Math.max(0, d3.max(allVals) ?? 0) * 1.1;
  const xScale = d3.scaleLinear().domain([xMin, xMax]).range([0, iW]);

  const yScale = d3.scaleBand()
    .domain(markers.map(m => m.taxon))
    .range([0, iH])
    .padding(0.3);

  // 零线
  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)")
    .attr("stroke-dasharray", "4,4");

  // 置信区间线
  g.selectAll(".ci-line")
    .data(markers)
    .join("line")
    .attr("x1", (_, i) => xScale(cis[i][0]))
    .attr("x2", (_, i) => xScale(cis[i][1]))
    .attr("y1", m => (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("y2", m => (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("stroke", m => m.enriched_in === "disease" ? "#ff6b6b" : "#4ecdc4")
    .attr("stroke-width", 1.5);

  // 效应值菱形点
  g.selectAll(".point")
    .data(markers)
    .join("rect")
    .attr("x", (_, i) => xScale(diffs[i]) - 4)
    .attr("y", m => (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2 - 4)
    .attr("width", 8).attr("height", 8)
    .attr("transform", m => {
      const cx = xScale(m.mean_disease - m.mean_control);
      const cy = (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2;
      return `rotate(45, ${cx}, ${cy})`;
    })
    .attr("fill", m => m.enriched_in === "disease" ? "#ff6b6b" : "#4ecdc4");

  // Y轴
  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat(d => d.length > 18 ? d.slice(0, 16) + "\u2026" : d))
    .attr("font-size", 9)
    .selectAll("text").attr("font-style", "italic");

  // X轴
  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5))
    .attr("font-size", 9);

  g.append("text")
    .attr("x", iW / 2).attr("y", iH + 25)
    .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 10)
    .text("Mean Difference (Disease \u2212 Control)");
}

// ── LDA Effect Size Bar Chart / LDA 效应值柱状图 ──────────────────────────────

function drawLDAChart(svgEl: SVGSVGElement, markers: Marker[]) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const sorted = [...markers].sort((a, b) => {
    const scoreA = a.enriched_in === "disease" ? a.lda_score : -a.lda_score;
    const scoreB = b.enriched_in === "disease" ? b.lda_score : -b.lda_score;
    return scoreB - scoreA;
  });

  const margin = { top: 10, right: 20, bottom: 30, left: 130 };
  const W = 700, H = Math.max(300, sorted.length * 22 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const maxLDA = d3.max(sorted, m => m.lda_score) ?? 5;
  const xScale = d3.scaleLinear().domain([-maxLDA, maxLDA]).range([0, iW]);

  const yScale = d3.scaleBand()
    .domain(sorted.map(m => m.taxon))
    .range([0, iH])
    .padding(0.2);

  // 零线
  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)");

  // LDA 柱
  g.selectAll("rect")
    .data(sorted)
    .join("rect")
    .attr("x", m => m.enriched_in === "disease" ? xScale(0) : xScale(-m.lda_score))
    .attr("y", m => yScale(m.taxon) ?? 0)
    .attr("width", m => Math.abs(xScale(m.lda_score) - xScale(0)))
    .attr("height", yScale.bandwidth())
    .attr("fill", m => m.enriched_in === "disease" ? "#ff6b6b" : "#4ecdc4")
    .attr("opacity", 0.8)
    .attr("rx", 2);

  // Y轴
  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat(d => d.length > 16 ? d.slice(0, 14) + "\u2026" : d))
    .attr("font-size", 9)
    .selectAll("text").attr("font-style", "italic");

  // X轴
  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5))
    .attr("font-size", 9);

  // 图例
  const legend = svg.append("g").attr("transform", `translate(${margin.left + 10}, ${H - 8})`);
  legend.append("rect").attr("width", 12).attr("height", 8).attr("fill", "#ff6b6b").attr("opacity", 0.8);
  legend.append("text").attr("x", 16).attr("y", 7).text("Enriched in Disease").attr("fill", "currentColor").attr("font-size", 10);
  legend.append("rect").attr("x", 160).attr("width", 12).attr("height", 8).attr("fill", "#4ecdc4").attr("opacity", 0.8);
  legend.append("text").attr("x", 176).attr("y", 7).text("Enriched in Control").attr("fill", "currentColor").attr("font-size", 10);
}
