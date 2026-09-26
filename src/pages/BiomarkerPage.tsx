/**
 * BiomarkerPage.tsx — Disease Biomarker Discovery
 * Wilcoxon + BH-FDR + LDA + forest plot
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { API_BASE } from "@/util/apiBase";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";
import classes from "./BiomarkerPage.module.css";

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

interface DiseaseItem { name: string; sample_count: number; }

const BiomarkerPage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [selected, setSelected] = useState("");
  const [ldaThreshold, setLdaThreshold] = useState(2.0);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<BiomarkerResult | null>(null);
  const forestRef = useRef<SVGSVGElement>(null);
  const ldaRef = useRef<SVGSVGElement>(null);

  const dName = (n: string) => (locale === "zh" && diseaseZh[n]) ? diseaseZh[n] : diseaseDisplayNameI18n(n, locale);

  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`).then(r => r.json())
      .then(d => setDiseases(d.diseases ?? [])).catch(() => {});
    fetch(`${API_BASE}/api/disease-names-zh`).then(r => r.json())
      .then(setDiseaseZh).catch(() => {});
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  const runAnalysis = () => {
    if (!selected) return;
    setLoading(true);
    setResult(null);
    fetch(`${API_BASE}/api/biomarker-discovery?disease=${encodeURIComponent(selected)}&lda_threshold=${ldaThreshold}`)
      .then(r => { if (!r.ok) throw new Error(); return r.json(); })
      .then((data: BiomarkerResult) => setResult(data))
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (!forestRef.current || !result || result.markers.length === 0) return;
    drawForestPlot(forestRef.current, result.markers.slice(0, 30));
  }, [result]);

  useEffect(() => {
    if (!ldaRef.current || !result || result.markers.length === 0) return;
    drawLDAChart(ldaRef.current, result.markers.slice(0, 30));
  }, [result]);

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("biomarker.back")}</Link>
        <h1>{t("biomarker.title")}</h1>
        <p>{t("biomarker.subtitle")}</p>
      </div>

      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("biomarker.selectDisease")}</label>
          <select className={classes.select} value={selected} onChange={e => setSelected(e.target.value)}>
            <option value="">--</option>
            {sortedDiseases.slice(0, 200).map(d => (
              <option key={d.name} value={d.name}>{dName(d.name)} ({d.sample_count})</option>
            ))}
          </select>
        </div>
        <div className={classes.field}>
          <label>{t("biomarker.ldaThreshold")}</label>
          <select className={classes.select} value={ldaThreshold} onChange={e => setLdaThreshold(Number(e.target.value))}>
            <option value={1.5}>1.5</option>
            <option value={2.0}>2.0</option>
            <option value={2.5}>2.5</option>
            <option value={3.0}>3.0</option>
          </select>
        </div>
        <button className={classes.runBtn} onClick={runAnalysis} disabled={!selected || loading}>
          {loading ? t("biomarker.running") : t("biomarker.runAnalysis")}
        </button>
      </div>

      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}

      {result && (
        <>
          <div className={classes.statsRow}>
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
            <div className={classes.noResults}>{t("biomarker.noResults")}</div>
          ) : (
            <>
              <div className={classes.chartCard}>
                <h3>{t("biomarker.forestPlot")}</h3>
                <svg ref={forestRef} className={classes.chart} />
              </div>

              <div className={classes.chartCard}>
                <h3>{t("biomarker.ldaPlot")}</h3>
                <svg ref={ldaRef} className={classes.chart} />
              </div>

              <div className={classes.chartCard}>
                <h3>{t("biomarker.table")}</h3>
                <table className={classes.table}>
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

export default BiomarkerPage;

// ── Forest Plot ──────────────────────────────────────────────────────────────

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

  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)")
    .attr("stroke-dasharray", "4,4");

  g.selectAll(".ci-line")
    .data(markers)
    .join("line")
    .attr("x1", (_, i) => xScale(cis[i][0]))
    .attr("x2", (_, i) => xScale(cis[i][1]))
    .attr("y1", m => (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("y2", m => (yScale(m.taxon) ?? 0) + yScale.bandwidth() / 2)
    .attr("stroke", m => m.enriched_in === "disease" ? "#ff6b6b" : "#4ecdc4")
    .attr("stroke-width", 1.5);

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

  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat(d => d.length > 18 ? d.slice(0, 16) + "…" : d))
    .attr("font-size", 9)
    .selectAll("text").attr("font-style", "italic");

  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5))
    .attr("font-size", 9);

  g.append("text")
    .attr("x", iW / 2).attr("y", iH + 25)
    .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 10)
    .text("Mean Difference (Disease − Control)");
}

// ── LDA Effect Size Bar Chart ─────────────────────────────────────────────────

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

  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)");

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

  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat(d => d.length > 16 ? d.slice(0, 14) + "…" : d))
    .attr("font-size", 9)
    .selectAll("text").attr("font-style", "italic");

  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5))
    .attr("font-size", 9);

  const legend = svg.append("g").attr("transform", `translate(${margin.left + 10}, ${H - 8})`);
  legend.append("rect").attr("width", 12).attr("height", 8).attr("fill", "#ff6b6b").attr("opacity", 0.8);
  legend.append("text").attr("x", 16).attr("y", 7).text("Enriched in Disease").attr("fill", "currentColor").attr("font-size", 10);
  legend.append("rect").attr("x", 160).attr("width", 12).attr("height", 8).attr("fill", "#4ecdc4").attr("opacity", 0.8);
  legend.append("text").attr("x", 176).attr("y", 7).text("Enriched in Control").attr("fill", "currentColor").attr("font-size", 10);
}
