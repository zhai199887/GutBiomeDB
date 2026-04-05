import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportTable } from "@/util/export";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { phylumColor } from "@/util/phylumColors";
import type { BiomarkerResult, Marker } from "./types";
import classes from "../DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface Props {
  disease: string;
}

const formatP = (value: number) => {
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const BiomarkerPanel = ({ disease }: Props) => {
  const { t, locale } = useI18n();
  const forestRef = useRef<SVGSVGElement>(null);
  const ldaRef = useRef<SVGSVGElement>(null);

  const [ldaThreshold, setLdaThreshold] = useState(2.0);
  const [enrichedFilter, setEnrichedFilter] = useState<"all" | "disease" | "control">("all");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<BiomarkerResult | null>(null);

  useEffect(() => {
    setResult(null);
    setError(null);
  }, [disease]);

  const filteredMarkers = useMemo(() => {
    if (!result) return [];
    return result.markers.filter((marker) => (
      enrichedFilter === "all" ? true : marker.enriched_in === enrichedFilter
    ));
  }, [enrichedFilter, result]);

  useEffect(() => {
    if (!forestRef.current) return;
    if (filteredMarkers.length === 0) {
      d3.select(forestRef.current).selectAll("*").remove();
      return;
    }
    drawForestPlot(forestRef.current, filteredMarkers.slice(0, 30), locale);
  }, [filteredMarkers, locale]);

  useEffect(() => {
    if (!ldaRef.current) return;
    if (filteredMarkers.length === 0) {
      d3.select(ldaRef.current).selectAll("*").remove();
      return;
    }
    drawLdaChart(ldaRef.current, filteredMarkers.slice(0, 30), locale);
  }, [filteredMarkers, locale]);

  const runAnalysis = () => {
    if (!disease) return;
    setLoading(true);
    setError(null);
    setResult(null);
    cachedFetch<BiomarkerResult>(
      `${API_BASE}/api/biomarker-discovery?disease=${encodeURIComponent(disease)}&lda_threshold=${ldaThreshold}`,
    )
      .then(setResult)
      .catch(() => {
        setError(locale === "zh" ? "后端未启动或连接失败" : "Backend not available or connection failed");
      })
      .finally(() => setLoading(false));
  };

  const exportCsv = () => {
    if (filteredMarkers.length === 0) return;
    exportTable(
      filteredMarkers.map((marker) => ({
        Taxon: marker.taxon,
        Phylum: marker.phylum,
        Log2FC: marker.log2fc,
        LDA: marker.lda_score,
        Adjusted_P: marker.adjusted_p,
        Enriched_In: marker.enriched_in,
        Disease_Prevalence: marker.prevalence_disease,
        Control_Prevalence: marker.prevalence_control,
      })),
      `biomarker_${disease}_${Date.now()}`,
    );
  };

  return (
    <div>
      <div className={classes.biomarkerControls}>
        <div className={classes.field}>
          <label>{t("biomarker.ldaThreshold")}</label>
          <select
            className={classes.inlineSelect}
            value={ldaThreshold}
            onChange={(event) => setLdaThreshold(Number(event.target.value))}
          >
            <option value={1.5}>1.5</option>
            <option value={2.0}>2.0</option>
            <option value={2.5}>2.5</option>
            <option value={3.0}>3.0</option>
          </select>
        </div>

        <div className={classes.field}>
          <label>{t("disease.biomarker.enrichFilter")}</label>
          <select
            className={classes.inlineSelect}
            value={enrichedFilter}
            onChange={(event) => setEnrichedFilter(event.target.value as "all" | "disease" | "control")}
          >
            <option value="all">{t("disease.biomarker.enrichAll")}</option>
            <option value="disease">{t("disease.biomarker.enrichDisease")}</option>
            <option value="control">{t("disease.biomarker.enrichControl")}</option>
          </select>
        </div>

        <button className={classes.runBtn} onClick={runAnalysis} disabled={loading}>
          {loading ? t("biomarker.running") : t("biomarker.runAnalysis")}
        </button>
      </div>

      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}
      {error && <div className={classes.errorMsg}>{error}</div>}

      {result && (
        <>
          <div className={classes.profileHeader}>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_markers}</span>
              <span className={classes.statLabel}>{t("biomarker.markerCount")}</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_disease.toLocaleString("en")}</span>
              <span className={classes.statLabel}>{t("biomarker.diseaseSamples")}</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.n_control.toLocaleString("en")}</span>
              <span className={classes.statLabel}>{t("biomarker.controlSamples")}</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{filteredMarkers.length}</span>
              <span className={classes.statLabel}>{t("disease.biomarker.enrichFilter")}</span>
            </div>
          </div>

          {filteredMarkers.length === 0 ? (
            <div className={classes.emptyPlot}>{t("biomarker.noResults")}</div>
          ) : (
            <>
              <div className={classes.chartCard}>
                <div className={classes.cardHeader}>
                  <h3>{t("biomarker.forestPlot")}</h3>
                  <div className={classes.exportActions}>
                    <button onClick={exportCsv}>{t("export.csv")}</button>
                    <button onClick={() => forestRef.current && exportSVG(forestRef.current, `forest_${disease}_${Date.now()}`)}>{t("export.svg")}</button>
                    <button onClick={() => forestRef.current && exportPNG(forestRef.current, `forest_${disease}_${Date.now()}`)}>{t("export.png")}</button>
                  </div>
                </div>
                <svg ref={forestRef} className={classes.chart} />
              </div>

              <div className={classes.chartCard}>
                <div className={classes.cardHeader}>
                  <h3>{t("biomarker.ldaPlot")}</h3>
                  <div className={classes.exportActions}>
                    <button onClick={() => ldaRef.current && exportSVG(ldaRef.current, `lda_${disease}_${Date.now()}`)}>{t("export.svg")}</button>
                    <button onClick={() => ldaRef.current && exportPNG(ldaRef.current, `lda_${disease}_${Date.now()}`)}>{t("export.png")}</button>
                  </div>
                </div>
                <svg ref={ldaRef} className={classes.chart} />
              </div>

              <div className={classes.chartCard}>
                <h3>{t("biomarker.table")}</h3>
                <table className={classes.generaTable}>
                  <thead>
                    <tr>
                      <th>{t("biomarker.taxon")}</th>
                      <th>{t("disease.genera.phylum")}</th>
                      <th>{t("biomarker.log2fc")}</th>
                      <th>{t("biomarker.lda")}</th>
                      <th>{t("biomarker.pValue")}</th>
                      <th>{t("biomarker.enrichedIn")}</th>
                      <th>{t("disease.genera.prevD")}</th>
                      <th>{t("disease.genera.prevC")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredMarkers.slice(0, 50).map((marker) => (
                      <tr key={marker.taxon}>
                        <td>
                          <Link to={`/species/${encodeURIComponent(marker.taxon)}`} className={classes.genusLink}>
                            {marker.taxon}
                          </Link>
                        </td>
                        <td>
                          <span className={classes.phylumBadge}>{marker.phylum}</span>
                        </td>
                        <td className={marker.log2fc > 0 ? classes.enriched : classes.depleted}>
                          {marker.log2fc > 0 ? "+" : ""}
                          {marker.log2fc.toFixed(3)}
                        </td>
                        <td>{marker.lda_score.toFixed(2)}</td>
                        <td>{formatP(marker.adjusted_p)}</td>
                        <td>{marker.enriched_in === "disease" ? t("disease.biomarker.enrichDisease") : t("disease.biomarker.enrichControl")}</td>
                        <td>{(marker.prevalence_disease * 100).toFixed(1)}%</td>
                        <td>{(marker.prevalence_control * 100).toFixed(1)}%</td>
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

function tooltipForMarker(marker: Marker, locale: string) {
  return renderToString(
    <div className="tooltip-table">
      <span>{locale === "zh" ? "菌属" : "Genus"}</span><span><i>{marker.taxon}</i></span>
      <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{marker.phylum}</span>
      <span>log2FC</span><span>{marker.log2fc.toFixed(3)}</span>
      <span>LDA</span><span>{marker.lda_score.toFixed(2)}</span>
      <span>adj.p</span><span>{formatP(marker.adjusted_p)}</span>
      <span>{locale === "zh" ? "疾病流行率" : "Disease prevalence"}</span><span>{(marker.prevalence_disease * 100).toFixed(1)}%</span>
      <span>{locale === "zh" ? "对照流行率" : "Control prevalence"}</span><span>{(marker.prevalence_control * 100).toFixed(1)}%</span>
    </div>,
  );
}

function navigateToSpecies(taxon: string) {
  window.location.href = `/species/${encodeURIComponent(taxon)}`;
}

function drawForestPlot(svgEl: SVGSVGElement, markers: Marker[], locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const sorted = [...markers].sort((a, b) => Math.abs(b.log2fc) - Math.abs(a.log2fc));
  const margin = { top: 18, right: 28, bottom: 36, left: 176 };
  const width = 760;
  const height = Math.max(320, sorted.length * 24 + margin.top + margin.bottom);
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;

  const minX = Math.min(-2, d3.min(sorted, (marker) => marker.ci_low) ?? -2) * 1.1;
  const maxX = Math.max(2, d3.max(sorted, (marker) => marker.ci_high) ?? 2) * 1.1;
  const x = d3.scaleLinear().domain([minX, maxX]).range([0, innerWidth]);
  const y = d3.scaleBand().domain(sorted.map((marker) => marker.taxon)).range([0, innerHeight]).padding(0.28);

  svg.attr("viewBox", `0 0 ${width} ${height}`);
  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  root.append("line")
    .attr("x1", x(0))
    .attr("x2", x(0))
    .attr("y1", 0)
    .attr("y2", innerHeight)
    .attr("stroke", "rgba(255,255,255,0.25)")
    .attr("stroke-dasharray", "4,4");

  root.append("g")
    .call(d3.axisLeft(y).tickFormat((value) => value.length > 18 ? `${value.slice(0, 16)}…` : value))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("font-style", "italic");

  root.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(6))
    .attr("font-size", 10);

  root.append("text")
    .attr("x", innerWidth / 2)
    .attr("y", innerHeight + 30)
    .attr("text-anchor", "middle")
    .attr("fill", "currentColor")
    .attr("font-size", 10)
    .text(locale === "zh" ? "log₂ Fold Change（95% CI）" : "log₂ Fold Change (95% CI)");

  root.selectAll("line.ci")
    .data(sorted)
    .join("line")
    .attr("class", "ci")
    .attr("x1", (marker) => x(marker.ci_low))
    .attr("x2", (marker) => x(marker.ci_high))
    .attr("y1", (marker) => (y(marker.taxon) ?? 0) + y.bandwidth() / 2)
    .attr("y2", (marker) => (y(marker.taxon) ?? 0) + y.bandwidth() / 2)
    .attr("stroke", (marker) => phylumColor(marker.phylum))
    .attr("stroke-width", 1.6)
    .attr("opacity", 0.85);

  root.selectAll("circle.point")
    .data(sorted)
    .join("circle")
    .attr("class", "point")
    .attr("cx", (marker) => x(marker.log2fc))
    .attr("cy", (marker) => (y(marker.taxon) ?? 0) + y.bandwidth() / 2)
    .attr("r", 5.5)
    .attr("fill", (marker) => marker.enriched_in === "control" ? "transparent" : phylumColor(marker.phylum))
    .attr("stroke", (marker) => phylumColor(marker.phylum))
    .attr("stroke-width", (marker) => marker.enriched_in === "control" ? 1.8 : 0.8)
    .attr("data-tooltip", (marker) => tooltipForMarker(marker, locale))
    .style("cursor", "pointer")
    .on("click", (_, marker) => navigateToSpecies(marker.taxon));
}

function drawLdaChart(svgEl: SVGSVGElement, markers: Marker[], locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const sorted = [...markers].sort((a, b) => {
    const scoreA = a.enriched_in === "control" ? -a.lda_score : a.lda_score;
    const scoreB = b.enriched_in === "control" ? -b.lda_score : b.lda_score;
    return scoreB - scoreA;
  });

  const signedScores = sorted.map((marker) => (marker.enriched_in === "control" ? -marker.lda_score : marker.lda_score));
  const maxAbs = Math.max(d3.max(signedScores, (value) => Math.abs(value)) ?? 2, 2);
  const margin = { top: 18, right: 28, bottom: 36, left: 176 };
  const width = 760;
  const height = Math.max(320, sorted.length * 24 + margin.top + margin.bottom);
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const x = d3.scaleLinear().domain([-maxAbs * 1.1, maxAbs * 1.1]).range([0, innerWidth]);
  const y = d3.scaleBand().domain(sorted.map((marker) => marker.taxon)).range([0, innerHeight]).padding(0.2);

  svg.attr("viewBox", `0 0 ${width} ${height}`);
  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  root.append("line")
    .attr("x1", x(0))
    .attr("x2", x(0))
    .attr("y1", 0)
    .attr("y2", innerHeight)
    .attr("stroke", "rgba(255,255,255,0.25)");

  root.append("g")
    .call(d3.axisLeft(y).tickFormat((value) => value.length > 18 ? `${value.slice(0, 16)}…` : value))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("font-style", "italic");

  root.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(6))
    .attr("font-size", 10);

  root.append("text")
    .attr("x", innerWidth / 2)
    .attr("y", innerHeight + 30)
    .attr("text-anchor", "middle")
    .attr("fill", "currentColor")
    .attr("font-size", 10)
    .text(locale === "zh" ? "LDA 效应值（方向签名）" : "LDA Effect Size (signed)");

  root.selectAll("rect.bar")
    .data(sorted)
    .join("rect")
    .attr("class", "bar")
    .attr("x", (marker) => marker.enriched_in === "control" ? x(-marker.lda_score) : x(0))
    .attr("y", (marker) => y(marker.taxon) ?? 0)
    .attr("width", (marker) => Math.abs(x(marker.lda_score) - x(0)))
    .attr("height", y.bandwidth())
    .attr("rx", 3)
    .attr("fill", (marker) => phylumColor(marker.phylum))
    .attr("opacity", 0.9)
    .attr("data-tooltip", (marker) => tooltipForMarker(marker, locale))
    .style("cursor", "pointer")
    .on("click", (_, marker) => navigateToSpecies(marker.taxon));
}

export default BiomarkerPanel;
