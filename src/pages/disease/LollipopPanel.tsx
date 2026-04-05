import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportTable } from "@/util/export";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { phylumColor } from "@/util/phylumColors";
import type { LollipopItem } from "./types";
import classes from "../DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface Props {
  disease: string;
}

const formatP = (value: number) => {
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const LollipopPanel = ({ disease }: Props) => {
  const { t, locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  const [data, setData] = useState<LollipopItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pCutoff, setPCutoff] = useState("0.05");
  const [fcCutoff, setFcCutoff] = useState("0");
  const [showOnlySig, setShowOnlySig] = useState(false);

  useEffect(() => {
    if (!disease) return;
    setLoading(true);
    setError(null);
    setData([]);
    cachedFetch<{ data: LollipopItem[] }>(`${API_BASE}/api/lollipop-data?disease=${encodeURIComponent(disease)}&top_n=120`)
      .then((response) => setData(response.data ?? []))
      .catch(() => {
        setError(locale === "zh" ? "后端未启动或连接失败" : "Backend not available or connection failed");
      })
      .finally(() => setLoading(false));
  }, [disease, locale]);

  const filteredData = useMemo(() => {
    const cutoff = pCutoff === "all" ? Number.POSITIVE_INFINITY : Number(pCutoff);
    const fc = Number(fcCutoff);
    return data.filter((item) => (
      item.adjusted_p <= cutoff
      && Math.abs(item.log2fc) >= fc
      && (!showOnlySig || item.adjusted_p < 0.05)
    ));
  }, [data, fcCutoff, pCutoff, showOnlySig]);

  useEffect(() => {
    if (!svgRef.current) return;
    if (filteredData.length === 0) {
      d3.select(svgRef.current).selectAll("*").remove();
      return;
    }
    drawLollipop(svgRef.current, filteredData, locale, Number(fcCutoff));
  }, [fcCutoff, filteredData, locale]);

  const exportCsv = () => {
    if (filteredData.length === 0) return;
    exportTable(
      filteredData.map((item) => ({
        Genus: item.genus,
        Phylum: item.phylum,
        Log2FC: item.log2fc,
        Adjusted_P: item.adjusted_p,
        P_value: item.p_value,
        NegLog10P: item.neg_log10p,
        Disease_Prevalence: item.prevalence_disease,
        Control_Prevalence: item.prevalence_control,
      })),
      `lollipop_${disease}_${Date.now()}`,
    );
  };

  const phyla = [...new Set(filteredData.map((item) => item.phylum))].filter(Boolean);

  return (
    <div>
      <div className={classes.biomarkerControls}>
        <div className={classes.field}>
          <label>{t("disease.lollipop.pCutoff")}</label>
          <select className={classes.inlineSelect} value={pCutoff} onChange={(event) => setPCutoff(event.target.value)}>
            <option value="all">{locale === "zh" ? "全部" : "All"}</option>
            <option value="0.05">0.05</option>
            <option value="0.01">0.01</option>
            <option value="0.001">0.001</option>
          </select>
        </div>

        <div className={classes.field}>
          <label>{t("disease.lollipop.fcCutoff")}</label>
          <select className={classes.inlineSelect} value={fcCutoff} onChange={(event) => setFcCutoff(event.target.value)}>
            <option value="0">{locale === "zh" ? "全部" : "All"}</option>
            <option value="0.5">0.5</option>
            <option value="1">1.0</option>
            <option value="2">2.0</option>
          </select>
        </div>

        <label className={classes.toggleLabel}>
          <input type="checkbox" checked={showOnlySig} onChange={(event) => setShowOnlySig(event.target.checked)} />
          <span>{t("disease.lollipop.sigOnly")}</span>
        </label>
      </div>

      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}
      {error && <div className={classes.errorMsg}>{error}</div>}

      {!loading && !error && filteredData.length === 0 && (
        <div className={classes.emptyPlot}>{locale === "zh" ? "当前过滤条件下没有结果" : "No taxa under current filters"}</div>
      )}

      {filteredData.length > 0 && (
        <div className={classes.chartCard}>
          <div className={classes.cardHeader}>
            <h3>{t("lollipop.title")}</h3>
            <div className={classes.exportActions}>
              <button onClick={exportCsv}>{t("export.csv")}</button>
              <button onClick={() => svgRef.current && exportSVG(svgRef.current, `lollipop_${disease}_${Date.now()}`)}>{t("export.svg")}</button>
              <button onClick={() => svgRef.current && exportPNG(svgRef.current, `lollipop_${disease}_${Date.now()}`)}>{t("export.png")}</button>
            </div>
          </div>

          <svg ref={svgRef} className={classes.chart} />

          <div className={classes.legend}>
            {phyla.map((phylum) => (
              <div key={phylum} className={classes.legendItem}>
                <span className={classes.legendDot} style={{ background: phylumColor(phylum) }} />
                <span>{phylum}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

function tooltipForItem(item: LollipopItem, locale: string) {
  return renderToString(
    <div className="tooltip-table">
      <span>{locale === "zh" ? "菌属" : "Genus"}</span><span><i>{item.genus}</i></span>
      <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{item.phylum}</span>
      <span>log₂FC</span><span>{item.log2fc.toFixed(3)}</span>
      <span>p value</span><span>{item.p_value.toExponential(2)}</span>
      <span>adj.p (BH)</span><span>{formatP(item.adjusted_p)}</span>
      <span>{locale === "zh" ? "疾病流行率" : "Prev. Disease"}</span><span>{(item.prevalence_disease * 100).toFixed(1)}%</span>
      <span>{locale === "zh" ? "对照流行率" : "Prev. Control"}</span><span>{(item.prevalence_control * 100).toFixed(1)}%</span>
      <span>{locale === "zh" ? "疾病均值" : "Mean Disease"}</span><span>{item.mean_disease.toFixed(3)}%</span>
      <span>{locale === "zh" ? "对照均值" : "Mean Control"}</span><span>{item.mean_control.toFixed(3)}%</span>
    </div>,
  );
}

function drawLollipop(svgEl: SVGSVGElement, data: LollipopItem[], locale: string, fcCutoff: number) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const sorted = [...data].sort((a, b) => a.log2fc - b.log2fc);
  const margin = { top: 18, right: 28, bottom: 36, left: 176 };
  const width = 780;
  const height = Math.max(360, sorted.length * 20 + margin.top + margin.bottom);
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const maxAbs = Math.max(d3.max(sorted, (item) => Math.abs(item.log2fc)) ?? 2, 2);
  const x = d3.scaleLinear().domain([-maxAbs * 1.15, maxAbs * 1.15]).range([0, innerWidth]);
  const y = d3.scaleBand().domain(sorted.map((item) => item.genus)).range([0, innerHeight]).padding(0.3);
  const r = d3.scaleLinear().domain([0, d3.max(sorted, (item) => item.neg_log10p) ?? 1]).range([3.5, 10]);

  svg.attr("viewBox", `0 0 ${width} ${height}`);
  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  root.append("line")
    .attr("x1", x(0))
    .attr("x2", x(0))
    .attr("y1", 0)
    .attr("y2", innerHeight)
    .attr("stroke", "rgba(255,255,255,0.25)");

  if (fcCutoff > 0) {
    [-fcCutoff, fcCutoff].forEach((value) => {
      root.append("line")
        .attr("x1", x(value))
        .attr("x2", x(value))
        .attr("y1", 0)
        .attr("y2", innerHeight)
        .attr("stroke", "rgba(255,255,255,0.18)")
        .attr("stroke-dasharray", "4,3");
    });
  }

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
    .text("log₂ Fold Change");

  root.selectAll("line.stick")
    .data(sorted)
    .join("line")
    .attr("class", "stick")
    .attr("x1", x(0))
    .attr("x2", (item) => x(item.log2fc))
    .attr("y1", (item) => (y(item.genus) ?? 0) + y.bandwidth() / 2)
    .attr("y2", (item) => (y(item.genus) ?? 0) + y.bandwidth() / 2)
    .attr("stroke", (item) => phylumColor(item.phylum))
    .attr("stroke-width", 1.5)
    .attr("opacity", 0.6);

  root.selectAll("circle.dot")
    .data(sorted)
    .join("circle")
    .attr("class", "dot")
    .attr("cx", (item) => x(item.log2fc))
    .attr("cy", (item) => (y(item.genus) ?? 0) + y.bandwidth() / 2)
    .attr("r", (item) => r(item.neg_log10p))
    .attr("fill", (item) => phylumColor(item.phylum))
    .attr("opacity", 0.88)
    .attr("data-tooltip", (item) => tooltipForItem(item, locale))
    .style("cursor", "pointer")
    .on("click", (_, item) => {
      window.location.href = `/species/${encodeURIComponent(item.genus)}`;
    });
}

export default LollipopPanel;
