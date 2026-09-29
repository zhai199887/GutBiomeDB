import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { exportTable } from "@/util/export";

import type { BiomarkerProfileData } from "./types";
import { formatPValue, formatPrevalence, formatPercent, translateDimensionName } from "./utils";

interface BiomarkerPanelProps {
  genus: string;
}

export default function BiomarkerPanel({ genus }: BiomarkerPanelProps) {
  const { locale, t } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [data, setData] = useState<BiomarkerProfileData | null>(null);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    cachedFetch<BiomarkerProfileData>(`${API_BASE}/api/biomarker-profile?genus=${encodeURIComponent(genus)}`)
      .then((payload) => setData(payload))
      .catch(() => setData(null));
  }, [genus]);

  const rows = useMemo(() => {
    if (!data) return [];
    const source = showAll ? data.profiles : data.profiles.filter((item) => item.significant);
    return source.slice(0, showAll ? 60 : 40);
  }, [data, showAll]);

  useEffect(() => {
    if (!svgRef.current || rows.length === 0) return;
    drawBiomarkerChart(svgRef.current, rows, locale);
  }, [locale, rows]);

  if (!data || data.profiles.length === 0) return null;

  const exportRows = rows.map((item) => ({
    disease: item.disease,
    log2fc: item.log2fc,
    adjusted_p: item.adjusted_p,
    effect_size: item.effect_size,
    prevalence_disease: item.prevalence_disease,
    prevalence_control: item.prevalence_control,
    n_samples: item.n_samples,
    n_control: item.n_control,
    significant: item.significant,
  }));

  return (
    <section className="species-block">
      <div className="species-blockHeader">
        <div>
          <h2>{t("species.tab.biomarker")}</h2>
          <p>{t("species.biomarker.subtitle")}</p>
        </div>
        <div className="species-toggleRow">
          <button type="button" className={!showAll ? "is-active" : ""} onClick={() => setShowAll(false)}>
            {t("species.sigOnly")}
          </button>
          <button type="button" className={showAll ? "is-active" : ""} onClick={() => setShowAll(true)}>
            {t("species.biomarker.showAll")}
          </button>
        </div>
      </div>

      <div className="species-summaryPills">
        <span>{`${t("species.biomarker.enriched")}: ${data.n_enriched}`}</span>
        <span>{`${t("species.biomarker.depleted")}: ${data.n_depleted}`}</span>
        <span>{`${t("species.biomarker.tested")}: ${data.n_diseases_tested}`}</span>
        <span>{`NC n: ${data.n_control.toLocaleString("en")}`}</span>
      </div>

      <article className="species-chartCard">
        <div className="species-chartHeader">
          <div>
            <h3>{t("species.biomarker.chartTitle")}</h3>
            <p>{t("species.biomarker.chartHint")}</p>
          </div>
          <div className="species-exportRow">
            <button type="button" onClick={() => exportTable(exportRows, `${genus}_biomarker_profile`)}>CSV</button>
            <button type="button" onClick={() => svgRef.current && exportSVG(svgRef.current, `${genus}_biomarker_profile`)}>SVG</button>
            <button type="button" onClick={() => svgRef.current && exportPNG(svgRef.current, `${genus}_biomarker_profile`)}>PNG</button>
          </div>
        </div>
        <svg ref={svgRef} className="species-chart" />
      </article>
    </section>
  );
}

function drawBiomarkerChart(svgEl: SVGSVGElement, rows: BiomarkerProfileData["profiles"], locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const width = 980;
  const height = Math.max(260, rows.length * 22 + 110);
  const margin = { top: 22, right: 100, bottom: 42, left: 260 };
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maxAbs = d3.max(rows, (row) => Math.abs(row.log2fc)) ?? 1;
  const x = d3.scaleLinear().domain([-maxAbs * 1.1, maxAbs * 1.1]).range([0, plotWidth]);
  const y = d3.scaleBand().domain(rows.map((row) => row.disease)).range([0, plotHeight]).padding(0.14);

  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  root.append("line")
    .attr("x1", x(0))
    .attr("x2", x(0))
    .attr("y1", 0)
    .attr("y2", plotHeight)
    .attr("stroke", "rgba(255,255,255,0.3)")
    .attr("stroke-width", 1.1);

  root.selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", (row) => (row.log2fc >= 0 ? x(0) : x(row.log2fc)))
    .attr("y", (row) => y(row.disease) ?? 0)
    .attr("width", (row) => Math.abs(x(row.log2fc) - x(0)))
    .attr("height", y.bandwidth())
    .attr("rx", 4)
    .attr("fill", (row) => (row.log2fc >= 0 ? "#ef4444" : "#2563eb"))
    .attr("opacity", (row) => (row.significant ? 0.88 : 0.35))
    .attr("data-tooltip", (row) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "疾病" : "Disease"}</span><span>{translateDimensionName(row.disease, locale, "disease")}</span>
        <span>log2FC</span><span>{row.log2fc.toFixed(3)}</span>
        <span>FDR</span><span>{formatPValue(row.adjusted_p)}</span>
        <span>{locale === "zh" ? "效应量" : "Effect size"}</span><span>{row.effect_size.toFixed(3)}</span>
        <span>{locale === "zh" ? "疾病组流行率" : "Disease prevalence"}</span><span>{formatPrevalence(row.prevalence_disease)}</span>
        <span>{locale === "zh" ? "对照组流行率" : "Control prevalence"}</span><span>{formatPrevalence(row.prevalence_control)}</span>
        <span>{locale === "zh" ? "疾病组均值" : "Disease mean"}</span><span>{formatPercent(row.mean_disease, 4)}</span>
        <span>{locale === "zh" ? "对照组均值" : "Control mean"}</span><span>{formatPercent(row.mean_control, 4)}</span>
      </div>,
    ));

  root.append("g")
    .call(
      d3.axisLeft(y).tickFormat((value) => {
        const label = translateDimensionName(String(value), locale, "disease");
        return label.length > 34 ? `${label.slice(0, 33)}…` : label;
      }),
    )
    .attr("font-size", 11);

  root.append("g")
    .attr("transform", `translate(0,${plotHeight})`)
    .call(d3.axisBottom(x).ticks(6))
    .attr("font-size", 10);
}
