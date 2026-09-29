/**
 * LollipopPage.tsx — Lollipop Differential Abundance Plot
 * log2FC + significance + phylum-level coloring
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { API_BASE } from "@/util/apiBase";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";
import classes from "./LollipopPage.module.css";

const PHYLUM_COLORS: Record<string, string> = {
  Bacillota: "#e74c3c",
  Bacteroidota: "#3498db",
  Actinomycetota: "#2ecc71",
  Pseudomonadota: "#f39c12",
  Verrucomicrobiota: "#9b59b6",
  Fusobacteriota: "#1abc9c",
  Euryarchaeota: "#e67e22",
  Synergistota: "#34495e",
};
const DEFAULT_COLOR = "#95a5a6";

interface LollipopItem {
  genus: string;
  phylum: string;
  log2fc: number;
  neg_log10p: number;
  p_value: number;
  mean_disease: number;
  mean_control: number;
}

interface DiseaseItem { name: string; sample_count: number; }

const LollipopPage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [selected, setSelected] = useState("");
  const [data, setData] = useState<LollipopItem[]>([]);
  const [loading, setLoading] = useState(false);
  const svgRef = useRef<SVGSVGElement>(null);

  const dName = (n: string) => (locale === "zh" && diseaseZh[n]) ? diseaseZh[n] : diseaseDisplayNameI18n(n, locale);

  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`).then(r => r.json())
      .then(d => setDiseases(d.diseases ?? [])).catch(() => {});
    fetch(`${API_BASE}/api/disease-names-zh`).then(r => r.json())
      .then(setDiseaseZh).catch(() => {});
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  useEffect(() => {
    if (!selected) return;
    setLoading(true);
    fetch(`${API_BASE}/api/lollipop-data?disease=${encodeURIComponent(selected)}`)
      .then(r => r.json())
      .then(d => setData(d.data ?? []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selected]);

  useEffect(() => {
    if (!svgRef.current || data.length === 0) return;
    drawLollipop(svgRef.current, data);
  }, [data]);

  const phyla = [...new Set(data.map(d => d.phylum))];

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("lollipop.back")}</Link>
        <h1>{t("lollipop.title")}</h1>
        <p>{t("lollipop.subtitle")}</p>
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
      </div>

      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}

      {data.length > 0 && (
        <div className={classes.chartCard}>
          <svg ref={svgRef} className={classes.chart} />
          <div className={classes.legend}>
            {phyla.map(p => (
              <div key={p} className={classes.legendItem}>
                <span className={classes.legendDot} style={{ background: PHYLUM_COLORS[p] ?? DEFAULT_COLOR }} />
                <span>{p}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default LollipopPage;

function drawLollipop(svgEl: SVGSVGElement, data: LollipopItem[]) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const sorted = [...data].sort((a, b) => a.log2fc - b.log2fc);

  const margin = { top: 10, right: 40, bottom: 30, left: 130 };
  const W = 700, H = Math.max(400, sorted.length * 18 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const maxAbs = d3.max(sorted, d => Math.abs(d.log2fc)) ?? 3;
  const xScale = d3.scaleLinear().domain([-maxAbs * 1.1, maxAbs * 1.1]).range([0, iW]);

  const yScale = d3.scaleBand()
    .domain(sorted.map(d => d.genus))
    .range([0, iH])
    .padding(0.3);

  const maxSig = d3.max(sorted, d => d.neg_log10p) ?? 10;
  const rScale = d3.scaleLinear().domain([0, maxSig]).range([3, 10]);

  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)");

  g.selectAll(".stick")
    .data(sorted)
    .join("line")
    .attr("x1", xScale(0))
    .attr("x2", d => xScale(d.log2fc))
    .attr("y1", d => (yScale(d.genus) ?? 0) + yScale.bandwidth() / 2)
    .attr("y2", d => (yScale(d.genus) ?? 0) + yScale.bandwidth() / 2)
    .attr("stroke", d => PHYLUM_COLORS[d.phylum] ?? DEFAULT_COLOR)
    .attr("stroke-width", 1.5)
    .attr("opacity", 0.6);

  g.selectAll(".dot")
    .data(sorted)
    .join("circle")
    .attr("cx", d => xScale(d.log2fc))
    .attr("cy", d => (yScale(d.genus) ?? 0) + yScale.bandwidth() / 2)
    .attr("r", d => rScale(d.neg_log10p))
    .attr("fill", d => PHYLUM_COLORS[d.phylum] ?? DEFAULT_COLOR)
    .attr("opacity", 0.85);

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
    .text("Log\u2082 Fold Change");
}
