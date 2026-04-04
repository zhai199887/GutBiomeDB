/**
 * ChordPanel.tsx — 弦图面板（嵌入 NetworkPage Tab）
 * 从 ChordPage.tsx 提取核心逻辑，去掉 page wrapper
 */
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { cachedFetch } from "@/util/apiCache";
import classes from "../ChordPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const PHYLUM_COLORS: Record<string, string> = {
  Bacillota: "#e74c3c", Bacteroidota: "#3498db", Actinomycetota: "#2ecc71",
  Pseudomonadota: "#f39c12", Verrucomicrobiota: "#9b59b6",
  Fusobacteriota: "#1abc9c", Euryarchaeota: "#e67e22",
};
const DISEASE_COLOR = "#ff6b6b";
const DEFAULT_GENUS_COLOR = "#95a5a6";

interface ChordData {
  diseases: string[];
  genera: string[];
  phyla: string[];
  matrix: number[][];
}

const ChordPanel = () => {
  const { t, locale } = useI18n();
  const [topDiseases, setTopDiseases] = useState(10);
  const [topGenera, setTopGenera] = useState(12);
  const [data, setData] = useState<ChordData | null>(null);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const svgRef = useRef<SVGSVGElement>(null);

  const dName = (n: string) => (locale === "zh" && diseaseZh[n]) ? diseaseZh[n] : diseaseDisplayNameI18n(n, locale);

  // 加载中文疾病名
  useEffect(() => {
    cachedFetch<Record<string, string>>(`${API_BASE}/api/disease-names-zh`)
      .then(setDiseaseZh)
      .catch(() => {});
  }, []);

  // 加载弦图数据
  useEffect(() => {
    setLoading(true);
    setError("");
    cachedFetch<ChordData>(`${API_BASE}/api/chord-data?top_diseases=${topDiseases}&top_genera=${topGenera}`)
      .then((d) => setData(d))
      .catch((err) => setError((err as Error).message ?? "Failed to load chord data"))
      .finally(() => setLoading(false));
  }, [topDiseases, topGenera]);

  // 绘制弦图
  useEffect(() => {
    if (!svgRef.current || !data) return;
    drawChord(svgRef.current, data, dName);
  }, [data, locale, diseaseZh]);

  return (
    <div>
      {/* 控件 */}
      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("chord.topDiseases")}</label>
          <select className={classes.select} value={topDiseases} onChange={e => setTopDiseases(Number(e.target.value))}>
            {[5, 8, 10, 12, 15].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
        <div className={classes.field}>
          <label>{t("chord.topGenera")}</label>
          <select className={classes.select} value={topGenera} onChange={e => setTopGenera(Number(e.target.value))}>
            {[8, 10, 12, 15, 20].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
      </div>

      {loading && <div className={classes.loading}>{t("search.searching")}</div>}
      {error && <div className={classes.loading} style={{ color: "#ff6b6b" }}>{error}</div>}

      {data && (
        <>
          <div className={classes.chordContainer}>
            <svg ref={svgRef} className={classes.chordSvg} />
          </div>
          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem" }}>
            <button onClick={() => { const svg = svgRef.current; if (svg) exportSVG(svg, `chord_${Date.now()}`); }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
            <button onClick={() => { const svg = svgRef.current; if (svg) exportPNG(svg, `chord_${Date.now()}`); }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
          </div>
          <p className={classes.hint}>{t("chord.hovering")}</p>
        </>
      )}
    </div>
  );
};

export default ChordPanel;

// ── D3 Chord Diagram / D3 弦图 ───────────────────────────────────────────────

function drawChord(svgEl: SVGSVGElement, data: ChordData, dName: (n: string) => string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const { diseases, genera, phyla, matrix } = data;
  const n = diseases.length + genera.length;

  const fullMatrix: number[][] = Array.from({ length: n }, () => Array(n).fill(0));

  for (let i = 0; i < diseases.length; i++) {
    for (let j = 0; j < genera.length; j++) {
      const val = matrix[i]?.[j] ?? 0;
      fullMatrix[i][diseases.length + j] = val;
      fullMatrix[diseases.length + j][i] = val;
    }
  }

  const size = 600;
  const outerRadius = size / 2 - 80;
  const innerRadius = outerRadius - 20;

  svg.attr("viewBox", `0 0 ${size} ${size}`);
  const g = svg.append("g").attr("transform", `translate(${size / 2},${size / 2})`);

  const chord = d3.chord().padAngle(0.04).sortSubgroups(d3.descending);
  const chords = chord(fullMatrix);

  const arc = d3.arc<d3.ChordGroup>().innerRadius(innerRadius).outerRadius(outerRadius);
  const ribbon = d3.ribbon<d3.Chord, d3.ChordSubgroup>().radius(innerRadius);

  const colorOf = (i: number) => {
    if (i < diseases.length) return DISEASE_COLOR;
    return PHYLUM_COLORS[phyla[i - diseases.length]] ?? DEFAULT_GENUS_COLOR;
  };

  const nameOf = (i: number) => {
    if (i < diseases.length) return dName(diseases[i]);
    return genera[i - diseases.length];
  };

  const groups = g.selectAll(".arc")
    .data(chords.groups)
    .join("g")
    .attr("class", "arc");

  groups.append("path")
    .attr("d", arc as any)
    .attr("fill", d => colorOf(d.index))
    .attr("stroke", "rgba(0,0,0,0.3)")
    .style("cursor", "pointer")
    .on("mouseover", function (_event, d) {
      ribbons.style("opacity", r =>
        r.source.index === d.index || r.target.index === d.index ? 0.8 : 0.05
      );
    })
    .on("mouseout", () => {
      ribbons.style("opacity", 0.6);
    });

  groups.append("text")
    .each(d => { (d as any).angle = (d.startAngle + d.endAngle) / 2; })
    .attr("dy", "0.35em")
    .attr("transform", d => {
      const angle = ((d as any).angle * 180) / Math.PI - 90;
      const flip = (d as any).angle > Math.PI;
      return `rotate(${angle}) translate(${outerRadius + 10}) ${flip ? "rotate(180)" : ""}`;
    })
    .attr("text-anchor", d => (d as any).angle > Math.PI ? "end" : "start")
    .attr("font-size", 12)
    .attr("fill", "currentColor")
    .attr("font-weight", 500)
    .attr("font-style", d => d.index >= diseases.length ? "italic" : "normal")
    .text(d => {
      const name = nameOf(d.index);
      return name.length > 18 ? name.slice(0, 16) + "\u2026" : name;
    });

  const ribbons = g.selectAll(".ribbon")
    .data(chords)
    .join("path")
    .attr("class", "ribbon")
    .attr("d", ribbon as any)
    .attr("fill", d => colorOf(d.source.index))
    .attr("opacity", 0.6)
    .attr("stroke", "none");
}
