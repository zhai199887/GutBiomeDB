import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { phylumColor } from "@/util/phylumColors";

import styles from "./NetworkPanel.module.css";
import { API_BASE } from "./types";

const DISEASE_COLOR = "#ff6b6b";
const DEFAULT_GENUS_COLOR = "#94a3b8";

interface ChordData {
  diseases: string[];
  genera: string[];
  phyla: string[];
  matrix: number[][];
}

const ChordPanel = () => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [topDiseases, setTopDiseases] = useState(10);
  const [topGenera, setTopGenera] = useState(12);
  const [data, setData] = useState<ChordData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    cachedFetch<ChordData>(`${API_BASE}/api/chord-data?top_diseases=${topDiseases}&top_genera=${topGenera}`)
      .then((payload) => setData(payload))
      .catch((unknownError) => setError(unknownError instanceof Error ? unknownError.message : String(unknownError)))
      .finally(() => setLoading(false));
  }, [topDiseases, topGenera]);

  useEffect(() => {
    if (!svgRef.current || !data) return;
    drawChord(svgRef.current, data, locale);
  }, [data, locale]);

  return (
    <div className={styles.workspace}>
      <div className={styles.toolbar}>
        <div className={styles.field}>
          <label>{locale === "zh" ? "Top 疾病数" : "Top diseases"}</label>
          <select className={styles.select} value={topDiseases} onChange={(event) => setTopDiseases(Number(event.target.value))}>
            {[5, 8, 10, 12, 15].map((value) => (
              <option key={value} value={value}>{value}</option>
            ))}
          </select>
        </div>
        <div className={styles.field}>
          <label>{locale === "zh" ? "Top 菌属数" : "Top genera"}</label>
          <select className={styles.select} value={topGenera} onChange={(event) => setTopGenera(Number(event.target.value))}>
            {[8, 10, 12, 15, 20].map((value) => (
              <option key={value} value={value}>{value}</option>
            ))}
          </select>
        </div>
        <div className={styles.field}>
          <label>{locale === "zh" ? "导出" : "Export"}</label>
          <div className={styles.btnGroup}>
            <button type="button" className={styles.actionBtn} onClick={() => svgRef.current && exportSVG(svgRef.current, `network_chord_${Date.now()}`)}>SVG</button>
            <button type="button" className={styles.actionBtn} onClick={() => svgRef.current && exportPNG(svgRef.current, `network_chord_${Date.now()}`)}>PNG</button>
          </div>
        </div>
      </div>

      {loading ? <div className={styles.loading}>{locale === "zh" ? "正在构建弦图..." : "Building chord diagram..."}</div> : null}
      {error ? <div className={styles.error}>{error}</div> : null}

      {data ? (
        <div className={styles.graphCard}>
          <div className={styles.graphHead}>
            <div>
              <h3 className={styles.cardTitle}>{locale === "zh" ? "疾病 × 菌属弦图" : "Disease × genus chord diagram"}</h3>
              <p className={styles.cardSubtle}>
                {locale === "zh"
                  ? "悬停弧段或弦查看名称与权重。"
                  : "Hover arcs or ribbons to inspect names and weights."}
              </p>
            </div>
          </div>
          <div className={styles.graphContainer}>
            <svg ref={svgRef} />
          </div>
        </div>
      ) : null}
    </div>
  );
};

export default ChordPanel;

function drawChord(svgEl: SVGSVGElement, data: ChordData, locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const { diseases, genera, phyla, matrix } = data;
  const total = diseases.length + genera.length;
  const fullMatrix: number[][] = Array.from({ length: total }, () => Array(total).fill(0));

  for (let i = 0; i < diseases.length; i += 1) {
    for (let j = 0; j < genera.length; j += 1) {
      const value = matrix[i]?.[j] ?? 0;
      fullMatrix[i][diseases.length + j] = value;
      fullMatrix[diseases.length + j][i] = value;
    }
  }

  const size = 1320;
  const outerRadius = size / 2 - 195;
  const innerRadius = outerRadius - 36;

  svg.attr("viewBox", `0 0 ${size} ${size}`);
  const g = svg.append("g").attr("transform", `translate(${size / 2}, ${size / 2})`);

  const chord = d3.chord().padAngle(0.04).sortSubgroups(d3.descending);
  const chords = chord(fullMatrix);
  const arc = d3.arc<d3.ChordGroup>().innerRadius(innerRadius).outerRadius(outerRadius);
  const ribbon = d3.ribbon<d3.Chord, d3.ChordSubgroup>().radius(innerRadius);

  const colorOf = (index: number) => {
    if (index < diseases.length) return DISEASE_COLOR;
    return phylumColor(phyla[index - diseases.length]) ?? DEFAULT_GENUS_COLOR;
  };

  const nameOf = (index: number) => (
    index < diseases.length
      ? diseaseDisplayNameI18n(diseases[index], locale)
      : genera[index - diseases.length]
  );

  const groups = g.selectAll(".arc")
    .data(chords.groups)
    .join("g")
    .attr("class", "arc");

  const ribbons = g.selectAll(".ribbon")
    .data(chords)
    .join("path")
    .attr("class", "ribbon")
    .attr("d", ribbon as any)
    .attr("fill", (item) => colorOf(item.source.index))
    .attr("opacity", 0.62)
    .attr("stroke", "none");

  groups.append("path")
    .attr("d", arc as any)
    .attr("fill", (item) => colorOf(item.index))
    .attr("stroke", "rgba(0,0,0,0.3)")
    .style("cursor", "pointer")
    .on("mouseover", (_event, item) => {
      ribbons.style("opacity", (ribbonItem) => (
        ribbonItem.source.index === item.index || ribbonItem.target.index === item.index ? 0.85 : 0.06
      ));
    })
    .on("mouseout", () => {
      ribbons.style("opacity", 0.62);
    })
    .append("title")
    .text((item) => {
      const totalValue = d3.sum(fullMatrix[item.index] ?? []);
      return `${nameOf(item.index)}\n${locale === "zh" ? "总权重" : "Total weight"}: ${totalValue.toFixed(3)}`;
    });

  groups.append("text")
    .each((item) => { (item as any).angle = (item.startAngle + item.endAngle) / 2; })
    .attr("dy", "0.35em")
    .attr("transform", (item) => {
      const angle = ((item as any).angle * 180) / Math.PI - 90;
      const flip = (item as any).angle > Math.PI;
      return `rotate(${angle}) translate(${outerRadius + 18}) ${flip ? "rotate(180)" : ""}`;
    })
    .attr("text-anchor", (item) => ((item as any).angle > Math.PI ? "end" : "start"))
    .attr("font-size", 17)
    .attr("font-weight", 600)
    .attr("fill", "currentColor")
    .attr("font-style", (item) => (item.index >= diseases.length ? "italic" : "normal"))
    .text((item) => {
      const label = nameOf(item.index);
      return label.length > 48 ? `${label.slice(0, 45)}…` : label;
    });

  ribbons.append("title")
    .text((item) => {
      const left = nameOf(item.source.index);
      const right = nameOf(item.target.index);
      return `${left} ↔ ${right}\n${locale === "zh" ? "权重" : "Weight"}: ${item.source.value.toFixed(3)}`;
    });
}
