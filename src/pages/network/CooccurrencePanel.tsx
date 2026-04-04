/**
 * CooccurrencePanel.tsx — 共现网络面板（嵌入 NetworkPage Tab）
 * 从 CooccurrencePage.tsx 提取核心逻辑，去掉 page wrapper
 */
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import classes from "../CooccurrencePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface CoNode { id: string; mean_abundance: number; x?: number; y?: number; fx?: number | null; fy?: number | null; }
interface CoEdge { source: string | CoNode; target: string | CoNode; r: number; p_value: number; type: string; }
interface CoData { nodes: CoNode[]; edges: CoEdge[]; n_samples: number; n_genera: number; n_edges: number; }
interface DiseaseItem { name: string; sample_count: number; }

const CooccurrencePanel = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [disease, setDisease] = useState("");
  const [minR, setMinR] = useState(0.3);
  const [data, setData] = useState<CoData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const svgRef = useRef<SVGSVGElement>(null);

  const dName = (n: string) => (locale === "zh" && diseaseZh[n]) ? diseaseZh[n] : diseaseDisplayNameI18n(n, locale);

  // 加载疾病列表 + 中文名
  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`)
      .then(r => r.json())
      .then(d => setDiseases(d.diseases ?? []))
      .catch((err) => console.warn("disease-list fetch failed:", err));
    fetch(`${API_BASE}/api/disease-names-zh`)
      .then(r => r.json())
      .then(setDiseaseZh)
      .catch((err) => console.warn("disease-names-zh fetch failed:", err));
  }, []);

  // 加载共现网络数据
  useEffect(() => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams({ min_r: String(minR), top_genera: "40" });
    if (disease) params.set("disease", disease);
    fetch(`${API_BASE}/api/cooccurrence?${params}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: CoData) => setData(d))
      .catch((err) => setError(err.message ?? "Failed to load co-occurrence data"))
      .finally(() => setLoading(false));
  }, [disease, minR]);

  // 绘制共现网络
  useEffect(() => {
    if (!svgRef.current || !data) return;
    drawCooccurrence(svgRef.current, data);
  }, [data]);

  return (
    <div>
      {/* 控件：疾病选择器 + 最小相关系数 */}
      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("cooccurrence.disease")}</label>
          <select className={classes.select} value={disease} onChange={e => setDisease(e.target.value)}>
            <option value="">Healthy (NC)</option>
            {diseases.slice(0, 100).map(d => (
              <option key={d.name} value={d.name}>{dName(d.name)}</option>
            ))}
          </select>
        </div>
        <div className={classes.field}>
          <label>{t("cooccurrence.minR")}</label>
          <select className={classes.select} value={minR} onChange={e => setMinR(Number(e.target.value))}>
            <option value={0.2}>0.2</option>
            <option value={0.3}>0.3</option>
            <option value={0.4}>0.4</option>
            <option value={0.5}>0.5</option>
          </select>
        </div>
      </div>

      {loading && <div className={classes.loading}>{t("cooccurrence.computing")}</div>}
      {error && <div className={classes.loading} style={{ color: "#ff6b6b" }}>{error}</div>}

      {data && (
        <>
          <div className={classes.graphContainer}>
            <svg ref={svgRef} />
          </div>
          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem" }}>
            <button onClick={() => {
              if (!data) return;
              exportTable(
                data.edges.map((e) => ({
                  Source: typeof e.source === "string" ? e.source : e.source.id,
                  Target: typeof e.target === "string" ? e.target : e.target.id,
                  Correlation: e.r,
                  P_value: e.p_value,
                })),
                `cooccurrence_${Date.now()}`,
              );
            }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
            <button onClick={() => { const svg = svgRef.current; if (svg) exportSVG(svg, `cooccurrence_${Date.now()}`); }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
            <button onClick={() => { const svg = svgRef.current; if (svg) exportPNG(svg, `cooccurrence_${Date.now()}`); }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
          </div>
          <div className={classes.legend}>
            <div className={classes.legendItem}>
              <span className={classes.legendDot} style={{ background: "#4ecdc4" }} />
              <span>{t("cooccurrence.positive")}</span>
            </div>
            <div className={classes.legendItem}>
              <span className={classes.legendDot} style={{ background: "#ff6b6b" }} />
              <span>{t("cooccurrence.negative")}</span>
            </div>
          </div>
          <div className={classes.stats}>
            <span>{data.n_genera} genera</span>
            <span>{data.n_edges} edges</span>
            <span>{data.n_samples} samples</span>
          </div>
        </>
      )}
    </div>
  );
};

export default CooccurrencePanel;

// ── D3 共现网络力导向图 ──────────────────────────────────────────────────────

function drawCooccurrence(svgEl: SVGSVGElement, data: CoData) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const W = container.clientWidth;
  const H = container.clientHeight || 550;
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const nodes: CoNode[] = data.nodes.map(n => ({ ...n }));
  const edges: CoEdge[] = data.edges.map(e => ({ ...e }));

  const maxAbund = d3.max(nodes, n => n.mean_abundance) ?? 1;
  const nodeRadius = d3.scaleLinear().domain([0, maxAbund]).range([5, 20]);

  const g = svg.append("g");
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.3, 4])
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoom);

  const link = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", d => d.type === "positive" ? "#4ecdc4" : "#ff6b6b")
    .attr("stroke-opacity", d => Math.min(Math.abs(d.r), 0.8))
    .attr("stroke-width", d => Math.abs(d.r) * 3);

  const node = g.append("g")
    .selectAll("circle")
    .data(nodes)
    .join("circle")
    .attr("r", d => nodeRadius(d.mean_abundance))
    .attr("fill", "#4ecdc4")
    .attr("opacity", 0.8)
    .attr("stroke", "rgba(255,255,255,0.2)")
    .attr("stroke-width", 1)
    .attr("cursor", "pointer")
    .call(
      d3.drag<SVGCircleElement, CoNode>()
        .on("start", (event, d) => { if (!event.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on("drag", (event, d) => { d.fx = event.x; d.fy = event.y; })
        .on("end", (event, d) => { if (!event.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
    );

  const label = g.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .text(d => d.id.length > 12 ? d.id.slice(0, 10) + "\u2026" : d.id)
    .attr("font-size", 9)
    .attr("font-style", "italic")
    .attr("fill", "currentColor")
    .attr("text-anchor", "middle")
    .attr("dy", d => -(nodeRadius(d.mean_abundance) + 6))
    .style("pointer-events", "none")
    .style("text-shadow", "0 1px 4px rgba(0,0,0,0.9)");

  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(edges).id((d: any) => d.id).distance(100).strength(0.2))
    .force("charge", d3.forceManyBody().strength(-150))
    .force("center", d3.forceCenter(W / 2, H / 2))
    .force("collision", d3.forceCollide().radius((d: any) => nodeRadius(d.mean_abundance) + 8))
    .on("tick", () => {
      link
        .attr("x1", (d: any) => d.source.x).attr("y1", (d: any) => d.source.y)
        .attr("x2", (d: any) => d.target.x).attr("y2", (d: any) => d.target.y);
      node.attr("cx", (d: any) => d.x).attr("cy", (d: any) => d.y);
      label.attr("x", (d: any) => d.x).attr("y", (d: any) => d.y);
    });
}
