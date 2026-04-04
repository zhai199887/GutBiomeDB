/**
 * NetworkPage.tsx — 网络可视化页面（含三个 Tab）
 * Tab 1: 关联网络 (Association) — D3 力导向图
 * Tab 2: 弦图 (Chord) — D3 chord layout
 * Tab 3: 共现网络 (Co-occurrence) — Spearman 相关性力导向图
 */
import { useEffect, useRef, useState, lazy, Suspense } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayName } from "@/util/diseaseNames";
import classes from "./NetworkPage.module.css";

const ChordPanel = lazy(() => import("./network/ChordPanel"));
const CooccurrencePanel = lazy(() => import("./network/CooccurrencePanel"));

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

type TabKey = "association" | "chord" | "cooccurrence";

interface NetworkNode {
  id: string;
  type: "disease" | "genus";
  size: number;
  x?: number;
  y?: number;
  fx?: number | null;
  fy?: number | null;
}

interface NetworkEdge {
  source: string | NetworkNode;
  target: string | NetworkNode;
  weight: number;
}

interface NetworkData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}

const NetworkPage = () => {
  const { t, locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [data, setData] = useState<NetworkData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [activeTab, setActiveTab] = useState<TabKey>("association");

  // 加载关联网络数据 + 中文名
  useEffect(() => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    fetch(`${API_BASE}/api/network?top_diseases=12&top_genera=15`, { signal: controller.signal })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: NetworkData) => setData(d))
      .catch((err) => {
        if (err.name === "AbortError") {
          setError(t("compare.backendError"));
        } else {
          setError(t("compare.backendError"));
        }
      })
      .finally(() => { clearTimeout(timeout); setLoading(false); });
    fetch(`${API_BASE}/api/disease-names-zh`)
      .then((r) => r.json())
      .then(setDiseaseZh)
      .catch((err) => console.warn("disease-names-zh fetch failed:", err));
    return () => { clearTimeout(timeout); controller.abort(); };
  }, []);

  // 绘制力导向图（仅当 association tab 激活时）
  useEffect(() => {
    if (!svgRef.current || !data || activeTab !== "association") return;
    const dName = (name: string) => (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : diseaseDisplayName(name);
    drawNetwork(svgRef.current, data, dName);
  }, [data, locale, diseaseZh, activeTab]);

  const exportNetworkChart = (type: "svg" | "png") => {
    const svg = svgRef.current;
    if (!svg) return;
    type === "svg"
      ? exportSVG(svg, `network_association_${Date.now()}`)
      : exportPNG(svg, `network_association_${Date.now()}`);
  };

  const tabs: { key: TabKey; label: string }[] = [
    { key: "association", label: t("network.tabAssociation") },
    { key: "chord",       label: t("network.tabChord") },
    { key: "cooccurrence", label: t("network.tabCooccurrence") },
  ];

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("network.back")}</Link>
        <h1>{t("network.title")}</h1>
        <p>{t("network.subtitle")}</p>
      </div>

      {/* Tab 栏 */}
      <div className={classes.tabs}>
        {tabs.map(({ key, label }) => (
          <button
            key={key}
            className={activeTab === key ? classes.tabActive : classes.tab}
            onClick={() => setActiveTab(key)}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Tab 内容 */}
      {activeTab === "association" && (
        <>
          {loading && (
            <div className={classes.loading}>{t("search.searching")}</div>
          )}
          {error && (
            <div className={classes.loading} style={{ color: "#ff6b6b" }}>{error}</div>
          )}
          {data && (
            <div className={classes.graphContainer}>
              <svg ref={svgRef} />
              <div className={classes.legend}>
                <div className={classes.legendItem}>
                  <span className={classes.legendDot} style={{ background: "#ff6b6b" }} />
                  <span>{t("network.diseaseNode")}</span>
                </div>
                <div className={classes.legendItem}>
                  <span className={classes.legendDot} style={{ background: "#4ecdc4" }} />
                  <span>{t("network.genusNode")}</span>
                </div>
                <div className={classes.legendItem}>
                  <span className={classes.legendDot} style={{ background: "rgba(255,255,255,0.15)", border: "1px dashed var(--gray)" }} />
                  <span>{t("network.edgeWeight")}</span>
                </div>
              </div>
              <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem" }}>
                <button onClick={() => exportNetworkChart("svg")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
                <button onClick={() => exportNetworkChart("png")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
              </div>
            </div>
          )}
        </>
      )}

      {activeTab === "chord" && (
        <Suspense fallback={<div className={classes.loading}>{t("search.searching")}</div>}>
          <ChordPanel />
        </Suspense>
      )}

      {activeTab === "cooccurrence" && (
        <Suspense fallback={<div className={classes.loading}>{t("search.searching")}</div>}>
          <CooccurrencePanel />
        </Suspense>
      )}
    </div>
  );
};

export default NetworkPage;

// ── D3 force-directed network / D3 力导向网络 ──────────────────────────────

function drawNetwork(svgEl: SVGSVGElement, rawData: NetworkData, dName: (n: string) => string = (n) => n) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement!;
  const W = container.clientWidth;
  const H = container.clientHeight;
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  // 深拷贝，D3 会修改原数据
  const nodes: NetworkNode[] = rawData.nodes.map((n) => ({ ...n }));
  const edges: NetworkEdge[] = rawData.edges.map((e) => ({ ...e }));

  // 边权重比例尺
  const maxWeight = d3.max(edges, (e) => e.weight) ?? 1;
  const edgeOpacity = d3.scaleLinear().domain([0, maxWeight]).range([0.05, 0.5]);
  const edgeWidth = d3.scaleLinear().domain([0, maxWeight]).range([0.5, 3]);

  // 节点大小比例尺
  const diseaseNodes = nodes.filter((n) => n.type === "disease");
  const maxSize = d3.max(diseaseNodes, (n) => n.size) ?? 1;
  const nodeRadius = d3.scaleLinear().domain([0, maxSize]).range([8, 28]);

  // 创建缩放容器
  const g = svg.append("g");
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.3, 4])
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoom);

  // 绘制边
  const link = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", "rgba(255,255,255,0.3)")
    .attr("stroke-opacity", (d) => edgeOpacity(d.weight))
    .attr("stroke-width", (d) => edgeWidth(d.weight));

  // 绘制节点
  const node = g.append("g")
    .selectAll("circle")
    .data(nodes)
    .join("circle")
    .attr("r", (d) => d.type === "disease" ? nodeRadius(d.size) : 6)
    .attr("fill", (d) => d.type === "disease" ? "#ff6b6b" : "#4ecdc4")
    .attr("opacity", 0.85)
    .attr("stroke", "rgba(255,255,255,0.2)")
    .attr("stroke-width", 1)
    .attr("cursor", "pointer")
    .call(
      d3.drag<SVGCircleElement, NetworkNode>()
        .on("start", (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on("drag", (event, d) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on("end", (event, d) => {
          if (!event.active) simulation.alphaTarget(0);
          d.fx = null;
          d.fy = null;
        })
    );

  // 节点标签
  const label = g.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .text((d) => {
      const label = d.type === "disease" ? dName(d.id) : d.id;
      return label.length > 14 ? label.slice(0, 12) + "\u2026" : label;
    })
    .attr("font-size", (d) => d.type === "disease" ? 11 : 9)
    .attr("font-style", (d) => d.type === "genus" ? "italic" : "normal")
    .attr("font-weight", (d) => d.type === "disease" ? 600 : 400)
    .attr("fill", "currentColor")
    .attr("text-anchor", "middle")
    .attr("dy", (d) => d.type === "disease" ? -(nodeRadius(d.size) + 6) : -10)
    .style("pointer-events", "none")
    .style("text-shadow", "0 1px 4px rgba(0,0,0,0.9)");

  // 力学模拟
  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(edges).id((d: any) => d.id).distance(120).strength(0.3))
    .force("charge", d3.forceManyBody().strength(-200))
    .force("center", d3.forceCenter(W / 2, H / 2))
    .force("collision", d3.forceCollide().radius((d: any) => d.type === "disease" ? nodeRadius(d.size) + 10 : 15))
    .on("tick", () => {
      link
        .attr("x1", (d: any) => d.source.x)
        .attr("y1", (d: any) => d.source.y)
        .attr("x2", (d: any) => d.target.x)
        .attr("y2", (d: any) => d.target.y);
      node
        .attr("cx", (d: any) => d.x)
        .attr("cy", (d: any) => d.y);
      label
        .attr("x", (d: any) => d.x)
        .attr("y", (d: any) => d.y);
    });
}
