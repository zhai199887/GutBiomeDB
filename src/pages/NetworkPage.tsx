/**
 * NetworkPage.tsx — Microbe-Disease Association Network
 * 菌群-疾病关联网络：D3 力导向图可视化
 * Ref: Peryton database network visualization
 */
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import classes from "./NetworkPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

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
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});

  // Load network data + Chinese names / 加载网络数据 + 中文名
  useEffect(() => {
    fetch(`${API_BASE}/api/network?top_diseases=12&top_genera=15`)
      .then((r) => r.json())
      .then((d: NetworkData) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
    fetch(`${API_BASE}/api/disease-names-zh`)
      .then((r) => r.json())
      .then(setDiseaseZh)
      .catch(() => {});
  }, []);

  // Draw force-directed graph / 绘制力导向图
  useEffect(() => {
    if (!svgRef.current || !data) return;
    const dName = (name: string) => (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : name;
    drawNetwork(svgRef.current, data, dName);
  }, [data, locale, diseaseZh]);

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("network.back")}</Link>
        <h1>{t("network.title")}</h1>
        <p>{t("network.subtitle")}</p>
      </div>

      {loading && (
        <div className={classes.loading}>{t("search.searching")}</div>
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
        </div>
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

  // Deep copy data for D3 mutation / 深拷贝，D3 会修改原数据
  const nodes: NetworkNode[] = rawData.nodes.map((n) => ({ ...n }));
  const edges: NetworkEdge[] = rawData.edges.map((e) => ({ ...e }));

  // Edge weight scale / 边权重比例尺
  const maxWeight = d3.max(edges, (e) => e.weight) ?? 1;
  const edgeOpacity = d3.scaleLinear().domain([0, maxWeight]).range([0.05, 0.5]);
  const edgeWidth = d3.scaleLinear().domain([0, maxWeight]).range([0.5, 3]);

  // Node size scale / 节点大小比例尺
  const diseaseNodes = nodes.filter((n) => n.type === "disease");
  const maxSize = d3.max(diseaseNodes, (n) => n.size) ?? 1;
  const nodeRadius = d3.scaleLinear().domain([0, maxSize]).range([8, 28]);

  // Create zoom container / 创建缩放容器
  const g = svg.append("g");
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.3, 4])
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoom);

  // Draw edges / 绘制边
  const link = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", "rgba(255,255,255,0.3)")
    .attr("stroke-opacity", (d) => edgeOpacity(d.weight))
    .attr("stroke-width", (d) => edgeWidth(d.weight));

  // Draw nodes / 绘制节点
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

  // Node labels / 节点标签
  const label = g.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .text((d) => {
      const label = d.type === "disease" ? dName(d.id) : d.id;
      return label.length > 14 ? label.slice(0, 12) + "…" : label;
    })
    .attr("font-size", (d) => d.type === "disease" ? 11 : 9)
    .attr("font-style", (d) => d.type === "genus" ? "italic" : "normal")
    .attr("font-weight", (d) => d.type === "disease" ? 600 : 400)
    .attr("fill", "currentColor")
    .attr("text-anchor", "middle")
    .attr("dy", (d) => d.type === "disease" ? -(nodeRadius(d.size) + 6) : -10)
    .style("pointer-events", "none")
    .style("text-shadow", "0 1px 4px rgba(0,0,0,0.9)");

  // Force simulation / 力学模拟
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
