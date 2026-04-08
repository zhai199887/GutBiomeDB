/**
 * NetworkPage.tsx — 网络可视化页面（含四个 Tab）
 * Association / Chord / Co-occurrence / Disease vs HC
 */
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { renderToString } from "react-dom/server";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";

import classes from "./NetworkPage.module.css";

const ChordPanel = lazy(() => import("./network/ChordPanel"));
const CooccurrencePanel = lazy(() => import("./network/CooccurrencePanel"));
const NetworkComparePanel = lazy(() => import("./network/NetworkComparePanel"));

type TabKey = "association" | "chord" | "cooccurrence" | "compare";

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
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("association");
  const [data, setData] = useState<NetworkData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    cachedFetch<NetworkData>(`${API_BASE}/api/network?top_diseases=12&top_genera=15`)
      .then((payload) => setData(payload))
      .catch((unknownError) => setError(unknownError instanceof Error ? unknownError.message : String(unknownError)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!svgRef.current || !data || activeTab !== "association") return;
    return drawAssociationNetwork(svgRef.current, data, locale);
  }, [activeTab, data, locale]);

  const summary = useMemo(() => ({
    nodes: data?.nodes.length ?? 0,
    edges: data?.edges.length ?? 0,
    diseases: data?.nodes.filter((node) => node.type === "disease").length ?? 0,
    genera: data?.nodes.filter((node) => node.type === "genus").length ?? 0,
  }), [data]);

  const tabs: Array<{ key: TabKey; label: string }> = [
    { key: "association", label: locale === "zh" ? "关联网络" : "Association" },
    { key: "chord", label: locale === "zh" ? "弦图" : "Chord" },
    { key: "cooccurrence", label: locale === "zh" ? "共现网络" : "Co-occurrence" },
    { key: "compare", label: locale === "zh" ? "疾病 vs 健康" : "Disease vs HC" },
  ];

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{locale === "zh" ? "← 返回 Atlas" : "← Back to Atlas"}</Link>
        <h1>{locale === "zh" ? "Network 工作台" : "Network workspace"}</h1>
        <p>
          {locale === "zh"
            ? "集中查看关联图、弦图、共现网络与疾病对照重塑。"
            : "An integrated workspace for association graphs, chord diagrams, co-occurrence structure, and disease-vs-control rewiring."}
        </p>
      </div>

      <div className={classes.tabs}>
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={activeTab === tab.key ? classes.tabActive : classes.tab}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "association" ? (
        <div className={classes.associationPanel}>
          {loading ? <div className={classes.loading}>{locale === "zh" ? "正在加载关联网络..." : "Loading association network..."}</div> : null}
          {error ? <div className={classes.error}>{error}</div> : null}

          {data ? (
            <>
              <div className={classes.summaryGrid}>
                <div className={classes.summaryCard}>
                  <div className={classes.summaryValue}>{summary.nodes}</div>
                  <div className={classes.summaryLabel}>{locale === "zh" ? "总节点" : "Total nodes"}</div>
                </div>
                <div className={classes.summaryCard}>
                  <div className={classes.summaryValue}>{summary.edges}</div>
                  <div className={classes.summaryLabel}>{locale === "zh" ? "关联边" : "Edges"}</div>
                </div>
                <div className={classes.summaryCard}>
                  <div className={classes.summaryValue}>{summary.diseases}</div>
                  <div className={classes.summaryLabel}>{locale === "zh" ? "疾病节点" : "Disease nodes"}</div>
                </div>
                <div className={classes.summaryCard}>
                  <div className={classes.summaryValue}>{summary.genera}</div>
                  <div className={classes.summaryLabel}>{locale === "zh" ? "菌属节点" : "Genus nodes"}</div>
                </div>
              </div>

              <div className={classes.graphContainer}>
                <svg ref={svgRef} />
                <div className={classes.legend}>
                  <div className={classes.legendItem}>
                    <span className={classes.legendDot} style={{ background: "#ff6b6b" }} />
                    <span>{locale === "zh" ? "疾病节点" : "Disease node"}</span>
                  </div>
                  <div className={classes.legendItem}>
                    <span className={classes.legendDot} style={{ background: "#4ecdc4" }} />
                    <span>{locale === "zh" ? "菌属节点" : "Genus node"}</span>
                  </div>
                  <div className={classes.legendItem}>
                    <span className={classes.legendLine} />
                    <span>{locale === "zh" ? "边宽 = 平均丰度" : "Edge width = mean abundance"}</span>
                  </div>
                </div>
                <div className={classes.exportRow}>
                  <button type="button" className={classes.exportBtn} onClick={() => svgRef.current && exportSVG(svgRef.current, `network_association_${Date.now()}`)}>SVG</button>
                  <button type="button" className={classes.exportBtn} onClick={() => svgRef.current && exportPNG(svgRef.current, `network_association_${Date.now()}`)}>PNG</button>
                </div>
              </div>
            </>
          ) : null}
        </div>
      ) : null}

      {activeTab === "chord" ? (
        <Suspense fallback={<div className={classes.loading}>{locale === "zh" ? "正在加载弦图..." : "Loading chord diagram..."}</div>}>
          <ChordPanel />
        </Suspense>
      ) : null}

      {activeTab === "cooccurrence" ? (
        <Suspense fallback={<div className={classes.loading}>{locale === "zh" ? "正在加载共现网络..." : "Loading co-occurrence..."}</div>}>
          <CooccurrencePanel />
        </Suspense>
      ) : null}

      {activeTab === "compare" ? (
        <Suspense fallback={<div className={classes.loading}>{locale === "zh" ? "正在加载对比视图..." : "Loading compare view..."}</div>}>
          <NetworkComparePanel />
        </Suspense>
      ) : null}
    </div>
  );
};

export default NetworkPage;

function drawAssociationNetwork(svgEl: SVGSVGElement, rawData: NetworkData, locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement;
  const width = container?.clientWidth ?? 1200;
  const height = container?.clientHeight ?? 720;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const nodes: NetworkNode[] = rawData.nodes.map((node) => ({ ...node }));
  const edges: NetworkEdge[] = rawData.edges.map((edge) => ({ ...edge }));

  const nodeRadius = d3.scaleLinear()
    .domain([0, d3.max(nodes, (node) => node.size) ?? 1])
    .range([9, 30]);
  const edgeWidth = d3.scaleLinear()
    .domain([0, d3.max(edges, (edge) => edge.weight) ?? 1])
    .range([0.7, 3.6]);

  const diseaseConnections = new Map<string, number>();
  const genusConnections = new Map<string, number>();
  edges.forEach((edge) => {
    const source = typeof edge.source === "string" ? edge.source : edge.source.id;
    const target = typeof edge.target === "string" ? edge.target : edge.target.id;
    diseaseConnections.set(source, (diseaseConnections.get(source) ?? 0) + 1);
    genusConnections.set(target, (genusConnections.get(target) ?? 0) + 1);
  });

  const g = svg.append("g");
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.3, 4])
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoom);

  const link = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", "rgba(255,255,255,0.32)")
    .attr("stroke-opacity", 0.34)
    .attr("stroke-width", (edge) => edgeWidth(edge.weight));

  const overlay = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", "transparent")
    .attr("stroke-width", 12)
    .attr("data-tooltip", (edge) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "疾病" : "Disease"}</span>
        <span>{diseaseDisplayNameI18n(typeof edge.source === "string" ? edge.source : edge.source.id, locale)}</span>
        <span>{locale === "zh" ? "菌属" : "Genus"}</span>
        <span><i>{typeof edge.target === "string" ? edge.target : edge.target.id}</i></span>
        <span>{locale === "zh" ? "平均丰度" : "Mean abundance"}</span>
        <span>{edge.weight.toFixed(3)}%</span>
      </div>
    ));

  const node = g.append("g")
    .selectAll("circle")
    .data(nodes)
    .join("circle")
    .attr("r", (item) => item.type === "disease" ? nodeRadius(item.size) : 7)
    .attr("fill", (item) => item.type === "disease" ? "#ff6b6b" : "#4ecdc4")
    .attr("opacity", 0.88)
    .attr("stroke", "rgba(255,255,255,0.22)")
    .attr("stroke-width", 1)
    .attr("cursor", "pointer")
    .attr("data-tooltip", (item) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "类型" : "Type"}</span>
        <span>{item.type === "disease" ? (locale === "zh" ? "疾病" : "Disease") : (locale === "zh" ? "菌属" : "Genus")}</span>
        <span>{locale === "zh" ? "名称" : "Name"}</span>
        <span>{item.type === "disease" ? diseaseDisplayNameI18n(item.id, locale) : <i>{item.id}</i>}</span>
        <span>{locale === "zh" ? "样本/连接" : "Samples / links"}</span>
        <span>{item.type === "disease" ? item.size.toLocaleString() : (genusConnections.get(item.id) ?? 0)}</span>
        <span>{locale === "zh" ? "相关边" : "Linked edges"}</span>
        <span>{item.type === "disease" ? (diseaseConnections.get(item.id) ?? 0) : (genusConnections.get(item.id) ?? 0)}</span>
      </div>
    ))
    .call(
      d3.drag<SVGCircleElement, NetworkNode>()
        .on("start", (event, item) => {
          if (!event.active) simulation.alphaTarget(0.3).restart();
          item.fx = item.x;
          item.fy = item.y;
        })
        .on("drag", (event, item) => {
          item.fx = event.x;
          item.fy = event.y;
        })
        .on("end", (event, item) => {
          if (!event.active) simulation.alphaTarget(0);
          item.fx = null;
          item.fy = null;
        }),
    );

  const label = g.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .text((item) => {
      const labelText = item.type === "disease" ? diseaseDisplayNameI18n(item.id, locale) : item.id;
      return labelText.length > 38 ? `${labelText.slice(0, 35)}...` : labelText;
    })
    .attr("font-size", (item) => item.type === "disease" ? 14.5 : 12.5)
    .attr("font-style", (item) => item.type === "genus" ? "italic" : "normal")
    .attr("font-weight", (item) => item.type === "disease" ? 700 : 500)
    .attr("fill", "currentColor")
    .attr("text-anchor", "middle")
    .attr("dy", (item) => item.type === "disease" ? -(nodeRadius(item.size) + 9) : -12)
    .style("pointer-events", "none")
    .style("text-shadow", "0 1px 4px rgba(0,0,0,0.9)");

  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink<NetworkNode, NetworkEdge>(edges).id((item) => item.id).distance(138).strength(0.32))
    .force("charge", d3.forceManyBody().strength(-250))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius((item) => item.type === "disease" ? nodeRadius(item.size) + 12 : 18))
    .on("tick", () => {
      link
        .attr("x1", (edge: any) => edge.source.x)
        .attr("y1", (edge: any) => edge.source.y)
        .attr("x2", (edge: any) => edge.target.x)
        .attr("y2", (edge: any) => edge.target.y);
      overlay
        .attr("x1", (edge: any) => edge.source.x)
        .attr("y1", (edge: any) => edge.source.y)
        .attr("x2", (edge: any) => edge.target.x)
        .attr("y2", (edge: any) => edge.target.y);
      node
        .attr("cx", (item: any) => item.x)
        .attr("cy", (item: any) => item.y);
      label
        .attr("x", (item: any) => item.x)
        .attr("y", (item: any) => item.y);
    });

  return () => simulation.stop();
}
