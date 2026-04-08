import * as d3 from "d3";
import { renderToString } from "react-dom/server";

import { phylumColor } from "@/util/phylumColors";

import type { CoData, CoEdge, CoNode, ColorMode } from "./types";

const POSITIVE_COLOR = "#4ecdc4";
const NEGATIVE_COLOR = "#ff6b6b";
const HUB_STROKE = "#facc15";

const edgeSourceId = (edge: Pick<CoEdge, "source" | "target">) => (
  typeof edge.source === "string" ? edge.source : edge.source.id
);

const edgeTargetId = (edge: Pick<CoEdge, "source" | "target">) => (
  typeof edge.target === "string" ? edge.target : edge.target.id
);

export const cooccurrenceEdgeKey = (edge: Pick<CoEdge, "source" | "target">) => (
  [edgeSourceId(edge), edgeTargetId(edge)].sort().join("::")
);

const communityColor = (community: number) => d3.schemeTableau10[community % d3.schemeTableau10.length] ?? "#94a3b8";

const nodeTooltip = (node: CoNode, locale: string) => renderToString(
  <div className="tooltip-table">
    <span>{locale === "zh" ? "菌属" : "Genus"}</span>
    <span><i>{node.id}</i></span>
    <span>{locale === "zh" ? "门" : "Phylum"}</span>
    <span>{node.phylum}</span>
    <span>{locale === "zh" ? "平均丰度" : "Mean abundance"}</span>
    <span>{node.mean_abundance.toFixed(3)}%</span>
    <span>{locale === "zh" ? "检出率" : "Prevalence"}</span>
    <span>{(node.prevalence * 100).toFixed(1)}%</span>
    <span>{locale === "zh" ? "度" : "Degree"}</span>
    <span>{node.degree}</span>
    <span>{locale === "zh" ? "介数中心性" : "Betweenness"}</span>
    <span>{node.betweenness.toFixed(3)}</span>
    <span>{locale === "zh" ? "群落" : "Community"}</span>
    <span>{node.community + 1}</span>
  </div>
);

const edgeTooltip = (edge: CoEdge, locale: string) => renderToString(
  <div className="tooltip-table">
    <span>{locale === "zh" ? "菌属 A" : "Genus A"}</span>
    <span><i>{edgeSourceId(edge)}</i></span>
    <span>{locale === "zh" ? "菌属 B" : "Genus B"}</span>
    <span><i>{edgeTargetId(edge)}</i></span>
    <span>{locale === "zh" ? "相关系数" : "Correlation (r)"}</span>
    <span>{edge.r.toFixed(3)}</span>
    <span>{locale === "zh" ? "校正 p 值" : "Adjusted p-value"}</span>
    <span>{edge.adjusted_p.toFixed(4)}</span>
    <span>{locale === "zh" ? "方法" : "Method"}</span>
    <span>{edge.method}</span>
  </div>
);

const nodeColor = (node: CoNode, colorMode: ColorMode) => (
  colorMode === "community" ? communityColor(node.community) : phylumColor(node.phylum)
);

export function drawCooccurrenceGraph(
  svgEl: SVGSVGElement,
  data: CoData,
  {
    locale,
    colorMode,
    highlightEdgeKeys,
    highlightStroke,
    highlightDasharray,
    height = 700,
  }: {
    locale: string;
    colorMode: ColorMode;
    highlightEdgeKeys?: Set<string>;
    highlightStroke?: string;
    highlightDasharray?: string;
    height?: number;
  },
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const container = svgEl.parentElement;
  const width = container?.clientWidth ?? 960;
  const usableHighlight = highlightEdgeKeys && highlightEdgeKeys.size > 0 ? highlightEdgeKeys : undefined;

  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const nodes: CoNode[] = data.nodes.map((node) => ({ ...node }));
  const edges: CoEdge[] = data.edges.map((edge) => ({ ...edge }));

  const maxAbundance = d3.max(nodes, (node) => node.mean_abundance) ?? 1;
  const radius = d3.scaleSqrt().domain([0, maxAbundance]).range([6, 22]);

  const g = svg.append("g");
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.25, 4])
    .on("zoom", (event) => g.attr("transform", event.transform));
  svg.call(zoom);

  const link = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", (edge) => {
      const highlighted = usableHighlight?.has(cooccurrenceEdgeKey(edge));
      if (highlighted && highlightStroke) return highlightStroke;
      return edge.type === "positive" ? POSITIVE_COLOR : NEGATIVE_COLOR;
    })
    .attr("stroke-opacity", (edge) => {
      if (usableHighlight) return usableHighlight.has(cooccurrenceEdgeKey(edge)) ? 0.95 : 0.18;
      return Math.min(0.82, 0.18 + Math.abs(edge.r) * 0.8);
    })
    .attr("stroke-width", (edge) => {
      const highlighted = usableHighlight?.has(cooccurrenceEdgeKey(edge));
      return highlighted ? Math.max(2.6, Math.abs(edge.r) * 4.5) : Math.max(0.8, Math.abs(edge.r) * 3.2);
    })
    .attr("stroke-dasharray", (edge) => (
      usableHighlight?.has(cooccurrenceEdgeKey(edge)) ? (highlightDasharray ?? null) : null
    ));

  const overlay = g.append("g")
    .selectAll("line")
    .data(edges)
    .join("line")
    .attr("stroke", "transparent")
    .attr("stroke-width", 12)
    .attr("data-tooltip", (edge) => edgeTooltip(edge, locale));

  const node = g.append("g")
    .selectAll("circle")
    .data(nodes)
    .join("circle")
    .attr("r", (d) => radius(d.mean_abundance))
    .attr("fill", (d) => nodeColor(d, colorMode))
    .attr("opacity", 0.92)
    .attr("stroke", (d) => (d.is_hub ? HUB_STROKE : "rgba(255,255,255,0.22)"))
    .attr("stroke-width", (d) => (d.is_hub ? 2.6 : 1))
    .attr("cursor", "pointer")
    .attr("data-tooltip", (d) => nodeTooltip(d, locale))
    .call(
      d3.drag<SVGCircleElement, CoNode>()
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
        }),
    );

  const label = g.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .text((d) => (d.id.length > 28 ? `${d.id.slice(0, 25)}...` : d.id))
    .attr("font-size", 12)
    .attr("font-style", "italic")
    .attr("fill", "currentColor")
    .attr("text-anchor", "middle")
    .attr("dy", (d) => -(radius(d.mean_abundance) + 7))
    .style("pointer-events", "none")
    .style("font-weight", 600)
    .style("text-shadow", "0 1px 4px rgba(0,0,0,0.85)");

  const simulation = d3.forceSimulation(nodes)
    .force(
      "link",
      d3.forceLink<CoNode, CoEdge>(edges)
        .id((d) => d.id)
        .distance((edge) => 150 - Math.abs(edge.r) * 80)
        .strength((edge) => 0.12 + Math.abs(edge.r) * 0.35),
    )
    .force("charge", d3.forceManyBody().strength(-180))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide().radius((d) => radius(d.mean_abundance) + 10))
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
        .attr("cx", (d: any) => d.x)
        .attr("cy", (d: any) => d.y);
      label
        .attr("x", (d: any) => d.x)
        .attr("y", (d: any) => d.y);
    });

  return () => simulation.stop();
}
