/**
 * SankeyChart.tsx Taxonomy composition flow: Phylum → Genus (Top 20) with
 * interactive highlighting
 */
import { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { useNavigate } from "react-router-dom";
import * as d3 from "d3";
import { useData } from "@/data";
import { useI18n } from "@/i18n";
import { getCssVariable } from "@/util/dom";
import "@/components/tooltip";

// ── Types ───────────────────────────────────────────────────────────────────

interface SankeyNode {
  id: string;
  label: string;
  level: "phylum" | "genus";
  x: number;
  y: number;
  dy: number; // height
  value: number;
}

interface SankeyLink {
  source: string;
  target: string;
  value: number;
  sy: number; // source y offset
  ty: number; // target y offset
}

// Phylum → Genus mapping (from full taxonomy in abundance data)
const PHYLUM_MAP: Record<string, string> = {
  Bacteroides: "Bacteroidota",
  Parabacteroides: "Bacteroidota",
  Alistipes: "Bacteroidota",
  Segatella: "Bacteroidota",
  Bifidobacterium: "Actinomycetota",
  Collinsella: "Actinomycetota",
  Faecalibacterium: "Bacillota",
  Blautia: "Bacillota",
  Roseburia: "Bacillota",
  Ruminococcus: "Bacillota",
  Agathobacter: "Bacillota",
  Gemmiger: "Bacillota",
  Anaerostipes: "Bacillota",
  Dialister: "Bacillota",
  Clostridium: "Bacillota",
  Enterococcus: "Bacillota",
  Streptococcus: "Bacillota",
  Staphylococcus: "Bacillota",
  Veillonella: "Bacillota",
  Mediterraneibacter: "Bacillota",
  Fusicatenibacter: "Bacillota",
  Dorea: "Bacillota",
  Thomasclavelia: "Bacillota",
  Anaerobutyricum: "Bacillota",
  Enterocloster: "Bacillota",
  Phascolarctobacterium: "Bacillota",
  Shigella: "Pseudomonadota",
  Klebsiella: "Pseudomonadota",
  Haemophilus: "Pseudomonadota",
  Akkermansia: "Verrucomicrobiota",
};

// Phylum colors
const PHYLUM_COLORS: Record<string, string> = {
  Bacillota: "#e23fff",
  Bacteroidota: "#6ec1e4",
  Pseudomonadota: "#ff6b6b",
  Actinomycetota: "#ffd93d",
  Verrucomicrobiota: "#6bcb77",
};

// ── Component ───────────────────────────────────────────────────────────────

const SankeyChart = () => {
  const { t } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const navigate = useNavigate();
  const navigateRef = useRef(navigate);
  const abundance = useData((s) => s.abundance);
  const [highlight, setHighlight] = useState<string | null>(null);

  useEffect(() => {
    navigateRef.current = navigate;
  }, [navigate]);

  useEffect(() => {
    if (!svgRef.current || !abundance) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    const phylumMap = abundance.phylum_map ?? PHYLUM_MAP;

    // ── Build Sankey data ─────────────────────────────────────────────────
    const genera = abundance.genera;
    // Calculate average abundance across all conditions
    const genusAbundance: Record<string, number> = {};
    for (const g of genera) {
      const vals = Object.values(abundance.by_disease).map((d) => d[g] ?? 0);
      genusAbundance[g] = d3.mean(vals) ?? 0;
    }

    // Get top 20 genera by abundance
    const topGenera = genera
      .filter((g) => phylumMap[g])
      .sort((a, b) => (genusAbundance[b] ?? 0) - (genusAbundance[a] ?? 0))
      .slice(0, 20);

    // Aggregate by phylum
    const phylumTotals: Record<string, number> = {};
    for (const g of topGenera) {
      const p = phylumMap[g] ?? "Other";
      phylumTotals[p] = (phylumTotals[p] ?? 0) + (genusAbundance[g] ?? 0);
    }

    const phyla = Object.entries(phylumTotals)
      .sort((a, b) => b[1] - a[1])
      .map(([name]) => name);

    // ── Layout dimensions ─────────────────────────────────────────────────
    const W = 900;
    const H = 620;
    const margin = { top: 50, right: 180, bottom: 50, left: 150 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const nodeW = 14;
    const nodePad = 1;

    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const g = svg
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // ── Compute node positions ────────────────────────────────────────────
    const totalValue = d3.sum(Object.values(phylumTotals));

    // Left side: phyla
    const phylumNodes: SankeyNode[] = [];
    let py = 0;
    for (const p of phyla) {
      const val = phylumTotals[p];
      const dy = Math.max((val / totalValue) * iH, 10);
      phylumNodes.push({
        id: `p_${p}`,
        label: p,
        level: "phylum",
        x: 0,
        y: py,
        dy,
        value: val,
      });
      py += dy + nodePad;
    }
    // Center vertically
    const phylumH = py - nodePad;
    const phylumOffset = (iH - phylumH) / 2;
    phylumNodes.forEach((n) => (n.y += phylumOffset));

    // Right side: genera
    const genusNodes: SankeyNode[] = [];
    let gy = 0;
    // Sort genera by phylum then abundance
    const sortedGenera = [...topGenera].sort((a, b) => {
      const pi = phyla.indexOf(phylumMap[a]);
      const pj = phyla.indexOf(phylumMap[b]);
      if (pi !== pj) return pi - pj;
      return (genusAbundance[b] ?? 0) - (genusAbundance[a] ?? 0);
    });

    for (const gen of sortedGenera) {
      const val = genusAbundance[gen] ?? 0;
      const dy = Math.max((val / totalValue) * iH, 8);
      genusNodes.push({
        id: `g_${gen}`,
        label: gen,
        level: "genus",
        x: iW - nodeW,
        y: gy,
        dy,
        value: val,
      });
      gy += dy + nodePad;
    }
    const genusH = gy - nodePad;
    const genusOffset = (iH - genusH) / 2;
    genusNodes.forEach((n) => (n.y += genusOffset));

    // ── Build links ──────────────────────────────────────────────────────
    // Track source/target offsets for stacking links
    const phylumOffsets: Record<string, number> = {};
    phylumNodes.forEach((n) => (phylumOffsets[n.id] = 0));
    const genusOffsets: Record<string, number> = {};
    genusNodes.forEach((n) => (genusOffsets[n.id] = 0));

    const links: SankeyLink[] = [];
    for (const gen of sortedGenera) {
      const p = phylumMap[gen];
      const pNode = phylumNodes.find((n) => n.label === p)!;
      const gNode = genusNodes.find((n) => n.label === gen)!;
      const val = genusAbundance[gen] ?? 0;
      const linkH = Math.max((val / totalValue) * iH, 2);

      const sy = pNode.y + (phylumOffsets[pNode.id] ?? 0);
      const ty = gNode.y + (genusOffsets[gNode.id] ?? 0);

      links.push({ source: pNode.id, target: gNode.id, value: val, sy, ty });
      phylumOffsets[pNode.id] = (phylumOffsets[pNode.id] ?? 0) + linkH;
      genusOffsets[gNode.id] = (genusOffsets[gNode.id] ?? 0) + linkH;
    }

    // ── Draw links (curved paths) ─────────────────────────────────────────
    const linkGroup = g.append("g").attr("class", "links");
    linkGroup
      .selectAll("path")
      .data(links)
      .join("path")
      .attr("d", (d) => {
        const pNode = phylumNodes.find((n) => n.id === d.source)!;
        const gNode = genusNodes.find((n) => n.id === d.target)!;
        const linkH = Math.max((d.value / totalValue) * iH, 2);
        const x0 = pNode.x + nodeW;
        const x1 = gNode.x;
        const xm = (x0 + x1) / 2;
        return `M${x0},${d.sy} C${xm},${d.sy} ${xm},${d.ty} ${x1},${d.ty}
                L${x1},${d.ty + linkH} C${xm},${d.ty + linkH} ${xm},${d.sy + linkH} ${x0},${d.sy + linkH} Z`;
      })
      .attr("fill", (d) => {
        const pLabel = phylumNodes.find((n) => n.id === d.source)?.label ?? "";
        return PHYLUM_COLORS[pLabel] ?? "#666";
      })
      .attr("opacity", 0.35)
      .attr("role", "graphics-symbol")
      .attr(
        "data-phylum",
        (d) => phylumNodes.find((n) => n.id === d.source)?.label ?? "",
      )
      .attr(
        "data-genus",
        (d) => genusNodes.find((n) => n.id === d.target)?.label ?? "",
      )
      .attr("data-tooltip", (d) => {
        const pLabel = phylumNodes.find((n) => n.id === d.source)?.label ?? "";
        const gLabel = genusNodes.find((n) => n.id === d.target)?.label ?? "";
        return renderToString(
          <div className="tooltip-table">
            <span>Phylum</span>
            <span>{pLabel}</span>
            <span>Genus</span>
            <span>
              <i>{gLabel}</i>
            </span>
            <span>Avg. Abundance</span>
            <span>{(d.value * 100).toFixed(3)}%</span>
          </div>,
        );
      })
      .on("mouseenter", function (_, d) {
        const gLabel = genusNodes.find((n) => n.id === d.target)?.label ?? "";
        setHighlight(gLabel);
      })
      .on("mouseleave", () => setHighlight(null));

    // ── Draw phylum nodes ────────────────────────────────────────────────
    g.selectAll(".phylum-node")
      .data(phylumNodes)
      .join("rect")
      .attr("class", "phylum-node")
      .attr("x", (d) => d.x)
      .attr("y", (d) => d.y)
      .attr("width", nodeW)
      .attr("height", (d) => d.dy)
      .attr("fill", (d) => PHYLUM_COLORS[d.label] ?? "#666")
      .attr("opacity", 0.9)
      .attr("data-phylum", (d) => d.label)
      .attr("rx", 3)
      .style("cursor", "pointer")
      .on("mouseenter", (_, d) => setHighlight(d.label))
      .on("mouseleave", () => setHighlight(null));

    // Phylum labels
    g.selectAll(".phylum-label")
      .data(phylumNodes)
      .join("text")
      .attr("class", "phylum-label")
      .attr("x", -8)
      .attr("y", (d) => d.y + d.dy / 2)
      .attr("text-anchor", "end")
      .attr("dominant-baseline", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 11)
      .attr("font-weight", 600)
      .attr("opacity", 1)
      .attr("data-phylum", (d) => d.label)
      .text((d) => d.label);

    // ── Draw genus nodes ─────────────────────────────────────────────────
    g.selectAll(".genus-node")
      .data(genusNodes)
      .join("rect")
      .attr("class", "genus-node")
      .attr("x", (d) => d.x)
      .attr("y", (d) => d.y)
      .attr("width", nodeW)
      .attr("height", (d) => d.dy)
      .attr("fill", (d) => PHYLUM_COLORS[phylumMap[d.label] ?? ""] ?? "#666")
      .attr("opacity", 0.9)
      .attr("data-phylum", (d) => phylumMap[d.label] ?? "")
      .attr("data-genus", (d) => d.label)
      .attr("rx", 3)
      .style("cursor", "pointer")
      .on("mouseenter", (_, d) => setHighlight(d.label))
      .on("mouseleave", () => setHighlight(null));

    // Keep labels readable when small nodes are closer than the text height.
    const labelGap = 12;
    const genusLabelY = new Map<string, number>();
    let previousLabelY = -Infinity;
    for (const node of genusNodes) {
      const targetY = node.y + node.dy / 2;
      const labelY = Math.max(targetY, previousLabelY + labelGap);
      genusLabelY.set(node.id, labelY);
      previousLabelY = labelY;
    }
    const maxLabelY = iH - 4;
    const overflow = previousLabelY - maxLabelY;
    if (overflow > 0) {
      genusLabelY.forEach((y, id) => genusLabelY.set(id, y - overflow));
    }

    // Genus labels
    g.selectAll(".genus-label")
      .data(genusNodes)
      .join("text")
      .attr("class", "genus-label")
      .attr("x", (d) => d.x + nodeW + 6)
      .attr("y", (d) => genusLabelY.get(d.id) ?? d.y + d.dy / 2)
      .attr("dominant-baseline", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", 10)
      .attr("font-style", "italic")
      .attr("opacity", 1)
      .attr("data-phylum", (d) => phylumMap[d.label] ?? "")
      .attr("data-genus", (d) => d.label)
      .style("cursor", "pointer")
      .style("text-decoration", "underline")
      .style("text-decoration-color", getCssVariable("--accent"))
      .on("mouseenter", (_, d) => setHighlight(d.label))
      .on("mouseleave", () => setHighlight(null))
      .on("click", (_, d) => navigateRef.current(`/species/${d.label}`))
      .text((d) => `${d.label} (${(d.value * 100).toFixed(2)}%)`);

    // ── Column headers ───────────────────────────────────────────────────
    g.append("text")
      .attr("x", nodeW / 2)
      .attr("y", -8)
      .attr("text-anchor", "middle")
      .attr("fill", "var(--light-gray)")
      .attr("font-size", 12)
      .attr("font-weight", 700)
      .text("Phylum");
    g.append("text")
      .attr("x", iW - nodeW / 2)
      .attr("y", -8)
      .attr("text-anchor", "middle")
      .attr("fill", "var(--light-gray)")
      .attr("font-size", 12)
      .attr("font-weight", 700)
      .text("Genus");

    return () => {
      svg.selectAll("[data-tooltip]").each(function () {
        const element = this as Element & { _tippy?: { destroy: () => void } };
        element._tippy?.destroy();
      });
      svg.selectAll("*").remove();
    };
  }, [abundance]);

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;
    const root = d3.select(svg);
    const isActive = (phylum: string, genus?: string) =>
      !highlight || highlight === phylum || highlight === genus;

    root
      .selectAll<SVGPathElement, unknown>(".links path")
      .attr("opacity", function () {
        const path = this as SVGPathElement;
        const phylum = path.getAttribute("data-phylum") ?? "";
        const genus = path.getAttribute("data-genus") ?? "";
        return !highlight ? 0.35 : isActive(phylum, genus) ? 0.7 : 0.08;
      });
    root
      .selectAll<SVGRectElement, unknown>(".phylum-node")
      .attr("opacity", function () {
        const phylum = this.getAttribute("data-phylum") ?? "";
        return !highlight || highlight === phylum ? 0.9 : 0.3;
      });
    root
      .selectAll<SVGTextElement, unknown>(".phylum-label")
      .attr("opacity", function () {
        const phylum = this.getAttribute("data-phylum") ?? "";
        return !highlight || highlight === phylum ? 1 : 0.3;
      });
    root
      .selectAll<SVGRectElement, unknown>(".genus-node")
      .attr("opacity", function () {
        const phylum = this.getAttribute("data-phylum") ?? "";
        const genus = this.getAttribute("data-genus") ?? "";
        return !highlight || isActive(phylum, genus) ? 0.9 : 0.3;
      });
    root
      .selectAll<SVGTextElement, unknown>(".genus-label")
      .attr("opacity", function () {
        const phylum = this.getAttribute("data-phylum") ?? "";
        const genus = this.getAttribute("data-genus") ?? "";
        return !highlight || isActive(phylum, genus) ? 1 : 0.3;
      });
  }, [highlight]);

  if (!abundance) return null;

  return (
    <div className="sub-section" style={{ marginTop: "1.5rem" }}>
      <h3>{t("sankey.title")}</h3>
      <p
        style={{
          color: "var(--light-gray)",
          fontSize: "0.85rem",
          margin: "0.3rem 0 0.8rem",
        }}
      >
        Hover to highlight connections. Width proportional to mean relative
        abundance.
      </p>
      <svg
        ref={svgRef}
        className="chart compare-chart"
        style={{ width: "100%", maxWidth: 880 }}
      />
    </div>
  );
};

export default SankeyChart;
