/**
 * LollipopPanel.tsx — Lollipop 差异丰度面板（嵌入 DiseasePage Tab）
 * log2FC + 显著性 + 门级着色
 */
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import classes from "../DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

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

interface Props {
  disease: string;
}

const LollipopPanel = ({ disease }: Props) => {
  const { t, locale } = useI18n();
  const [data, setData] = useState<LollipopItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  // 选病后自动请求数据
  useEffect(() => {
    if (!disease) return;
    setLoading(true);
    setData([]);
    setError(null);
    fetch(`${API_BASE}/api/lollipop-data?disease=${encodeURIComponent(disease)}`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(d => setData(d.data ?? []))
      .catch((err) => {
        console.error("Lollipop API error:", err);
        setError(locale === "zh" ? "后端未启动或连接失败" : "Backend not available or connection failed");
      })
      .finally(() => setLoading(false));
  }, [disease, locale]);

  // 绘制 Lollipop 图
  useEffect(() => {
    if (!svgRef.current || data.length === 0) return;
    drawLollipop(svgRef.current, data);
  }, [data]);

  const exportLollipopCsv = () => {
    if (!data.length) return;
    exportTable(
      data.map((d) => ({
        Genus: d.genus,
        Phylum: d.phylum,
        Log2FC: d.log2fc,
        P_value: d.p_value,
        Mean_Disease: d.mean_disease,
        Mean_Control: d.mean_control,
      })),
      `lollipop_${disease}_${Date.now()}`,
    );
  };

  const exportLollipopChart = (type: "svg" | "png") => {
    const svg = svgRef.current;
    if (!svg) return;
    type === "svg"
      ? exportSVG(svg, `lollipop_${disease}_${Date.now()}`)
      : exportPNG(svg, `lollipop_${disease}_${Date.now()}`);
  };

  const phyla = [...new Set(data.map(d => d.phylum))];

  return (
    <div>
      {/* 加载状态 */}
      {loading && <div className={classes.loading}>{t("biomarker.running")}</div>}

      {/* 错误信息 */}
      {error && <div className={classes.errorMsg}>{error}</div>}

      {/* Lollipop 图 */}
      {data.length > 0 && (
        <div className={classes.chartCard}>
          <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.5rem" }}>
            <button onClick={exportLollipopCsv} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
            <button onClick={() => exportLollipopChart("svg")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
            <button onClick={() => exportLollipopChart("png")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
          </div>
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

export default LollipopPanel;

// ── Lollipop 绘图函数 ────────────────────────────────────────────────────────

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

  // 零线
  g.append("line")
    .attr("x1", xScale(0)).attr("x2", xScale(0))
    .attr("y1", 0).attr("y2", iH)
    .attr("stroke", "rgba(255,255,255,0.3)");

  // 棒棒糖棍
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

  // 棒棒糖头
  g.selectAll(".dot")
    .data(sorted)
    .join("circle")
    .attr("cx", d => xScale(d.log2fc))
    .attr("cy", d => (yScale(d.genus) ?? 0) + yScale.bandwidth() / 2)
    .attr("r", d => rScale(d.neg_log10p))
    .attr("fill", d => PHYLUM_COLORS[d.phylum] ?? DEFAULT_COLOR)
    .attr("opacity", 0.85);

  // Y轴
  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat(d => d.length > 18 ? d.slice(0, 16) + "\u2026" : d))
    .attr("font-size", 9)
    .selectAll("text").attr("font-style", "italic");

  // X轴
  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5))
    .attr("font-size", 9);

  g.append("text")
    .attr("x", iW / 2).attr("y", iH + 25)
    .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 10)
    .text("Log\u2082 Fold Change");
}
