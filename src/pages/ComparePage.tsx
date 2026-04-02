/**
 * ComparePage.tsx
 * Differential microbiome analysis between two sample groups
 * 两组样本间的差异微生物组分析页面
 */
import { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import "@/components/tooltip";
import classes from "./ComparePage.module.css";

// ── Types / 类型定义 ──────────────────────────────────────────────────────────

interface GroupFilter {
  country: string;
  disease: string;
  age_group: string;
  sex: string;
}

interface DiffTaxon {
  taxon: string;
  mean_a: number;
  mean_b: number;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  effect_size: number;
}

interface DiffResult {
  summary: {
    group_a_name: string;
    group_b_name: string;
    group_a_n: number;
    group_b_n: number;
    taxonomy_level: string;
    method: string;
    total_taxa: number;
  };
  diff_taxa: DiffTaxon[];
  alpha_diversity: {
    group_a: { shannon: number[]; simpson: number[] };
    group_b: { shannon: number[]; simpson: number[] };
  };
  beta_diversity: {
    pcoa_coords: { x: number; y: number; group: "A" | "B" }[];
  };
}

interface FilterOptions {
  countries: string[];
  diseases: string[];
  age_groups: string[];
  sexes: string[];
}

// ── Constants / 常量 ──────────────────────────────────────────────────────────

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
const TAXONOMY_LEVELS = ["genus", "phylum"] as const;
const METHODS = ["wilcoxon", "t-test"] as const;
type Tab = "bar" | "volcano" | "alpha" | "beta";

// ── Main component / 主组件 ───────────────────────────────────────────────────

const ComparePage = () => {
  const [filterOptions, setFilterOptions] = useState<FilterOptions | null>(null);
  const [filterLoading, setFilterLoading] = useState(true);

  const [groupA, setGroupA] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [groupB, setGroupB] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [taxLevel, setTaxLevel] = useState<"genus" | "phylum">("genus");
  const [method, setMethod] = useState<"wilcoxon" | "t-test">("wilcoxon");

  const [result, setResult] = useState<DiffResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>("bar");

  // Load filter options on mount / 挂载时加载筛选选项
  useEffect(() => {
    fetch(`${API_BASE}/api/filter-options`)
      .then((r) => r.json())
      .then((data: FilterOptions) => {
        setFilterOptions(data);
        setFilterLoading(false);
      })
      .catch(() => {
        setFilterLoading(false);
        setError("无法连接后端 API（请先启动 api/start.bat）");
      });
  }, []);

  const runAnalysis = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch(`${API_BASE}/api/diff-analysis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          group_a_filter: groupA,
          group_b_filter: groupB,
          taxonomy_level: taxLevel,
          method,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail ?? "分析失败");
      }
      const data: DiffResult = await res.json();
      setResult(data);
      setActiveTab("bar");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  // Export result CSV / 导出结果CSV
  const exportCsv = () => {
    if (!result) return;
    const rows = [
      ["Taxon", "Mean_A", "Mean_B", "log2FC", "P_value", "Adjusted_P", "Effect_size"].join(","),
      ...result.diff_taxa.map((t) =>
        [t.taxon, t.mean_a, t.mean_b, t.log2fc, t.p_value, t.adjusted_p, t.effect_size].join(","),
      ),
    ];
    const blob = new Blob([rows.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `diff_analysis_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Export current chart as SVG / 导出当前图表SVG
  const exportSvg = () => {
    const svgEl = document.querySelector<SVGSVGElement>(".compare-chart");
    if (!svgEl) return;
    const blob = new Blob(
      [new XMLSerializer().serializeToString(svgEl)],
      { type: "image/svg+xml" },
    );
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `chart_${activeTab}_${Date.now()}.svg`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className={classes.page}>
      {/* Navigation / 导航栏 */}
      <div className={classes.nav}>
        <Link to="/" className={classes.back}>← Back to Atlas</Link>
        <h1 className={classes.title}>Differential Analysis</h1>
        <span className={classes.subtitle}>Compare gut microbiome composition between two groups</span>
      </div>

      {/* Filter panel / 筛选面板 */}
      <section className={classes.filterSection}>
        {filterLoading ? (
          <p className={classes.hint}>Loading filter options…</p>
        ) : (
          <div className={classes.filterGrid}>
            <GroupFilterPanel
              label="Group A"
              color="var(--secondary)"
              value={groupA}
              onChange={setGroupA}
              options={filterOptions}
            />
            <GroupFilterPanel
              label="Group B"
              color="var(--primary)"
              value={groupB}
              onChange={setGroupB}
              options={filterOptions}
            />
          </div>
        )}

        {/* Method controls / 方法控制 */}
        <div className={classes.controls}>
          <div className={classes.control}>
            <label>Taxonomy level</label>
            <div className={classes.btnGroup}>
              {TAXONOMY_LEVELS.map((l) => (
                <button
                  key={l}
                  className={classes.ctrlBtn}
                  data-active={taxLevel === l}
                  onClick={() => setTaxLevel(l)}
                >
                  {l}
                </button>
              ))}
            </div>
          </div>
          <div className={classes.control}>
            <label>Statistical test</label>
            <div className={classes.btnGroup}>
              {METHODS.map((m) => (
                <button
                  key={m}
                  className={classes.ctrlBtn}
                  data-active={method === m}
                  onClick={() => setMethod(m)}
                >
                  {m}
                </button>
              ))}
            </div>
          </div>
          <button
            className={classes.analyzeBtn}
            onClick={runAnalysis}
            disabled={loading}
          >
            {loading ? "Analyzing…" : "Run Analysis"}
          </button>
        </div>
      </section>

      {/* Error / 错误信息 */}
      {error && <div className={classes.error}>{error}</div>}

      {/* Results / 分析结果 */}
      {result && (
        <section className={classes.resultSection}>
          {/* Summary bar / 摘要信息栏 */}
          <div className={classes.summary}>
            <span>
              <b style={{ color: "var(--secondary)" }}>{result.summary.group_a_name}</b>
              {" "}(n={result.summary.group_a_n})
            </span>
            <span className={classes.vs}>vs</span>
            <span>
              <b style={{ color: "var(--primary)" }}>{result.summary.group_b_name}</b>
              {" "}(n={result.summary.group_b_n})
            </span>
            <span className={classes.meta}>
              {result.summary.total_taxa} {result.summary.taxonomy_level}s · {result.summary.method}
            </span>
          </div>

          {/* Tab bar / 标签栏 */}
          <div className={classes.tabs}>
            {([
              ["bar", "Differential Abundance"],
              ["volcano", "Volcano Plot"],
              ["alpha", "Alpha Diversity"],
              ["beta", "Beta Diversity (PCoA)"],
            ] as [Tab, string][]).map(([id, label]) => (
              <button
                key={id}
                className={classes.tab}
                data-active={activeTab === id}
                onClick={() => setActiveTab(id)}
              >
                {label}
              </button>
            ))}
          </div>

          {/* Chart area / 图表区域 */}
          <div className={classes.chartArea}>
            {activeTab === "bar"    && <DiffBarChart result={result} />}
            {activeTab === "volcano" && <VolcanoChart result={result} />}
            {activeTab === "alpha"  && <AlphaBoxChart result={result} />}
            {activeTab === "beta"   && <BetaPCoAChart result={result} />}
          </div>

          {/* Export buttons / 导出按钮 */}
          <div className={classes.exportRow}>
            <button className={classes.exportBtn} onClick={exportCsv}>⬇ Export CSV</button>
            <button className={classes.exportBtn} onClick={exportSvg}>⬇ Export SVG</button>
          </div>
        </section>
      )}
    </div>
  );
};

// ── Group filter panel sub-component / 组筛选面板子组件 ──────────────────────

const GroupFilterPanel = ({
  label,
  color,
  value,
  onChange,
  options,
}: {
  label: string;
  color: string;
  value: GroupFilter;
  onChange: (f: GroupFilter) => void;
  options: FilterOptions | null;
}) => {
  const set = (key: keyof GroupFilter) => (e: React.ChangeEvent<HTMLSelectElement>) =>
    onChange({ ...value, [key]: e.target.value });

  return (
    <div className={classes.groupPanel}>
      <h3 className={classes.groupLabel} style={{ borderColor: color, color }}>
        {label}
      </h3>
      <div className={classes.fieldRow}>
        <label>Country</label>
        <select value={value.country} onChange={set("country")} className={classes.select}>
          <option value="">— Any —</option>
          {options?.countries.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>Disease</label>
        <select value={value.disease} onChange={set("disease")} className={classes.select}>
          <option value="">— Any —</option>
          {options?.diseases.slice(0, 200).map((d) => (
            <option key={d} value={d}>{d.length > 40 ? d.slice(0, 38) + "…" : d}</option>
          ))}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>Age group</label>
        <select value={value.age_group} onChange={set("age_group")} className={classes.select}>
          <option value="">— Any —</option>
          {options?.age_groups.map((a) => (
            <option key={a} value={a}>{a.replace(/_/g, " ")}</option>
          ))}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>Sex</label>
        <select value={value.sex} onChange={set("sex")} className={classes.select}>
          <option value="">— Any —</option>
          {options?.sexes.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>
    </div>
  );
};

// ── Chart components / 图表组件 ───────────────────────────────────────────────

/** LEfSe-style differential abundance bar chart / 类LEfSe差异丰度柱状图 */
const DiffBarChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Show top 30 significant taxa / 展示前30个显著差异分类群
    const data = result.diff_taxa
      .filter((t) => t.adjusted_p < 0.05)
      .slice(0, 30)
      .sort((a, b) => a.log2fc - b.log2fc);

    if (data.length === 0) {
      svg.attr("viewBox", "0 0 700 100");
      svg.append("text").attr("x", 20).attr("y", 60)
        .text("No significant taxa (adjusted p < 0.05)")
        .attr("fill", "currentColor").attr("font-size", 14);
      return;
    }

    const margin = { top: 20, right: 120, bottom: 40, left: 160 };
    const W = 700 - margin.left - margin.right;
    const H = Math.max(300, data.length * 22);
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xExtent = d3.max(data, (d) => Math.abs(d.log2fc)) ?? 1;
    const xScale = d3.scaleLinear().domain([-xExtent, xExtent]).range([0, W]);
    const yScale = d3.scaleBand().domain(data.map((d) => d.taxon)).range([0, H]).padding(0.2);

    // Color bars by direction / 按方向上色
    g.selectAll(".bar")
      .data(data)
      .join("rect")
      .attr("class", "bar")
      .attr("x", (d) => d.log2fc < 0 ? xScale(d.log2fc) : xScale(0))
      .attr("y", (d) => yScale(d.taxon) ?? 0)
      .attr("width", (d) => Math.abs(xScale(d.log2fc) - xScale(0)))
      .attr("height", yScale.bandwidth())
      .attr("fill", (d) => d.log2fc > 0 ? "var(--secondary)" : "var(--primary)")
      .attr("opacity", 0.85)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Taxon</span><span>{d.taxon}</span>
            <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
            <span>adj.p</span><span>{d.adjusted_p.toExponential(2)}</span>
            <span>Mean A</span><span>{(d.mean_a * 100).toFixed(3)}%</span>
            <span>Mean B</span><span>{(d.mean_b * 100).toFixed(3)}%</span>
          </div>
        )
      );

    // Center axis / 中心轴
    g.append("line")
      .attr("x1", xScale(0)).attr("x2", xScale(0))
      .attr("y1", 0).attr("y2", H)
      .attr("stroke", "currentColor").attr("stroke-width", 1).attr("opacity", 0.4);

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${H})`)
      .call(d3.axisBottom(xScale).ticks(6).tickFormat((d) => `${d}`)  )
      .attr("font-size", 11);
    g.append("g")
      .call(d3.axisLeft(yScale).tickFormat((d) => d.length > 18 ? d.slice(0, 16) + "…" : d))
      .attr("font-size", 11);

    // Group labels / 组标签
    g.append("text").attr("x", xScale(-xExtent / 2)).attr("y", -8)
      .attr("text-anchor", "middle").attr("fill", "var(--primary)")
      .attr("font-size", 12).text(`← ${result.summary.group_b_name}`);
    g.append("text").attr("x", xScale(xExtent / 2)).attr("y", -8)
      .attr("text-anchor", "middle").attr("fill", "var(--secondary)")
      .attr("font-size", 12).text(`${result.summary.group_a_name} →`);

    // Set viewBox / 设置viewBox
    svg.attr("viewBox", `0 0 ${W + margin.left + margin.right} ${H + margin.top + margin.bottom}`);
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

/** Volcano plot / 火山图 */
const VolcanoChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const data = result.diff_taxa;
    const margin = { top: 30, right: 40, bottom: 60, left: 60 };
    const W = 600, H = 400;
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const xExt = d3.max(data, (d) => Math.abs(d.log2fc)) ?? 1;
    const negLogP = data.map((d) => -Math.log10(Math.max(d.adjusted_p, 1e-300)));
    const yMax = d3.max(negLogP) ?? 5;

    const xScale = d3.scaleLinear().domain([-xExt, xExt]).range([0, iW]);
    const yScale = d3.scaleLinear().domain([0, yMax * 1.05]).range([iH, 0]);

    // Color by significance / 按显著性上色
    const getColor = (d: DiffTaxon) => {
      const sig = d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1;
      if (!sig) return "var(--gray)";
      return d.log2fc > 0 ? "var(--secondary)" : "var(--primary)";
    };

    // Threshold lines / 阈值线
    const pThresh = -Math.log10(0.05);
    g.append("line")
      .attr("x1", 0).attr("x2", iW)
      .attr("y1", yScale(pThresh)).attr("y2", yScale(pThresh))
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);
    g.append("line")
      .attr("x1", xScale(-1)).attr("x2", xScale(-1))
      .attr("y1", 0).attr("y2", iH)
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);
    g.append("line")
      .attr("x1", xScale(1)).attr("x2", xScale(1))
      .attr("y1", 0).attr("y2", iH)
      .attr("stroke", "var(--light-gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.6);

    // Points / 散点
    g.selectAll(".dot")
      .data(data)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (d) => xScale(d.log2fc))
      .attr("cy", (_, i) => yScale(negLogP[i]!))
      .attr("r", (d) => d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1 ? 5 : 3)
      .attr("fill", getColor)
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d, i) =>
        renderToString(
          <div className="tooltip-table">
            <span>Taxon</span><span>{d.taxon}</span>
            <span>log2FC</span><span>{d.log2fc.toFixed(3)}</span>
            <span>−log₁₀(adj.p)</span><span>{negLogP[i]!.toFixed(2)}</span>
            <span>adj.p</span><span>{d.adjusted_p.toExponential(2)}</span>
          </div>
        )
      );

    // Labels for top significant points / 为最显著的点添加标签
    const topSig = data
      .filter((d) => d.adjusted_p < 0.05 && Math.abs(d.log2fc) > 1)
      .sort((a, b) => a.adjusted_p - b.adjusted_p)
      .slice(0, 8);

    g.selectAll(".label")
      .data(topSig)
      .join("text")
      .attr("class", "label")
      .attr("x", (d) => xScale(d.log2fc) + 6)
      .attr("y", (d) => yScale(-Math.log10(Math.max(d.adjusted_p, 1e-300))) - 4)
      .attr("font-size", 9)
      .attr("fill", "var(--white)")
      .text((d) => d.taxon.slice(0, 15));

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).ticks(6))
      .attr("font-size", 11);
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);

    // Axis labels / 坐标轴标签
    svg.append("text")
      .attr("x", W / 2).attr("y", H - 10)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("log₂ Fold Change");
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("−log₁₀(adj. p)");
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

/** Alpha diversity boxplots / Alpha多样性箱线图 */
const AlphaBoxChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    // Margins ensure Y-axis always aligns with yScale range bounds
    // margin确保Y轴始终与yScale范围边界对齐
    const margin = { top: 35, right: 20, bottom: 40, left: 50 };
    const W = 620, H = 360;
    const iH = H - margin.top - margin.bottom;   // plot height in group space

    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const { group_a, group_b } = result.alpha_diversity;
    const gA = result.summary.group_a_name;
    const gB = result.summary.group_b_name;

    // All positions are relative to the margin group / 所有x坐标相对于margin组
    const boxW = 80;
    const positions = [
      { x: 60,  data: group_a.shannon, color: "var(--secondary)", label: gA },
      { x: 170, data: group_b.shannon, color: "var(--primary)", label: gB },
      { x: 330, data: group_a.simpson, color: "var(--secondary)", label: gA },
      { x: 440, data: group_b.simpson, color: "var(--primary)", label: gB },
    ];

    const allVals = positions.flatMap((p) => p.data);

    // Guard: no data at all (both groups have zero matched samples)
    // 防御：两组均无匹配样本时显示空状态
    if (allVals.length === 0) {
      svg.attr("viewBox", `0 0 ${W} 100`);
      svg.append("text").attr("x", 20).attr("y", 50)
        .text("No alpha diversity data available")
        .attr("fill", "currentColor").attr("font-size", 13);
      return;
    }

    const yScale = d3.scaleLinear()
      .domain([0, d3.max(allVals) ?? 5])
      .range([iH, 0]);   // group-relative coordinates / 组内坐标

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    // drawBox returns fence values so drawOutliers can reuse them (no duplicate computation)
    // drawBox 返回须线边界值，供 drawOutliers 复用，避免重复计算
    const drawBox = (
      grp: d3.Selection<SVGGElement, unknown, null, undefined>,
      vals: number[],
      cx: number,
      color: string,
    ): { lower: number; upper: number } => {
      const sorted = [...vals].sort((a, b) => a - b);
      const q1 = d3.quantile(sorted, 0.25) ?? 0;
      const median = d3.quantile(sorted, 0.5) ?? 0;
      const q3 = d3.quantile(sorted, 0.75) ?? 0;
      const iqr = q3 - q1;
      const lower = Math.max(sorted[0]!, q1 - 1.5 * iqr);
      const upper = Math.min(sorted[sorted.length - 1]!, q3 + 1.5 * iqr);

      // Box / 箱体
      grp.append("rect")
        .attr("x", cx - boxW / 2).attr("y", yScale(q3))
        .attr("width", boxW).attr("height", Math.abs(yScale(q1) - yScale(q3)))
        .attr("fill", color).attr("opacity", 0.3)
        .attr("stroke", color).attr("stroke-width", 1.5);

      // Median line / 中位线
      grp.append("line")
        .attr("x1", cx - boxW / 2).attr("x2", cx + boxW / 2)
        .attr("y1", yScale(median)).attr("y2", yScale(median))
        .attr("stroke", color).attr("stroke-width", 2.5);

      // Whiskers / 须线
      for (const [y1, y2] of [[yScale(q1), yScale(lower)], [yScale(q3), yScale(upper)]]) {
        grp.append("line")
          .attr("x1", cx).attr("x2", cx)
          .attr("y1", y1!).attr("y2", y2!)
          .attr("stroke", color).attr("stroke-width", 1.5).attr("stroke-dasharray", "3,2");
      }

      return { lower, upper };
    };

    // Draw outlier points beyond whiskers using fence values from drawBox (no recomputation)
    // 使用 drawBox 返回的边界值绘制离群点，无需重复计算
    const drawOutliers = (
      grp: d3.Selection<SVGGElement, unknown, null, undefined>,
      vals: number[], cx: number, color: string,
      lower: number, upper: number,
    ) => {
      const outliers = vals.filter((v) => v < lower || v > upper);
      grp.selectAll(`.out${cx}`)
        .data(outliers)
        .join("circle")
        .attr("class", `out${cx}`)
        .attr("cx", (_, i) => cx + ((i % 3) - 1) * 3)
        .attr("cy", (v) => yScale(v))
        .attr("r", 2.5)
        .attr("fill", "none")
        .attr("stroke", color)
        .attr("opacity", 0.55);
    };

    // Draw boxes and outliers for Shannon (left) and Simpson (right)
    // 绘制Shannon（左）和Simpson（右）的箱线图及离群点
    const fA_sh = drawBox(g, group_a.shannon, 60,  "var(--secondary)");
    drawOutliers(g, group_a.shannon, 60,  "var(--secondary)", fA_sh.lower, fA_sh.upper);
    const fB_sh = drawBox(g, group_b.shannon, 170, "var(--primary)");
    drawOutliers(g, group_b.shannon, 170, "var(--primary)", fB_sh.lower, fB_sh.upper);
    const fA_si = drawBox(g, group_a.simpson, 330, "var(--secondary)");
    drawOutliers(g, group_a.simpson, 330, "var(--secondary)", fA_si.lower, fA_si.upper);
    const fB_si = drawBox(g, group_b.simpson, 440, "var(--primary)");
    drawOutliers(g, group_b.simpson, 440, "var(--primary)", fB_si.lower, fB_si.upper);

    // Y axis inside margin group — always aligned with yScale range / Y轴在margin组内，始终与yScale对齐
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 11).text("Diversity Index");

    // Panel titles (SVG-absolute x = group-relative x + margin.left)
    // 面板标题（SVG绝对x = 组相对x + margin.left）
    svg.append("text").attr("x", margin.left + 115).attr("y", 22)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 13).attr("font-weight", 600).text("Shannon Index");
    svg.append("text").attr("x", margin.left + 385).attr("y", 22)
      .attr("text-anchor", "middle").attr("fill", "currentColor")
      .attr("font-size", 13).attr("font-weight", 600).text("Simpson Index (1-D)");

    // X labels / X轴标签
    for (const pos of positions) {
      svg.append("text")
        .attr("x", pos.x + margin.left).attr("y", margin.top + iH + 18)
        .attr("text-anchor", "middle").attr("fill", pos.color)
        .attr("font-size", 10)
        .text(pos.label.length > 12 ? pos.label.slice(0, 11) + "…" : pos.label);
    }

    // Divider line between panels (group-relative x=250, SVG x=300)
    // 面板分隔线（组相对x=250，SVG x=300）
    svg.append("line")
      .attr("x1", margin.left + 250).attr("x2", margin.left + 250)
      .attr("y1", margin.top).attr("y2", margin.top + iH)
      .attr("stroke", "var(--gray)").attr("stroke-dasharray", "4,3").attr("opacity", 0.5);
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

/** Beta diversity PCoA scatter / Beta多样性PCoA散点图 */
const BetaPCoAChart = ({ result }: { result: DiffResult }) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const coords = result.beta_diversity.pcoa_coords;
    if (!coords.length) {
      svg.attr("viewBox", "0 0 560 100");
      svg.append("text").attr("x", 20).attr("y", 40)
        .text("PCoA data not available").attr("fill", "currentColor").attr("font-size", 14);
      return;
    }

    const W = 560, H = 420;
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const margin = { top: 30, right: 120, bottom: 60, left: 60 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const xExtent = d3.extent(coords, (d) => d.x) as [number, number];
    const yExtent = d3.extent(coords, (d) => d.y) as [number, number];

    const xScale = d3.scaleLinear().domain(xExtent).nice().range([0, iW]);
    const yScale = d3.scaleLinear().domain(yExtent).nice().range([iH, 0]);

    // Draw points / 绘制散点
    g.selectAll(".dot")
      .data(coords)
      .join("circle")
      .attr("class", "dot")
      .attr("cx", (d) => xScale(d.x))
      .attr("cy", (d) => yScale(d.y))
      .attr("r", 5)
      .attr("fill", (d) => d.group === "A" ? "var(--secondary)" : "var(--primary)")
      .attr("opacity", 0.7)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>Group</span>
            <span style={{ color: d.group === "A" ? "var(--secondary)" : "var(--primary)" }}>
              {d.group === "A" ? result.summary.group_a_name : result.summary.group_b_name}
            </span>
            <span>PC1</span><span>{d.x.toFixed(4)}</span>
            <span>PC2</span><span>{d.y.toFixed(4)}</span>
          </div>
        )
      );

    // Axes / 坐标轴
    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).ticks(5)).attr("font-size", 11);
    g.append("g").call(d3.axisLeft(yScale).ticks(5)).attr("font-size", 11);

    // Axis labels / 坐标轴标签
    svg.append("text").attr("x", W / 2).attr("y", H - 8)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("PC1 (Bray-Curtis)");
    svg.append("text")
      .attr("transform", `translate(14,${H / 2}) rotate(-90)`)
      .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 12)
      .text("PC2");

    // Legend / 图例
    const lx = iW + 10;
    svg.append("circle").attr("cx", margin.left + lx + 6).attr("cy", margin.top + 20).attr("r", 6)
      .attr("fill", "var(--secondary)");
    svg.append("text").attr("x", margin.left + lx + 16).attr("y", margin.top + 24)
      .attr("font-size", 11).attr("fill", "var(--secondary)")
      .text(result.summary.group_a_name.slice(0, 12));
    svg.append("circle").attr("cx", margin.left + lx + 6).attr("cy", margin.top + 44).attr("r", 6)
      .attr("fill", "var(--primary)");
    svg.append("text").attr("x", margin.left + lx + 16).attr("y", margin.top + 48)
      .attr("font-size", 11).attr("fill", "var(--primary)")
      .text(result.summary.group_b_name.slice(0, 12));
  }, [result]);

  return <svg ref={svgRef} className={`compare-chart ${classes.chart}`} />;
};

export default ComparePage;
