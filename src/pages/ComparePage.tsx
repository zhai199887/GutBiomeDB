/**
 * ComparePage.tsx
 * Differential microbiome analysis between two sample groups
 * 两组样本间的差异微生物组分析页面（主组件，图表已拆分为子组件）
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import "@/components/tooltip";
import classes from "./ComparePage.module.css";

// Sub-components / 子组件
import GroupFilterPanel from "./compare/GroupFilterPanel";
import DiffBarChart from "./compare/DiffBarChart";
import VolcanoChart from "./compare/VolcanoChart";
import AlphaBoxChart from "./compare/AlphaBoxChart";
import BetaPCoAChart from "./compare/BetaPCoAChart";

// Types & constants / 类型与常量
import type { GroupFilter, DiffResult, FilterOptions, Tab } from "./compare/types";
import { API_BASE, TAXONOMY_LEVELS, METHODS } from "./compare/types";

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
        setError("Differential analysis backend is under development. Stay tuned! / 差异分析后端正在开发中，敬请期待！");
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
        throw new Error(err.detail ?? "Analysis failed");
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
            {activeTab === "bar"     && <DiffBarChart result={result} />}
            {activeTab === "volcano" && <VolcanoChart result={result} />}
            {activeTab === "alpha"   && <AlphaBoxChart result={result} />}
            {activeTab === "beta"    && <BetaPCoAChart result={result} />}
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

export default ComparePage;
