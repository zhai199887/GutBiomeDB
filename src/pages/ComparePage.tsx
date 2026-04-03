/**
 * ComparePage.tsx
 * Differential microbiome analysis between two sample groups
 * 两组样本间的差异微生物组分析页面（主组件，图表已拆分为子组件）
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
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
  const { t } = useI18n();
  const [filterOptions, setFilterOptions] = useState<FilterOptions | null>(null);
  const [filterLoading, setFilterLoading] = useState(true);

  const [groupA, setGroupA] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [groupB, setGroupB] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [taxLevel, setTaxLevel] = useState<"genus" | "phylum">("genus");
  const [method, setMethod] = useState<string>("wilcoxon");

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
        setError(t("compare.backendError"));
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

  // All available methods / 所有可选统计方法
  const ALL_METHODS = ["wilcoxon", "t-test", "lefse", "permanova"] as const;

  // Tabs including LEfSe & PERMANOVA / 包含 LEfSe 和 PERMANOVA 的标签
  const TABS: [Tab, string][] = [
    ["bar", t("compare.tab.bar")],
    ["volcano", t("compare.tab.volcano")],
    ["alpha", t("compare.tab.alpha")],
    ["beta", t("compare.tab.beta")],
  ];

  // Add LEfSe and PERMANOVA tabs when those methods are selected
  if (result?.lefse_results) {
    TABS.push(["lefse", t("compare.tab.lefse")]);
  }
  if (result?.permanova) {
    TABS.push(["permanova", t("compare.tab.permanova")]);
  }

  return (
    <div className={classes.page}>
      {/* Navigation / 导航栏 */}
      <div className={classes.nav}>
        <Link to="/" className={classes.back}>{t("compare.back")}</Link>
        <h1 className={classes.title}>{t("compare.title")}</h1>
        <span className={classes.subtitle}>{t("compare.subtitle")}</span>
      </div>

      {/* Filter panel / 筛选面板 */}
      <section className={classes.filterSection}>
        {filterLoading ? (
          <p className={classes.hint}>{t("compare.loading")}</p>
        ) : (
          <div className={classes.filterGrid}>
            <GroupFilterPanel
              label={t("compare.groupA")}
              color="var(--secondary)"
              value={groupA}
              onChange={setGroupA}
              options={filterOptions}
            />
            <GroupFilterPanel
              label={t("compare.groupB")}
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
            <label>{t("compare.taxLevel")}</label>
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
            <label>{t("compare.statTest")}</label>
            <div className={classes.btnGroup}>
              {ALL_METHODS.map((m) => (
                <button
                  key={m}
                  className={classes.ctrlBtn}
                  data-active={method === m}
                  onClick={() => setMethod(m)}
                >
                  {m === "lefse" ? "LEfSe" : m === "permanova" ? "PERMANOVA" : m}
                </button>
              ))}
            </div>
          </div>
          <button
            className={classes.analyzeBtn}
            onClick={runAnalysis}
            disabled={loading}
          >
            {loading ? t("compare.analyzing") : t("compare.run")}
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
            <span className={classes.vs}>{t("compare.vs")}</span>
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
            {TABS.map(([id, label]) => (
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
            {activeTab === "lefse"   && <LefseResults result={result} />}
            {activeTab === "permanova" && <PermanovaResults result={result} />}
          </div>

          {/* Export buttons / 导出按钮 */}
          <div className={classes.exportRow}>
            <button className={classes.exportBtn} onClick={exportCsv}>{t("compare.export.csv")}</button>
            <button className={classes.exportBtn} onClick={exportSvg}>{t("compare.export.svg")}</button>
          </div>
        </section>
      )}
    </div>
  );
};

// ── LEfSe results display / LEfSe 结果展示 ──────────────────────────────────

const LefseResults = ({ result }: { result: DiffResult }) => {
  if (!result.lefse_results || result.lefse_results.length === 0) {
    return <p style={{ color: "var(--light-gray)", padding: "2rem" }}>No significant LEfSe features found.</p>;
  }

  const maxLda = Math.max(...result.lefse_results.map((r) => Math.abs(r.lda_score)));

  return (
    <div style={{ padding: "1rem" }}>
      <p style={{ color: "var(--light-gray)", fontSize: "0.85rem", marginBottom: "1rem" }}>
        LEfSe (LDA Effect Size) — features with LDA score ≥ 2.0 and Kruskal-Wallis p &lt; 0.05
      </p>
      <svg className="compare-chart" viewBox={`0 0 700 ${Math.max(200, result.lefse_results.length * 24 + 40)}`}
        style={{ width: "100%", maxWidth: 700 }}>
        {result.lefse_results.map((feat, i) => {
          const barWidth = (Math.abs(feat.lda_score) / maxLda) * 400;
          const isGroupA = feat.enriched_group === "A";
          const color = isGroupA ? "var(--secondary)" : "var(--primary)";
          return (
            <g key={feat.taxon} transform={`translate(200, ${i * 24 + 10})`}>
              <text x={-5} y={14} textAnchor="end" fill="currentColor" fontSize={11}>
                {feat.taxon.length > 22 ? feat.taxon.slice(0, 20) + "…" : feat.taxon}
              </text>
              <rect x={0} y={2} width={barWidth} height={18} fill={color} opacity={0.8} rx={2} />
              <text x={barWidth + 5} y={15} fill="currentColor" fontSize={10}>
                {feat.lda_score.toFixed(2)}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
};

// ── PERMANOVA results display / PERMANOVA 结果展示 ───────────────────────────

const PermanovaResults = ({ result }: { result: DiffResult }) => {
  if (!result.permanova) {
    return <p style={{ color: "var(--light-gray)", padding: "2rem" }}>No PERMANOVA results available.</p>;
  }

  const p = result.permanova;
  const sigStyle = { color: p.p_value < 0.05 ? "var(--primary)" : "var(--light-gray)" };

  return (
    <div style={{ padding: "2rem" }}>
      <h3 style={{ marginBottom: "1rem" }}>PERMANOVA Results</h3>
      <p style={{ color: "var(--light-gray)", fontSize: "0.85rem", marginBottom: "1.5rem" }}>
        Permutational Multivariate Analysis of Variance — tests whether group centroids differ
        in Bray-Curtis distance space ({p.permutations} permutations)
      </p>
      <table style={{ borderCollapse: "collapse", width: "100%", maxWidth: 500 }}>
        <tbody>
          {[
            ["F-statistic (pseudo-F)", p.f_statistic.toFixed(4)],
            ["p-value", <span style={sigStyle}>{p.p_value.toFixed(4)}{p.p_value < 0.05 ? " *" : ""}</span>],
            ["R² (effect size)", p.r_squared.toFixed(4)],
            ["Permutations", String(p.permutations)],
            ["Samples (A)", String(p.n_a)],
            ["Samples (B)", String(p.n_b)],
          ].map(([label, val], i) => (
            <tr key={i} style={{ borderBottom: "1px solid var(--gray)" }}>
              <td style={{ padding: "0.5rem 1rem", color: "var(--light-gray)" }}>{label}</td>
              <td style={{ padding: "0.5rem 1rem", fontWeight: 600 }}>{val}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p style={{ color: "var(--light-gray)", fontSize: "0.8rem", marginTop: "1rem" }}>
        {p.p_value < 0.05
          ? "The two groups have significantly different microbiome compositions (p < 0.05)."
          : "No significant difference in microbiome composition between the two groups."}
        {" "}R² = {(p.r_squared * 100).toFixed(1)}% of variation explained by grouping.
      </p>
    </div>
  );
};

export default ComparePage;
