/**
 * PhenotypePage — Phenotype Association Analysis
 * 表型关联分析页面（容器组件）
 *
 * Architecture:
 *   PhenotypeControls → dimension/group/tax-level selectors (with n= sample counts)
 *   PhenotypeStats    → 4-card result summary
 *   View tabs:
 *     ButterflyBarChart  → phylum-colored symmetric bar + significance markers
 *     LollipopChart      → log2FC vs -log10(p), size=prevalence, color=phylum
 *     PrevalenceAbundancePlot → prevalence vs abundance scatter
 *   PhenotypeResultTable → sortable/filterable full results table
 *   PhenotypeExport      → CSV / SVG / PNG export
 */
import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";

import PhenotypeControls from "@/pages/phenotype/PhenotypeControls";
import PhenotypeStats from "@/pages/phenotype/PhenotypeStats";
import ButterflyBarChart from "@/pages/phenotype/charts/ButterflyBarChart";
import LollipopChart from "@/pages/phenotype/charts/LollipopChart";
import PrevalenceAbundancePlot from "@/pages/phenotype/charts/PrevalenceAbundancePlot";
import PhenotypeResultTable from "@/pages/phenotype/PhenotypeResultTable";
import PhenotypeExport from "@/pages/phenotype/PhenotypeExport";

import {
  API_BASE,
  type DimType, type TaxLevel, type ViewMode,
  type PhenotypeAssociationResponse,
} from "@/pages/phenotype/types";

const PhenotypePage = () => {
  const { t, locale } = useI18n();

  // ── Control state ─────────────────────────────────────────────────────────
  const [dimType, setDimType] = useState<DimType>("sex");
  const [groupA, setGroupA] = useState("female");
  const [groupB, setGroupB] = useState("male");
  const [taxLevel, setTaxLevel] = useState<TaxLevel>("genus");
  const [minPrevalence, setMinPrevalence] = useState(0.10);

  // ── Result state ──────────────────────────────────────────────────────────
  const [result, setResult] = useState<PhenotypeAssociationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>("butterfly");
  const [showOnlySig, setShowOnlySig] = useState(false);

  const labelOf = useCallback((g: string) => {
    if (dimType === "disease") return diseaseDisplayNameI18n(g, locale);
    if (locale === "zh") {
      if (dimType === "age") return AGE_GROUP_ZH[g] ?? g.replace(/_/g, " ");
      if (dimType === "sex") return SEX_ZH[g] ?? g;
    }
    return g.replace(/_/g, " ");
  }, [dimType, locale]);

  // ── Dimension change: reset groups ───────────────────────────────────────
  const handleDimChange = (d: DimType) => {
    setDimType(d);
    setResult(null);
    setError(null);
    if (d === "sex") { setGroupA("female"); setGroupB("male"); }
    else if (d === "age") { setGroupA("Adult"); setGroupB("Older_Adult"); }
    else { setGroupA("UC"); setGroupB("CD"); }
  };

  // ── Run analysis ─────────────────────────────────────────────────────────
  const handleAnalyze = async () => {
    if (groupA === groupB) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const params = new URLSearchParams({
        dim_type: dimType,
        group_a: groupA,
        group_b: groupB,
        tax_level: taxLevel,
        min_prevalence: String(minPrevalence),
        top_n: "100",
      });
      const res = await fetch(`${API_BASE}/api/phenotype-association?${params}`);
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail ?? res.statusText);
      }
      const data: PhenotypeAssociationResponse = await res.json();
      setResult(data);
      setViewMode("butterfly");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const viewTabs: { key: ViewMode; label: string }[] = [
    { key: "butterfly",   label: t("phenotype.viewMode.butterfly") },
    { key: "lollipop",    label: t("phenotype.viewMode.lollipop") },
    { key: "prevalence",  label: t("phenotype.viewMode.prevalenceAbundance") },
  ];

  const chartSvgId = "phenotype-main-chart";

  return (
    <div style={{ padding: "2rem", maxWidth: "1200px", margin: "0 auto" }}>
      {/* Back link */}
      <div style={{ marginBottom: "1.2rem" }}>
        <Link to="/" style={{ color: "var(--primary)", textDecoration: "none" }}>
          {t("phenotype.back")}
        </Link>
      </div>

      {/* Title */}
      <h1 style={{ fontSize: "1.6rem", marginBottom: "0.4rem" }}>
        {t("phenotype.title")}
      </h1>
      <p style={{ color: "var(--light-gray)", marginBottom: "1.8rem", fontSize: "0.9rem" }}>
        {t("phenotype.subtitle")}
      </p>

      {/* Controls */}
      <PhenotypeControls
        dimType={dimType}
        groupA={groupA}
        groupB={groupB}
        taxLevel={taxLevel}
        minPrevalence={minPrevalence}
        loading={loading}
        onDimChange={handleDimChange}
        onGroupAChange={setGroupA}
        onGroupBChange={setGroupB}
        onTaxLevelChange={setTaxLevel}
        onMinPrevalenceChange={setMinPrevalence}
        onAnalyze={handleAnalyze}
      />

      {/* Loading state */}
      {loading && (
        <div style={{ textAlign: "center", padding: "3rem", color: "var(--light-gray)" }}>
          <div style={{ fontSize: "1.1rem", marginBottom: "0.5rem" }}>
            {t("phenotype.loading.analysis")}
          </div>
          <div style={{ fontSize: "0.8rem" }}>
            {locale === "zh"
              ? `正在对 ${labelOf(groupA)} 和 ${labelOf(groupB)} 运行 Mann-Whitney U + BH-FDR…`
              : `Running Mann-Whitney U + BH-FDR for ${labelOf(groupA)} vs ${labelOf(groupB)}…`}
          </div>
        </div>
      )}

      {/* Error state */}
      {error && (
        <div style={{ background: "rgba(255,100,100,0.1)", border: "1px solid #ff6b6b", borderRadius: "6px", padding: "1rem", marginBottom: "1rem", color: "#ff6b6b" }}>
          {locale === "zh" ? "分析失败：" : "Analysis failed: "}{error}
        </div>
      )}

      {/* Results */}
      {result && !loading && (
        <>
          {/* Summary cards */}
          <PhenotypeStats result={result} />

          {/* View mode tabs + export + sig filter */}
          <div style={{ display: "flex", gap: "0.4rem", alignItems: "center", flexWrap: "wrap", marginBottom: "1rem" }}>
            <div style={{ display: "flex", gap: "0.3rem" }}>
              {viewTabs.map(tab => (
                <button key={tab.key} onClick={() => setViewMode(tab.key)} style={{
                  background: viewMode === tab.key ? "var(--primary)" : "none",
                  border: "1px solid var(--gray)",
                  color: viewMode === tab.key ? "var(--black)" : "var(--light-gray)",
                  borderRadius: "4px",
                  padding: "0.3rem 0.8rem",
                  cursor: "pointer",
                  fontWeight: viewMode === tab.key ? 600 : 400,
                  fontSize: "0.85rem",
                }}>
                  {tab.label}
                </button>
              ))}
            </div>

            <label style={{ fontSize: "0.8rem", color: "var(--light-gray)", display: "flex", alignItems: "center", gap: "0.3rem", cursor: "pointer", marginLeft: "0.5rem" }}>
              <input type="checkbox" checked={showOnlySig} onChange={e => setShowOnlySig(e.target.checked)} />
              {t("phenotype.controls.showOnlySignificant")}
            </label>

            <div style={{ marginLeft: "auto" }}>
              <PhenotypeExport result={result} svgId={chartSvgId} />
            </div>
          </div>

          {/* Chart area */}
          <div style={{ background: "var(--dark-gray)", border: "1px solid var(--gray)", borderRadius: "8px", padding: "1rem", marginBottom: "1.5rem" }}>
            {result.results.length === 0 ? (
              <p style={{ color: "var(--light-gray)", textAlign: "center", padding: "2rem" }}>
                {t("phenotype.noResults")}
              </p>
            ) : (
              <>
                {viewMode === "butterfly" && (
                  <svg id={chartSvgId} style={{ display: "none" }} />
                )}
                {/* We use component's internal SVG ref, but wrap in div with id for export */}
                <div id={chartSvgId + "-wrapper"}>
                  {viewMode === "butterfly" && (
                    <ButterflyBarChart
                      results={result.results}
                      groupA={result.group_a}
                      groupB={result.group_b}
                      labelA={labelOf(result.group_a)}
                      labelB={labelOf(result.group_b)}
                      showOnlySig={showOnlySig}
                    />
                  )}
                  {viewMode === "lollipop" && (
                    <LollipopChart
                      results={result.results}
                      labelA={labelOf(result.group_a)}
                      labelB={labelOf(result.group_b)}
                    />
                  )}
                  {viewMode === "prevalence" && (
                    <PrevalenceAbundancePlot
                      results={result.results}
                      labelA={labelOf(result.group_a)}
                      labelB={labelOf(result.group_b)}
                    />
                  )}
                </div>
              </>
            )}
          </div>

          {/* Result table */}
          <div style={{ background: "var(--dark-gray)", border: "1px solid var(--gray)", borderRadius: "8px", padding: "1rem" }}>
            <h3 style={{ fontSize: "1rem", marginBottom: "0.8rem", color: "var(--light-gray)" }}>
              {locale === "zh" ? "完整结果表" : "Full Results Table"}
            </h3>
            <PhenotypeResultTable
              results={result.results}
              labelA={labelOf(result.group_a)}
              labelB={labelOf(result.group_b)}
            />
          </div>
        </>
      )}

      {/* Initial state (no result yet, not loading) */}
      {!result && !loading && !error && (
        <div style={{ textAlign: "center", padding: "4rem 2rem", color: "var(--light-gray)", border: "1px dashed var(--gray)", borderRadius: "8px" }}>
          <div style={{ fontSize: "1rem", marginBottom: "0.5rem" }}>
            {locale === "zh" ? "选择两个分组，点击「运行分析」开始" : "Select two groups and click \"Run Analysis\" to begin"}
          </div>
          <div style={{ fontSize: "0.8rem", opacity: 0.7 }}>
            {locale === "zh"
              ? "支持性别、年龄、疾病维度；Mann-Whitney U + BH-FDR；返回全部显著属"
              : "Supports sex, age, disease dimensions · Mann-Whitney U + BH-FDR · Returns all significant genera"}
          </div>
        </div>
      )}
    </div>
  );
};

export default PhenotypePage;
