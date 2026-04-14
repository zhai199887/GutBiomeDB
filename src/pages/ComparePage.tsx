import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { useI18n } from "@/i18n";
import "@/components/tooltip";
import { cachedFetch } from "@/util/apiCache";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { exportTable } from "@/util/export";

import classes from "./ComparePage.module.css";
import AlphaBoxChart from "./compare/AlphaBoxChart";
import BetaPCoAChart from "./compare/BetaPCoAChart";
import CrossStudyPanel from "./compare/CrossStudyPanel";
import DiffBarChart from "./compare/DiffBarChart";
import DiffHeatmap from "./compare/DiffHeatmap";
import GroupFilterPanel from "./compare/GroupFilterPanel";
import SpearmanChart from "./compare/SpearmanChart";
import StackedBarChart from "./compare/StackedBarChart";
import VolcanoChart from "./compare/VolcanoChart";
import {
  API_BASE,
  METHODS,
  TAXONOMY_LEVELS,
  type DiffResult,
  type FilterOptions,
  type GroupFilter,
  type SampleCountResult,
  type SpearmanResult,
  type Tab,
  type TaxonomyLevel,
} from "./compare/types";

const ComparePage = () => {
  const { t, locale } = useI18n();
  const [searchParams, setSearchParams] = useSearchParams();
  const [filterOptions, setFilterOptions] = useState<FilterOptions | null>(null);
  const [filterLoading, setFilterLoading] = useState(true);
  const [groupA, setGroupA] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [groupB, setGroupB] = useState<GroupFilter>({ country: "", disease: "", age_group: "", sex: "" });
  const [sampleCounts, setSampleCounts] = useState<SampleCountResult | null>(null);
  const [sampleCountLoading, setSampleCountLoading] = useState(false);
  const [taxLevel, setTaxLevel] = useState<TaxonomyLevel>("genus");
  const [method, setMethod] = useState<(typeof METHODS)[number]>("wilcoxon");
  const [result, setResult] = useState<DiffResult | null>(null);
  const [spearman, setSpearman] = useState<SpearmanResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [spearmanLoading, setSpearmanLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>(
    searchParams.get("tab") === "crossstudy" ? "crossstudy" : "bar",
  );

  useEffect(() => {
    cachedFetch<FilterOptions>(`${API_BASE}/api/filter-options`)
      .then((data) => setFilterOptions(data))
      .catch(() => setError(t("compare.backendError")))
      .finally(() => setFilterLoading(false));
  }, [t]);

  useEffect(() => {
    if (searchParams.get("tab") === "crossstudy") {
      setActiveTab("crossstudy");
      return;
    }
    if (activeTab === "crossstudy") {
      setActiveTab("bar");
    }
  }, [activeTab, searchParams]);

  useEffect(() => {
    if (filterLoading) return;
    setSampleCountLoading(true);
    const timer = window.setTimeout(async () => {
      try {
        const response = await fetch(`${API_BASE}/api/estimate-sample-count`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            group_a_filter: groupA,
            group_b_filter: groupB,
          }),
        });
        if (!response.ok) return;
        setSampleCounts(await response.json());
      } catch {
        // keep workspace usable even if preview requests fail
      } finally {
        setSampleCountLoading(false);
      }
    }, 250);
    return () => window.clearTimeout(timer);
  }, [filterLoading, groupA, groupB]);

  const setWorkspaceTab = (tab: Tab) => {
    setActiveTab(tab);
    const next = new URLSearchParams(searchParams);
    if (tab === "crossstudy") {
      next.set("tab", "crossstudy");
    } else {
      next.delete("tab");
    }
    setSearchParams(next, { replace: true });
  };

  const runAnalysis = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const response = await fetch(`${API_BASE}/api/diff-analysis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          group_a_filter: groupA,
          group_b_filter: groupB,
          taxonomy_level: taxLevel,
          method,
        }),
      });

      if (!response.ok) {
        const payload = await response.json();
        throw new Error(payload.detail ?? "Analysis failed");
      }

      const data: DiffResult = await response.json();
      setResult(data);
      setWorkspaceTab("bar");

      setSpearmanLoading(true);
      setSpearman(null);
      const corrResponse = await fetch(`${API_BASE}/api/spearman-analysis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          group_a_filter: groupA,
          group_b_filter: groupB,
          taxonomy_level: taxLevel,
          max_taxa: 16,
        }),
      });
      if (corrResponse.ok) {
        setSpearman(await corrResponse.json());
      }
    } catch (unknownError) {
      setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
    } finally {
      setLoading(false);
      setSpearmanLoading(false);
    }
  };

  const exportCsv = () => {
    if (!result) return;
    exportTable(
      result.diff_taxa.map((taxon) => ({
        Genus: taxon.taxon,
        Phylum: taxon.phylum,
        Mean_A_Percent: taxon.mean_a,
        Mean_B_Percent: taxon.mean_b,
        Prevalence_A: taxon.prevalence_a,
        Prevalence_B: taxon.prevalence_b,
        Log2FC: taxon.log2fc,
        P_Value: taxon.p_value,
        Adjusted_P: taxon.adjusted_p,
        Effect_Size: taxon.effect_size,
      })),
      `compare_${taxLevel}_${Date.now()}`,
    );
  };

  const exportSvgChart = () => {
    const svgElement = document.querySelector<SVGSVGElement>(".compare-chart");
    if (svgElement) exportSVG(svgElement, `compare_${activeTab}_${Date.now()}`);
  };

  const exportPngChart = () => {
    const svgElement = document.querySelector<SVGSVGElement>(".compare-chart");
    if (svgElement) exportPNG(svgElement, `compare_${activeTab}_${Date.now()}`);
  };

  const tabs = useMemo(() => {
    const nextTabs: Array<[Tab, string]> = [
      ["bar", t("compare.tab.bar")],
      ["volcano", t("compare.tab.volcano")],
      ["alpha", t("compare.tab.alpha")],
      ["beta", t("compare.tab.beta")],
      ["composition", locale === "zh" ? "组成结构" : "Composition"],
      ["heatmap", locale === "zh" ? "差异热图" : "Heatmap"],
      ["correlation", "Spearman"],
      ["crossstudy", t("compare.tab.crossStudy")],
    ];
    if (result?.lefse_results) nextTabs.push(["lefse", t("compare.tab.lefse")]);
    if (result?.permanova) nextTabs.push(["permanova", t("compare.tab.permanova")]);
    return nextTabs;
  }, [locale, result?.lefse_results, result?.permanova, t]);

  const renderActivePanel = () => {
    if (activeTab === "crossstudy") {
      return <CrossStudyPanel taxonomyLevel={taxLevel} />;
    }

    if (!result) {
      return (
        <div className={classes.emptyPanel}>
          {locale === "zh"
            ? "先定义 Group A / Group B 并运行差异分析；如果你要直接做跨研究元分析，切到 Cross-study。"
            : "Define Group A / Group B and run differential analysis first, or switch to Cross-study for project-level meta-analysis."}
        </div>
      );
    }

    if (activeTab === "bar") return <DiffBarChart result={result} />;
    if (activeTab === "volcano") return <VolcanoChart result={result} />;
    if (activeTab === "alpha") return <AlphaBoxChart result={result} />;
    if (activeTab === "beta") return <BetaPCoAChart result={result} />;
    if (activeTab === "composition") return <StackedBarChart result={result} />;
    if (activeTab === "heatmap") return <DiffHeatmap result={result} />;
    if (activeTab === "correlation") {
      return spearmanLoading ? (
        <div className={classes.emptyPanel}>
          {locale === "zh" ? "正在计算 Spearman 结构…" : "Computing Spearman structure..."}
        </div>
      ) : (
        <SpearmanChart result={spearman} />
      );
    }
    if (activeTab === "lefse") return <LefseResults result={result} />;
    if (activeTab === "permanova") return <PermanovaResults result={result} />;
    return null;
  };

  return (
    <div className={classes.page}>
      <div className={classes.nav}>
        <Link to="/" className={classes.back}>{t("compare.back")}</Link>
        <h1 className={classes.title}>{t("compare.title")}</h1>
        <span className={classes.subtitle}>
          {locale === "zh"
            ? "按真实样本规模预估、双组差异、alpha/beta 多样性、组成结构和跨研究证据逐层查看。"
            : "Inspect sample size, differential taxa, alpha/beta diversity, composition, and cross-study evidence in one workspace."}
        </span>
      </div>

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
              sampleCount={sampleCounts?.group_a ?? null}
            />
            <GroupFilterPanel
              label={t("compare.groupB")}
              color="var(--primary)"
              value={groupB}
              onChange={setGroupB}
              options={filterOptions}
              sampleCount={sampleCounts?.group_b ?? null}
            />
          </div>
        )}

        <div className={classes.controls}>
          <div className={classes.control}>
            <label>{t("compare.taxLevel")}</label>
            <div className={classes.btnGroup}>
              {TAXONOMY_LEVELS.map((level) => (
                <button
                  key={level}
                  type="button"
                  className={classes.ctrlBtn}
                  data-active={taxLevel === level}
                  onClick={() => setTaxLevel(level)}
                >
                  {level}
                </button>
              ))}
            </div>
          </div>

          <div className={classes.control}>
            <label>{t("compare.statTest")}</label>
            <div className={classes.btnGroup}>
              {METHODS.map((item) => (
                <button
                  key={item}
                  type="button"
                  className={classes.ctrlBtn}
                  data-active={method === item}
                  onClick={() => setMethod(item)}
                >
                  {item === "lefse" ? "LEfSe" : item === "permanova" ? "PERMANOVA" : item}
                </button>
              ))}
            </div>
          </div>

          <div className={classes.previewMeta}>
            {sampleCountLoading ? (
              <span>{locale === "zh" ? "正在预估样本量..." : "Estimating sample counts..."}</span>
            ) : (
              <>
                <span>A: {sampleCounts?.group_a.abundance_n ?? 0}</span>
                <span>B: {sampleCounts?.group_b.abundance_n ?? 0}</span>
              </>
            )}
          </div>

          <button className={classes.analyzeBtn} type="button" onClick={runAnalysis} disabled={loading}>
            {loading ? t("compare.analyzing") : t("compare.run")}
          </button>
        </div>
      </section>

      {error ? <div className={classes.error}>{error}</div> : null}

      <section className={classes.resultSection}>
        {result && activeTab !== "crossstudy" ? (
          <div className={classes.workspaceHeader}>
            <div className={classes.summaryCard}>
              <span>{locale === "zh" ? "工作台摘要" : "Workspace summary"}</span>
              <strong>{result.summary.group_a_name} vs {result.summary.group_b_name}</strong>
              <small>{taxLevel} / {method}</small>
            </div>
            <div className={classes.summaryCard}>
              <span>{locale === "zh" ? "匹配样本" : "Matched samples"}</span>
              <strong>{result.summary.group_a_n} / {result.summary.group_b_n}</strong>
              <small>A / B</small>
            </div>
            {(() => {
              const lvl = (result.summary.taxonomy_level || "").toLowerCase();
              const totalEn = lvl === "phylum" ? "Total phyla" : lvl === "family" ? "Total families" : "Total genera";
              const totalZh = lvl === "phylum" ? "总门数" : lvl === "family" ? "总科数" : "总属数";
              const sigEn = lvl === "phylum" ? "Significant phyla" : lvl === "family" ? "Significant families" : "Significant genera";
              const sigZh = lvl === "phylum" ? "显著门" : lvl === "family" ? "显著科" : "显著属";
              return (
                <>
                  <div className={classes.summaryCard}>
                    <span>{locale === "zh" ? totalZh : totalEn}</span>
                    <strong>{result.summary.total_taxa}</strong>
                    <small>{result.summary.taxonomy_level}</small>
                  </div>
                  <div className={classes.summaryCard}>
                    <span>{locale === "zh" ? sigZh : sigEn}</span>
                    <strong>{result.summary.significant_taxa}</strong>
                    <small>adj. p &lt; 0.05</small>
                  </div>
                </>
              );
            })()}
          </div>
        ) : null}

        <div className={classes.tabs}>
          {tabs.map(([tabId, label]) => (
            <button
              key={tabId}
              type="button"
              className={classes.tab}
              data-active={activeTab === tabId}
              onClick={() => setWorkspaceTab(tabId)}
            >
              {label}
            </button>
          ))}
        </div>

        <div className={classes.chartArea}>{renderActivePanel()}</div>

        {result && activeTab !== "crossstudy" ? (
          <div className={classes.exportRow}>
            <button className={classes.exportBtn} type="button" onClick={exportCsv}>{t("export.csv")}</button>
            <button className={classes.exportBtn} type="button" onClick={exportSvgChart}>{t("export.svg")}</button>
            <button className={classes.exportBtn} type="button" onClick={exportPngChart}>{t("export.png")}</button>
          </div>
        ) : null}
      </section>
    </div>
  );
};

const LefseResults = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  if (!result.lefse_results?.length) {
    return <div className={classes.emptyPanel}>{locale === "zh" ? "没有显著 LEfSe 特征" : "No significant LEfSe features found"}</div>;
  }

  return (
    <div className={classes.simpleTableWrap}>
      <table className={classes.simpleTable}>
        <thead>
          <tr>
            <th>{locale === "zh" ? "属" : "Genus"}</th>
            <th>LDA</th>
            <th>p</th>
            <th>{locale === "zh" ? "富集组" : "Enriched in"}</th>
          </tr>
        </thead>
        <tbody>
          {result.lefse_results.map((row) => (
            <tr key={row.taxon}>
              <td>{row.taxon}</td>
              <td>{row.lda_score.toFixed(2)}</td>
              <td>{row.p_value.toExponential(2)}</td>
              <td>{row.enriched_group}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const PermanovaResults = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  if (!result.permanova) {
    return <div className={classes.emptyPanel}>{locale === "zh" ? "没有 PERMANOVA 结果" : "No PERMANOVA result available"}</div>;
  }

  return (
    <div className={classes.permanovaBox}>
      <div>
        <span>pseudo-F</span>
        <strong>{result.permanova.f_statistic.toFixed(4)}</strong>
      </div>
      <div>
        <span>p-value</span>
        <strong>{result.permanova.p_value.toFixed(4)}</strong>
      </div>
      <div>
        <span>R²</span>
        <strong>{result.permanova.r_squared.toFixed(4)}</strong>
      </div>
      <div>
        <span>{locale === "zh" ? "置换次数" : "Permutations"}</span>
        <strong>{result.permanova.permutations}</strong>
      </div>
    </div>
  );
};

export default ComparePage;
