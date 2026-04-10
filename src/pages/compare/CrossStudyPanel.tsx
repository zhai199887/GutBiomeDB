import { useEffect, useMemo, useRef, useState, type RefObject } from "react";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { exportTable } from "@/util/export";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";

import classes from "./CrossStudyPanel.module.css";
import type { CrossStudyMarker, CrossStudyResult, ProjectInfo, TaxonomyLevel } from "./types";
import { API_BASE } from "./types";

type CrossStudyView = "forest" | "heatmap" | "consistency" | "bubble" | "table";

function calcConsistency(marker: CrossStudyMarker): number {
  const entries = Object.values(marker.per_project);
  if (!entries.length) return 0;
  const positive = entries.filter((entry) => entry.log2fc > 0).length;
  const negative = entries.length - positive;
  return Math.round((Math.max(positive, negative) / entries.length) * 100);
}

function directionColor(direction: CrossStudyMarker["direction"]): string {
  if (direction === "disease") return "#22c55e";
  if (direction === "control") return "#3b82f6";
  return "#94a3b8";
}

const CrossStudyPanel = ({ taxonomyLevel }: { taxonomyLevel: TaxonomyLevel }) => {
  const { t, locale } = useI18n();
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [diseases, setDiseases] = useState<string[]>([]);
  const [selectedProjects, setSelectedProjects] = useState<string[]>([]);
  const [projectSearch, setProjectSearch] = useState("");
  const [disease, setDisease] = useState("");
  const [method, setMethod] = useState<"wilcoxon" | "t-test">("wilcoxon");
  const [view, setView] = useState<CrossStudyView>("forest");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<CrossStudyResult | null>(null);

  const forestRef = useRef<SVGSVGElement>(null);
  const heatmapRef = useRef<SVGSVGElement>(null);
  const consistencyRef = useRef<SVGSVGElement>(null);
  const bubbleRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    Promise.all([
      cachedFetch<{ projects: ProjectInfo[] }>(`${API_BASE}/api/project-list`),
      cachedFetch<{ diseases: string[] }>(`${API_BASE}/api/filter-options`),
    ])
      .then(([projectPayload, filterPayload]) => {
        setProjects(projectPayload.projects ?? []);
        setDiseases((filterPayload.diseases ?? []).filter((item) => item.toUpperCase() !== "NC"));
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!projects.length) return;
    const raw = localStorage.getItem("crossStudyPreselect");
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw) as { projectIds?: string[]; disease?: string } | string[];
      const projectIds = Array.isArray(parsed) ? parsed : (parsed.projectIds ?? []);
      const prefilledDisease = Array.isArray(parsed) ? "" : (parsed.disease ?? "");
      const allowed = new Set(projects.filter((project) => project.has_control).map((project) => project.project_id));
      setSelectedProjects(projectIds.filter((projectId) => allowed.has(projectId)));
      if (prefilledDisease) {
        setDisease(prefilledDisease);
      }
    } catch {
      // ignore malformed localStorage payload
    } finally {
      localStorage.removeItem("crossStudyPreselect");
    }
  }, [projects]);

  const filteredProjects = useMemo(() => {
    const query = projectSearch.trim().toLowerCase();
    const base = projects.filter((project) => project.has_control);
    return base
      .filter((project) => {
        if (disease && !project.diseases.some((item) => item.toLowerCase() === disease.toLowerCase())) {
          return false;
        }
        if (!query) return true;
        return (
          project.project_id.toLowerCase().includes(query) ||
          project.diseases.some((item) => item.toLowerCase().includes(query))
        );
      })
      .sort((a, b) => b.sample_count - a.sample_count);
  }, [disease, projectSearch, projects]);

  const projectSizeMap = useMemo(() => {
    const map = new Map<string, number>();
    result?.project_summaries.forEach((summary) => {
      map.set(summary.project_id, summary.n_disease + summary.n_control);
    });
    return map;
  }, [result?.project_summaries]);

  const markers = result?.consensus_markers ?? [];
  const chartMarkers = markers.slice(0, 24);
  const projectIds = result?.project_summaries.filter((item) => !item.error).map((item) => item.project_id) ?? [];

  const toggleProject = (projectId: string) => {
    setSelectedProjects((previous) => (
      previous.includes(projectId)
        ? previous.filter((item) => item !== projectId)
        : [...previous, projectId]
    ));
  };

  const runAnalysis = async () => {
    if (!disease) {
      setError(t("crossStudy.needDisease"));
      return;
    }
    if (selectedProjects.length < 2) {
      setError(t("crossStudy.needProjects"));
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const response = await fetch(`${API_BASE}/api/cross-study`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          project_ids: selectedProjects,
          disease,
          method,
          taxonomy_level: taxonomyLevel,
          p_threshold: 0.05,
          min_studies: 2,
        }),
      });

      if (!response.ok) {
        const payload = await response.json();
        throw new Error(payload.detail ?? "Cross-study analysis failed");
      }

      setResult(await response.json());
      setView("forest");
    } catch (unknownError) {
      setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
    } finally {
      setLoading(false);
    }
  };

  const exportCsv = () => {
    if (!result) return;
    exportTable(
      result.consensus_markers.map((marker) => ({
        Taxon: marker.taxon,
        Meta_log2FC: marker.meta_log2fc,
        Meta_SE: marker.meta_se,
        Meta_P: marker.meta_p,
        Adjusted_Meta_P: marker.adjusted_meta_p,
        CI_Low: marker.ci_low,
        CI_High: marker.ci_high,
        Direction: marker.direction,
        N_Studies: marker.n_studies,
        N_Significant: marker.n_significant,
        I2: marker.I2,
        Consistency: calcConsistency(marker),
      })),
      `cross_study_${taxonomyLevel}_${Date.now()}`,
    );
  };

  const exportCurrentSvg = (kind: "svg" | "png") => {
    if (view === "table") return;
    const currentSvg =
      view === "forest" ? forestRef.current :
      view === "heatmap" ? heatmapRef.current :
      view === "consistency" ? consistencyRef.current :
      bubbleRef.current;

    if (!currentSvg) return;
    if (kind === "svg") {
      exportSVG(currentSvg, `cross_study_${view}_${Date.now()}`);
    } else {
      exportPNG(currentSvg, `cross_study_${view}_${Date.now()}`);
    }
  };

  return (
    <div className={classes.panel}>
      <h2 className={classes.title}>{t("crossStudy.title")}</h2>
      <p className={classes.subtitle}>
        {locale === "zh"
          ? `按 ${taxonomyLevel} 层级做跨研究元分析，查看方向一致性、异质性和候选标志物。`
          : `Cross-study meta-analysis at the ${taxonomyLevel} level with consistency, heterogeneity, and candidate-marker views.`}
      </p>

      <div className={classes.controlRow}>
        <label>{t("crossStudy.selectDisease")}</label>
        <input
          list="cross-study-disease-list"
          className={classes.select}
          value={disease}
          onChange={(event) => setDisease(event.target.value)}
          placeholder={t("crossStudy.pickDisease")}
        />
        <datalist id="cross-study-disease-list">
          {diseases.map((item) => (
            <option key={item} value={item} label={diseaseDisplayNameI18n(item, locale)} />
          ))}
        </datalist>
      </div>

      <div className={classes.controlRow}>
        <label>{locale === "zh" ? "项目检索" : "Project search"}</label>
        <input
          className={classes.select}
          value={projectSearch}
          placeholder={locale === "zh" ? "按项目 ID 或疾病关键词过滤…" : "Filter by project ID or disease keyword..."}
          onChange={(event) => setProjectSearch(event.target.value)}
        />
        <span className={classes.metaText}>
          {filteredProjects.length.toLocaleString("en-US")} {locale === "zh" ? "个候选项目" : "eligible projects"}
        </span>
      </div>

      <div className={classes.projectSection}>
        <label>{t("crossStudy.selectProjects")} ({selectedProjects.length} {t("crossStudy.selected")})</label>
        <div className={classes.projectGrid}>
          {filteredProjects.map((project) => (
            <button
              key={project.project_id}
              type="button"
              className={classes.projectChip}
              data-active={selectedProjects.includes(project.project_id)}
              onClick={() => toggleProject(project.project_id)}
            >
              <span className={classes.chipId}>{project.project_id}</span>
              <span className={classes.chipCount}>n={project.sample_count}</span>
              <span className={classes.chipMeta}>{project.country}</span>
            </button>
          ))}
        </div>
        {selectedProjects.length > 12 ? (
          <p className={classes.softHint}>
            {locale === "zh"
              ? "已选项目较多，同步元分析会更慢，但本轮不做硬限制。"
              : "Large project sets will take longer under the synchronous meta-analysis path, but no hard cap is applied."}
          </p>
        ) : null}
      </div>

      <div className={classes.controlRow}>
        <label>{t("crossStudy.method")}</label>
        <div className={classes.btnGroup}>
          {(["wilcoxon", "t-test"] as const).map((item) => (
            <button
              key={item}
              type="button"
              className={classes.methodBtn}
              data-active={method === item}
              onClick={() => setMethod(item)}
            >
              {item}
            </button>
          ))}
        </div>
        <button className={classes.runBtn} type="button" onClick={runAnalysis} disabled={loading}>
          {loading ? t("crossStudy.running") : t("crossStudy.run")}
        </button>
      </div>

      {error ? <div className={classes.error}>{error}</div> : null}

      {result ? (
        <div className={classes.results}>
          <div className={classes.summaryRow}>
            <span>{t("crossStudy.nProjects")}: <b>{result.n_projects}</b></span>
            <span>{t("crossStudy.disease")}: <b>{diseaseDisplayNameI18n(result.disease, locale)}</b></span>
            <span>{locale === "zh" ? "层级" : "Level"}: <b>{result.taxonomy_level}</b></span>
            <span>{t("crossStudy.consensus")}: <b>{result.total_significant}</b></span>
          </div>

          <div className={classes.projectStatus}>
            {result.project_summaries.map((summary) => (
              <span key={summary.project_id} className={classes.projBadge} data-error={Boolean(summary.error)}>
                {summary.project_id}: {summary.error ?? `D=${summary.n_disease} C=${summary.n_control}`}
              </span>
            ))}
          </div>

          <div className={classes.viewTabs}>
            {(["forest", "heatmap", "consistency", "bubble", "table"] as const).map((item) => (
              <button
                key={item}
                type="button"
                className={classes.viewTab}
                data-active={view === item}
                onClick={() => setView(item)}
              >
                {{
                  forest: locale === "zh" ? "森林图" : "Forest",
                  heatmap: locale === "zh" ? "热图" : "Heatmap",
                  consistency: locale === "zh" ? "一致性" : "Consistency",
                  bubble: "Bubble",
                  table: locale === "zh" ? "表格" : "Table",
                }[item]}
              </button>
            ))}
          </div>

          {view === "forest" ? <ForestView svgRef={forestRef} markers={chartMarkers} locale={locale} /> : null}
          {view === "heatmap" ? <HeatmapView svgRef={heatmapRef} markers={chartMarkers} projectIds={projectIds} locale={locale} /> : null}
          {view === "consistency" ? <ConsistencyView svgRef={consistencyRef} markers={chartMarkers} locale={locale} /> : null}
          {view === "bubble" ? <BubbleView svgRef={bubbleRef} markers={chartMarkers} projectSizeMap={projectSizeMap} locale={locale} /> : null}
          {view === "table" ? (
            <div className={classes.tableWrap}>
              <table className={classes.table}>
                <thead>
                  <tr>
                    <th>{t("crossStudy.col.taxon")}</th>
                    <th>log2FC</th>
                    <th>95% CI</th>
                    <th>p</th>
                    <th>{locale === "zh" ? "BH 校正 p" : "BH-adjusted p"}</th>
                    <th>I2</th>
                    <th>{t("crossStudy.col.direction")}</th>
                    <th>{t("crossStudy.col.nStudies")}</th>
                  </tr>
                </thead>
                <tbody>
                  {markers.map((marker) => (
                    <tr key={marker.taxon}>
                      <td>{marker.taxon}</td>
                      <td>{marker.meta_log2fc.toFixed(3)}</td>
                      <td>[{marker.ci_low.toFixed(2)}, {marker.ci_high.toFixed(2)}]</td>
                      <td>{marker.meta_p < 0.001 ? "<0.001" : marker.meta_p.toFixed(4)}</td>
                      <td>{marker.adjusted_meta_p < 0.001 ? "<0.001" : marker.adjusted_meta_p.toFixed(4)}</td>
                      <td>{marker.I2.toFixed(1)}%</td>
                      <td>{marker.direction}</td>
                      <td>{marker.n_significant}/{marker.n_studies}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}

          <div className={classes.exportRow}>
            <button className={classes.exportBtn} type="button" onClick={exportCsv}>{t("export.csv")}</button>
            <button className={classes.exportBtn} type="button" onClick={() => exportCurrentSvg("svg")}>{t("export.svg")}</button>
            <button className={classes.exportBtn} type="button" onClick={() => exportCurrentSvg("png")}>{t("export.png")}</button>
          </div>
        </div>
      ) : null}
    </div>
  );
};

const ForestView = ({
  svgRef,
  markers,
  locale,
}: {
  svgRef: RefObject<SVGSVGElement | null>;
  markers: CrossStudyMarker[];
  locale: string;
}) => {
  const width = 860;
  const rowHeight = 28;
  const height = Math.max(220, 84 + markers.length * rowHeight);
  const min = Math.min(...markers.map((marker) => marker.ci_low), -2);
  const max = Math.max(...markers.map((marker) => marker.ci_high), 2);
  const scale = (value: number) => 260 + ((value - min) / Math.max(max - min, 1e-6)) * 450;

  const legendY = height - 14;
  return (
    <svg ref={svgRef} className="compare-chart" viewBox={`0 0 ${width} ${height + 28}`} style={{ width: "100%", maxWidth: 860 }}>
      <line x1={scale(0)} x2={scale(0)} y1={48} y2={height - 28} stroke="#6b7280" strokeDasharray="4 4" />
      {markers.map((marker, index) => {
        const y = 66 + index * rowHeight;
        return (
          <g key={marker.taxon}>
            <title>{`${marker.taxon}\nlog2FC: ${marker.meta_log2fc.toFixed(3)}\n95% CI: [${marker.ci_low.toFixed(2)}, ${marker.ci_high.toFixed(2)}]\np: ${marker.meta_p.toExponential(2)}\nadj.p: ${marker.adjusted_meta_p.toExponential(2)}\nI2: ${marker.I2.toFixed(1)}%`}</title>
            <text x={180} y={y + 4} textAnchor="end" fill="currentColor" fontSize="11">
              {marker.taxon.length > 28 ? `${marker.taxon.slice(0, 26)}...` : marker.taxon}
            </text>
            <line x1={scale(marker.ci_low)} x2={scale(marker.ci_high)} y1={y} y2={y} stroke={directionColor(marker.direction)} strokeWidth="2" />
            <circle cx={scale(marker.meta_log2fc)} cy={y} r={4 + Math.min(marker.n_significant, 4)} fill={directionColor(marker.direction)} />
            <text x={740} y={y + 4} fill="#94a3b8" fontSize="10">
              {marker.adjusted_meta_p < 0.001 ? "adj.p <0.001" : `adj.p ${marker.adjusted_meta_p.toFixed(3)}`}
            </text>
          </g>
        );
      })}
      <text x={width / 2} y={22} textAnchor="middle" fill="currentColor" fontSize="13">
        {locale === "zh" ? "跨研究效应森林图" : "Cross-study forest plot"}
      </text>
      {/* Legend */}
      <circle cx={196} cy={legendY} r={5} fill="#22c55e" />
      <text x={204} y={legendY + 4} fill="#94a3b8" fontSize="9">{locale === "zh" ? "疾病富集" : "Disease-enriched"}</text>
      <circle cx={316} cy={legendY} r={5} fill="#3b82f6" />
      <text x={324} y={legendY + 4} fill="#94a3b8" fontSize="9">{locale === "zh" ? "对照富集" : "Control-enriched"}</text>
      <circle cx={432} cy={legendY} r={5} fill="#94a3b8" />
      <text x={440} y={legendY + 4} fill="#94a3b8" fontSize="9">{locale === "zh" ? "方向不一致" : "Inconsistent direction"}</text>
      <text x={590} y={legendY + 4} fill="#64748b" fontSize="9">{locale === "zh" ? "圆圈大小 = 显著研究数" : "Circle size = no. significant studies"}</text>
    </svg>
  );
};

const HeatmapView = ({
  svgRef,
  markers,
  projectIds,
  locale,
}: {
  svgRef: RefObject<SVGSVGElement | null>;
  markers: CrossStudyMarker[];
  projectIds: string[];
  locale: string;
}) => {
  const width = Math.max(880, 260 + projectIds.length * 60);
  const cellWidth = 58;
  const cellHeight = 24;
  const topPadding = 126;
  const labelY = 62;
  const rowStartY = 100;
  const legendX = Math.max(248, width - 220);
  const height = topPadding + markers.length * cellHeight;
  const color = (value: number) => value >= 0 ? `rgba(34, 197, 94, ${0.16 + Math.min(Math.abs(value) / 2.5, 0.64)})` : `rgba(59, 130, 246, ${0.16 + Math.min(Math.abs(value) / 2.5, 0.64)})`;

  return (
    <svg ref={svgRef} className="compare-chart" viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", maxWidth: width }}>
      <defs>
        <linearGradient id="cross-study-heatmap-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="rgba(59, 130, 246, 0.8)" />
          <stop offset="50%" stopColor="rgba(148, 163, 184, 0.18)" />
          <stop offset="100%" stopColor="rgba(34, 197, 94, 0.8)" />
        </linearGradient>
      </defs>
      <text x={legendX} y={8} fill="#cbd5e1" fontSize="10">
        log2FC
      </text>
      <rect
        x={legendX}
        y={14}
        width={200}
        height={10}
        rx={999}
        fill="url(#cross-study-heatmap-gradient)"
        stroke="rgba(148, 163, 184, 0.22)"
      />
      <text x={legendX} y={38} fill="#94a3b8" fontSize="9">
        {locale === "zh" ? "对照富集 (-)" : "Control-enriched (-)"}
      </text>
      <text x={legendX + 100} y={38} textAnchor="middle" fill="#94a3b8" fontSize="9">
        0
      </text>
      <text x={legendX + 200} y={38} textAnchor="end" fill="#94a3b8" fontSize="9">
        {locale === "zh" ? "疾病富集 (+)" : "Disease-enriched (+)"}
      </text>
      <text x={width / 2} y={18} textAnchor="middle" fill="currentColor" fontSize="13">
        {locale === "zh" ? "项目内 log2FC 热图" : "Per-project log2FC heatmap"}
      </text>
      {projectIds.map((projectId, col) => (
        <text
          key={projectId}
          x={248 + col * cellWidth + cellWidth / 2}
          y={labelY}
          transform={`rotate(-40, ${248 + col * cellWidth + cellWidth / 2}, ${labelY})`}
          fill="#94a3b8"
          fontSize="9"
          textAnchor="end"
        >
          {projectId.slice(0, 12)}
        </text>
      ))}
      {markers.map((marker, row) => (
        <g key={marker.taxon}>
          <text x={205} y={rowStartY + row * cellHeight + 15} textAnchor="end" fill="currentColor" fontSize="10">
            {marker.taxon.length > 26 ? `${marker.taxon.slice(0, 24)}...` : marker.taxon}
          </text>
          {projectIds.map((projectId, col) => {
            const value = marker.per_project[projectId]?.log2fc ?? 0;
            const pValue = marker.per_project[projectId]?.p_value;
            return (
              <rect
                key={`${marker.taxon}-${projectId}`}
                x={248 + col * cellWidth}
                y={rowStartY + row * cellHeight}
                width={cellWidth - 4}
                height={cellHeight - 4}
                rx={4}
                fill={marker.per_project[projectId] ? color(value) : "rgba(148, 163, 184, 0.12)"}
              >
                <title>{`${marker.taxon}\n${projectId}\nlog2FC: ${value.toFixed(3)}\np: ${pValue?.toExponential(2) ?? "NA"}\nadj.p(meta): ${marker.adjusted_meta_p.toExponential(2)}`}</title>
              </rect>
            );
          })}
        </g>
      ))}
    </svg>
  );
};

const ConsistencyView = ({
  svgRef,
  markers,
  locale,
}: {
  svgRef: RefObject<SVGSVGElement | null>;
  markers: CrossStudyMarker[];
  locale: string;
}) => {
  const width = 760;
  const height = 120 + markers.length * 26;
  const legendY = height + 10;
  return (
    <svg ref={svgRef} className="compare-chart" viewBox={`0 0 ${width} ${legendY + 20}`} style={{ width: "100%", maxWidth: 760 }}>
      <text x={width / 2} y={24} textAnchor="middle" fill="currentColor" fontSize="13">
        {locale === "zh" ? "跨研究一致性" : "Cross-study consistency"}
      </text>
      {markers.map((marker, index) => {
        const score = calcConsistency(marker);
        const widthPx = (score / 100) * 420;
        const y = 50 + index * 26;
        return (
          <g key={marker.taxon}>
            <title>{`${marker.taxon}\nConsistency: ${score}%\nI2: ${marker.I2.toFixed(1)}%\nDirection: ${marker.direction}`}</title>
            <text x={165} y={y + 11} textAnchor="end" fill="currentColor" fontSize="10">
              {marker.taxon.length > 26 ? `${marker.taxon.slice(0, 24)}...` : marker.taxon}
            </text>
            <rect x={200} y={y} width={widthPx} height={18} rx={5} fill={directionColor(marker.direction)} opacity="0.85" />
            <text x={630} y={y + 12} fill="#94a3b8" fontSize="10">
              {score}% / I2 {marker.I2.toFixed(0)}%
            </text>
          </g>
        );
      })}
      {/* Legend */}
      <rect x={155} cy={legendY} y={legendY} width={14} height={10} rx={3} fill="#22c55e" opacity="0.85" />
      <text x={172} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "疾病富集" : "Disease-enriched"}</text>
      <rect x={276} y={legendY} width={14} height={10} rx={3} fill="#3b82f6" opacity="0.85" />
      <text x={293} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "对照富集" : "Control-enriched"}</text>
      <rect x={397} y={legendY} width={14} height={10} rx={3} fill="#94a3b8" opacity="0.85" />
      <text x={414} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "方向不一致" : "Inconsistent"}</text>
      <text x={490} y={legendY + 9} fill="#64748b" fontSize="9">{locale === "zh" ? "条长 = 一致性得分；右侧 I² = 异质性" : "Bar length = consistency score; I² = heterogeneity"}</text>
    </svg>
  );
};

const BubbleView = ({
  svgRef,
  markers,
  projectSizeMap,
  locale,
}: {
  svgRef: RefObject<SVGSVGElement | null>;
  markers: CrossStudyMarker[];
  projectSizeMap: Map<string, number>;
  locale: string;
}) => {
  const width = 860;
  const height = 150 + markers.length * 24;
  const min = Math.min(...markers.map((marker) => marker.meta_log2fc), -2);
  const max = Math.max(...markers.map((marker) => marker.meta_log2fc), 2);
  const scale = (value: number) => 260 + ((value - min) / Math.max(max - min, 1e-6)) * 480;

  const legendY = height + 8;
  return (
    <svg ref={svgRef} className="compare-chart" viewBox={`0 0 ${width} ${legendY + 30}`} style={{ width: "100%", maxWidth: 860 }}>
      <text x={width / 2} y={24} textAnchor="middle" fill="currentColor" fontSize="13">
        {locale === "zh" ? "效应值与样本规模气泡图" : "Effect-size bubble view"}
      </text>
      <line x1={scale(0)} x2={scale(0)} y1={40} y2={height - 30} stroke="#64748b" strokeDasharray="4 4" />
      {markers.map((marker, index) => {
        const y = 56 + index * 24;
        const sampleSize = Object.keys(marker.per_project).reduce((sum, projectId) => sum + (projectSizeMap.get(projectId) ?? 0), 0);
        const radius = Math.max(4, Math.min(12, Math.sqrt(sampleSize) / 6));
        return (
          <g key={marker.taxon}>
            <title>{`${marker.taxon}\nlog2FC: ${marker.meta_log2fc.toFixed(3)}\nCombined n: ${sampleSize}\nadj.p: ${marker.adjusted_meta_p.toExponential(2)}`}</title>
            <text x={205} y={y + 4} textAnchor="end" fill="currentColor" fontSize="10">
              {marker.taxon.length > 26 ? `${marker.taxon.slice(0, 24)}...` : marker.taxon}
            </text>
            <circle
              cx={scale(marker.meta_log2fc)}
              cy={y}
              r={radius}
              fill={directionColor(marker.direction)}
              opacity={marker.adjusted_meta_p < 0.05 ? 0.9 : 0.45}
            />
            <text x={750} y={y + 4} fill="#94a3b8" fontSize="10">
              n={sampleSize}
            </text>
          </g>
        );
      })}
      {/* Legend */}
      <circle cx={212} cy={legendY + 5} r={6} fill="#22c55e" opacity="0.9" />
      <text x={221} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "疾病富集" : "Disease-enriched"}</text>
      <circle cx={330} cy={legendY + 5} r={6} fill="#3b82f6" opacity="0.9" />
      <text x={339} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "对照富集" : "Control-enriched"}</text>
      <circle cx={448} cy={legendY + 5} r={6} fill="#94a3b8" opacity="0.9" />
      <text x={457} y={legendY + 9} fill="#94a3b8" fontSize="9">{locale === "zh" ? "方向不一致" : "Inconsistent"}</text>
      <text x={534} y={legendY + 9} fill="#64748b" fontSize="9">{locale === "zh" ? "气泡大小 = 总样本量" : "Bubble size = total sample size"}</text>
    </svg>
  );
};

export default CrossStudyPanel;
