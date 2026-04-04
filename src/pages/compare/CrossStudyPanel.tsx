/**
 * CrossStudyPanel.tsx
 * Cross-study meta-analysis: multi-cohort consensus biomarker discovery
 * 跨研究元分析：多队列一致性标志物发现
 */
import { useEffect, useRef, useState } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import type { CrossStudyResult, CrossStudyMarker, ProjectInfo } from "./types";
import { API_BASE } from "./types";
import classes from "./CrossStudyPanel.module.css";

const CrossStudyPanel = () => {
  const { t, locale } = useI18n();
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [selectedProjects, setSelectedProjects] = useState<string[]>([]);
  const [disease, setDisease] = useState("");
  const [diseases, setDiseases] = useState<string[]>([]);
  const [method, setMethod] = useState("wilcoxon");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<CrossStudyResult | null>(null);
  const [view, setView] = useState<"forest" | "heatmap" | "table">("forest");
  const forestRef = useRef<SVGSVGElement>(null);
  const heatmapRef = useRef<SVGSVGElement>(null);

  // Load projects & diseases
  useEffect(() => {
    const ctrl = new AbortController();
    Promise.all([
      fetch(`${API_BASE}/api/project-list`, { signal: ctrl.signal }).then(r => r.json()),
      fetch(`${API_BASE}/api/filter-options`, { signal: ctrl.signal }).then(r => r.json()),
    ]).then(([projData, filterData]) => {
      setProjects(projData.projects || []);
      setDiseases(filterData.diseases || []);
    }).catch(() => {});
    return () => ctrl.abort();
  }, []);

  const toggleProject = (pid: string) => {
    setSelectedProjects(prev =>
      prev.includes(pid) ? prev.filter(p => p !== pid) : [...prev, pid]
    );
  };

  const runAnalysis = async () => {
    if (selectedProjects.length < 2) {
      setError(t("crossStudy.needProjects"));
      return;
    }
    if (!disease) {
      setError(t("crossStudy.needDisease"));
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch(`${API_BASE}/api/cross-study`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          project_ids: selectedProjects,
          disease,
          method,
          taxonomy_level: "genus",
          p_threshold: 0.05,
          min_studies: 2,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail ?? "Analysis failed");
      }
      const data: CrossStudyResult = await res.json();
      setResult(data);
      setView("forest");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  // Forest plot
  useEffect(() => {
    if (!result || view !== "forest" || !forestRef.current) return;
    const markers = result.consensus_markers.slice(0, 25);
    if (markers.length === 0) return;

    const svg = d3.select(forestRef.current);
    svg.selectAll("*").remove();

    const margin = { top: 40, right: 30, bottom: 40, left: 160 };
    const rowH = 24;
    const w = 700;
    const h = margin.top + markers.length * rowH + margin.bottom;
    svg.attr("viewBox", `0 0 ${w} ${h}`);

    const xMax = Math.max(2, d3.max(markers, d => Math.abs(d.ci_high)) ?? 2, d3.max(markers, d => Math.abs(d.ci_low)) ?? 2);
    const x = d3.scaleLinear().domain([-xMax, xMax]).range([margin.left, w - margin.right]);
    const y = (i: number) => margin.top + i * rowH + rowH / 2;

    // Zero line
    svg.append("line")
      .attr("x1", x(0)).attr("x2", x(0))
      .attr("y1", margin.top - 10).attr("y2", h - margin.bottom)
      .attr("stroke", "#555").attr("stroke-dasharray", "3,3");

    // X axis
    const xAxis = d3.axisBottom(x).ticks(5);
    svg.append("g")
      .attr("transform", `translate(0,${h - margin.bottom})`)
      .call(xAxis)
      .selectAll("text").attr("fill", "#aaa").attr("font-size", 10);
    svg.selectAll(".domain, .tick line").attr("stroke", "#555");

    // X label
    svg.append("text")
      .attr("x", (margin.left + w - margin.right) / 2)
      .attr("y", h - 5)
      .attr("text-anchor", "middle")
      .attr("fill", "#888").attr("font-size", 11)
      .text("log₂FC (meta-analysis)");

    markers.forEach((m, i) => {
      const cy = y(i);
      // Taxon label
      svg.append("text")
        .attr("x", margin.left - 8).attr("y", cy + 4)
        .attr("text-anchor", "end")
        .attr("fill", "#ddd").attr("font-size", 11)
        .text(m.taxon.length > 18 ? m.taxon.slice(0, 16) + "…" : m.taxon);

      // CI line
      svg.append("line")
        .attr("x1", x(m.ci_low)).attr("x2", x(m.ci_high))
        .attr("y1", cy).attr("y2", cy)
        .attr("stroke", m.direction === "disease" ? "#e23fff" : m.direction === "control" ? "#556eff" : "#888")
        .attr("stroke-width", 1.5);

      // Effect size diamond
      const color = m.direction === "disease" ? "#e23fff" : m.direction === "control" ? "#556eff" : "#888";
      svg.append("circle")
        .attr("cx", x(m.meta_log2fc)).attr("cy", cy)
        .attr("r", 4).attr("fill", color);

      // P-value and I²
      svg.append("text")
        .attr("x", w - margin.right + 5).attr("y", cy + 4)
        .attr("fill", m.meta_p < 0.05 ? "#e23fff" : "#666").attr("font-size", 9)
        .text(`p=${m.meta_p < 0.001 ? "<.001" : m.meta_p.toFixed(3)}`);
    });

    // Title
    svg.append("text")
      .attr("x", (margin.left + w - margin.right) / 2)
      .attr("y", 16).attr("text-anchor", "middle")
      .attr("fill", "#ddd").attr("font-size", 13).attr("font-weight", 600)
      .text(t("crossStudy.forestTitle"));
  }, [result, view, t]);

  // Heatmap
  useEffect(() => {
    if (!result || view !== "heatmap" || !heatmapRef.current) return;
    const markers = result.consensus_markers.slice(0, 30);
    const projectIds = result.project_summaries.filter(p => !p.error).map(p => p.project_id);
    if (markers.length === 0 || projectIds.length === 0) return;

    const svg = d3.select(heatmapRef.current);
    svg.selectAll("*").remove();

    const margin = { top: 80, right: 30, bottom: 20, left: 140 };
    const cellW = 50;
    const cellH = 20;
    const w = margin.left + projectIds.length * cellW + margin.right;
    const h = margin.top + markers.length * cellH + margin.bottom;
    svg.attr("viewBox", `0 0 ${w} ${h}`);

    const colorScale = d3.scaleDiverging(d3.interpolateRdBu).domain([3, 0, -3]);

    // Project labels
    projectIds.forEach((pid, j) => {
      svg.append("text")
        .attr("x", margin.left + j * cellW + cellW / 2)
        .attr("y", margin.top - 8)
        .attr("text-anchor", "end")
        .attr("transform", `rotate(-45, ${margin.left + j * cellW + cellW / 2}, ${margin.top - 8})`)
        .attr("fill", "#aaa").attr("font-size", 9)
        .text(pid.length > 12 ? pid.slice(0, 10) + "…" : pid);
    });

    markers.forEach((m, i) => {
      // Taxon label
      svg.append("text")
        .attr("x", margin.left - 5).attr("y", margin.top + i * cellH + cellH / 2 + 4)
        .attr("text-anchor", "end")
        .attr("fill", "#ddd").attr("font-size", 10)
        .text(m.taxon.length > 16 ? m.taxon.slice(0, 14) + "…" : m.taxon);

      projectIds.forEach((pid, j) => {
        const projData = m.per_project[pid];
        const val = projData ? projData.log2fc : 0;
        svg.append("rect")
          .attr("x", margin.left + j * cellW + 1)
          .attr("y", margin.top + i * cellH + 1)
          .attr("width", cellW - 2).attr("height", cellH - 2)
          .attr("fill", projData ? colorScale(val) : "#222")
          .attr("rx", 2);

        if (projData) {
          svg.append("text")
            .attr("x", margin.left + j * cellW + cellW / 2)
            .attr("y", margin.top + i * cellH + cellH / 2 + 4)
            .attr("text-anchor", "middle")
            .attr("fill", Math.abs(val) > 1.5 ? "#fff" : "#ccc").attr("font-size", 8)
            .text(val.toFixed(1));
        }
      });
    });
  }, [result, view]);

  const exportCsv = () => {
    if (!result) return;
    exportTable(
      result.consensus_markers.map(m => ({
        Taxon: m.taxon,
        Meta_log2FC: m.meta_log2fc,
        Meta_SE: m.meta_se,
        Meta_P: m.meta_p,
        CI_Low: m.ci_low,
        CI_High: m.ci_high,
        N_Studies: m.n_studies,
        N_Significant: m.n_significant,
        I2: m.I2,
        Q_P: m.Q_p,
        Direction: m.direction,
      })),
      `cross_study_${disease}_${Date.now()}`,
    );
  };

  // Filter projects that have control samples and contain the selected disease
  const filteredProjects = disease
    ? projects.filter(p => p.has_control && p.diseases.some(d => d.toLowerCase() === disease.toLowerCase()))
    : projects.filter(p => p.has_control);

  return (
    <div className={classes.panel}>
      <h2 className={classes.title}>{t("crossStudy.title")}</h2>
      <p className={classes.subtitle}>{t("crossStudy.subtitle")}</p>

      {/* Disease selector */}
      <div className={classes.controlRow}>
        <label>{t("crossStudy.selectDisease")}</label>
        <select
          className={classes.select}
          value={disease}
          onChange={e => { setDisease(e.target.value); setSelectedProjects([]); }}
        >
          <option value="">{t("crossStudy.pickDisease")}</option>
          {diseases.filter(d => d.toUpperCase() !== "NC").map(d => (
            <option key={d} value={d}>{diseaseDisplayNameI18n(d, locale)}</option>
          ))}
        </select>
      </div>

      {/* Project multi-select */}
      {disease && (
        <div className={classes.projectSection}>
          <label>{t("crossStudy.selectProjects")} ({selectedProjects.length} {t("crossStudy.selected")})</label>
          <div className={classes.projectGrid}>
            {filteredProjects.slice(0, 50).map(p => (
              <button
                key={p.project_id}
                className={classes.projectChip}
                data-active={selectedProjects.includes(p.project_id)}
                onClick={() => toggleProject(p.project_id)}
              >
                <span className={classes.chipId}>{p.project_id}</span>
                <span className={classes.chipCount}>n={p.sample_count}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Method + Run */}
      <div className={classes.controlRow}>
        <label>{t("crossStudy.method")}</label>
        <div className={classes.btnGroup}>
          {(["wilcoxon", "t-test"] as const).map(m => (
            <button key={m} className={classes.methodBtn} data-active={method === m} onClick={() => setMethod(m)}>
              {m}
            </button>
          ))}
        </div>
        <button className={classes.runBtn} onClick={runAnalysis} disabled={loading}>
          {loading ? t("crossStudy.running") : t("crossStudy.run")}
        </button>
      </div>

      {error && <div className={classes.error}>{error}</div>}

      {/* Results */}
      {result && (
        <div className={classes.results}>
          {/* Summary */}
          <div className={classes.summaryRow}>
            <span>{t("crossStudy.nProjects")}: <b>{result.n_projects}</b></span>
            <span>{t("crossStudy.disease")}: <b>{result.disease}</b></span>
            <span>{t("crossStudy.consensus")}: <b>{result.total_significant}</b></span>
          </div>

          {/* Project status */}
          <div className={classes.projectStatus}>
            {result.project_summaries.map(ps => (
              <span key={ps.project_id} className={classes.projBadge} data-error={!!ps.error}>
                {ps.project_id}: {ps.error ?? `D=${ps.n_disease} C=${ps.n_control}`}
              </span>
            ))}
          </div>

          {/* View tabs */}
          <div className={classes.viewTabs}>
            {(["forest", "heatmap", "table"] as const).map(v => (
              <button key={v} className={classes.viewTab} data-active={view === v} onClick={() => setView(v)}>
                {t(`crossStudy.view.${v}` as const)}
              </button>
            ))}
          </div>

          {/* Charts */}
          {view === "forest" && (
            <svg ref={forestRef} className="compare-chart" style={{ width: "100%", maxWidth: 750 }} />
          )}
          {view === "heatmap" && (
            <svg ref={heatmapRef} className="compare-chart" style={{ width: "100%", maxWidth: 700 }} />
          )}
          {view === "table" && (
            <div className={classes.tableWrap}>
              <table className={classes.table}>
                <thead>
                  <tr>
                    <th>{t("crossStudy.col.taxon")}</th>
                    <th>log₂FC</th>
                    <th>SE</th>
                    <th>95% CI</th>
                    <th>P</th>
                    <th>I²</th>
                    <th>{t("crossStudy.col.direction")}</th>
                    <th>{t("crossStudy.col.nStudies")}</th>
                  </tr>
                </thead>
                <tbody>
                  {result.consensus_markers.map(m => (
                    <tr key={m.taxon}>
                      <td>{m.taxon}</td>
                      <td style={{ color: m.meta_log2fc > 0 ? "#e23fff" : "#556eff" }}>
                        {m.meta_log2fc.toFixed(3)}
                      </td>
                      <td>{m.meta_se.toFixed(4)}</td>
                      <td>[{m.ci_low.toFixed(2)}, {m.ci_high.toFixed(2)}]</td>
                      <td style={{ color: m.meta_p < 0.05 ? "#e23fff" : "#888" }}>
                        {m.meta_p < 0.001 ? "<0.001" : m.meta_p.toFixed(4)}
                      </td>
                      <td>{m.I2.toFixed(0)}%</td>
                      <td>{m.direction}</td>
                      <td>{m.n_significant}/{m.n_studies}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Export */}
          <div className={classes.exportRow}>
            <button className={classes.exportBtn} onClick={exportCsv}>{t("export.csv")}</button>
            <button className={classes.exportBtn} onClick={() => {
              const svg = view === "forest" ? forestRef.current : heatmapRef.current;
              if (svg) exportSVG(svg, `cross_study_${view}_${Date.now()}`);
            }}>{t("export.svg")}</button>
            <button className={classes.exportBtn} onClick={() => {
              const svg = view === "forest" ? forestRef.current : heatmapRef.current;
              if (svg) exportPNG(svg, `cross_study_${view}_${Date.now()}`);
            }}>{t("export.png")}</button>
          </div>
        </div>
      )}
    </div>
  );
};

export default CrossStudyPanel;
