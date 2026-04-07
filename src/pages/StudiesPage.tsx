import { Fragment, useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { countryName } from "@/util/countries";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { exportTable } from "@/util/export";

import css from "./StudiesPage.module.css";
import ProjectDetailPanel from "./studies/ProjectDetailPanel";
import StudiesMap from "./studies/StudiesMap";
import TimelineChart from "./studies/TimelineChart";
import type { ProjectDetailResult, ProjectListResult, ProjectTimelineResult, StudiesSummary, StudyProjectInfo } from "./studies/types";

const PAGE_SIZE = 20;

type SortKey = "samples" | "diseases" | "year" | "nc";
type ControlFilter = "all" | "yes" | "no";

const EMPTY_SUMMARY: StudiesSummary = {
  total_projects: 0,
  total_samples: 0,
  total_nc: 0,
  total_disease: 0,
  n_countries: 0,
  n_diseases: 0,
  year_range: [],
};

const StudiesPage = () => {
  const { t, locale } = useI18n();
  const navigate = useNavigate();

  const [projects, setProjects] = useState<StudyProjectInfo[]>([]);
  const [summary, setSummary] = useState<StudiesSummary>(EMPTY_SUMMARY);
  const [timeline, setTimeline] = useState<ProjectTimelineResult["timeline"]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [search, setSearch] = useState("");
  const [countryFilter, setCountryFilter] = useState("");
  const [diseaseFilter, setDiseaseFilter] = useState("");
  const [controlFilter, setControlFilter] = useState<ControlFilter>("all");
  const [sortBy, setSortBy] = useState<SortKey>("samples");
  const [page, setPage] = useState(0);

  const [expandedProject, setExpandedProject] = useState<string | null>(null);
  const [projectDetail, setProjectDetail] = useState<Record<string, ProjectDetailResult | null>>({});
  const [detailLoading, setDetailLoading] = useState<Record<string, boolean>>({});
  const [selectedProjects, setSelectedProjects] = useState<string[]>([]);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      cachedFetch<ProjectListResult>(`${API_BASE}/api/project-list`),
      cachedFetch<ProjectTimelineResult>(`${API_BASE}/api/project-timeline`),
    ])
      .then(([projectPayload, timelinePayload]) => {
        setProjects(projectPayload.projects ?? []);
        setSummary(projectPayload.summary ?? EMPTY_SUMMARY);
        setTimeline(timelinePayload.timeline ?? []);
      })
      .catch((unknownError) => {
        setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
      })
      .finally(() => setLoading(false));
  }, []);

  const diseaseOptions = useMemo(
    () => Array.from(new Set(projects.flatMap((project) => project.diseases))).sort((a, b) => a.localeCompare(b)),
    [projects],
  );
  const countryOptions = useMemo(
    () => Array.from(new Set(projects.flatMap((project) => project.country_list))).filter(Boolean).sort((a, b) => a.localeCompare(b)),
    [projects],
  );
  const projectCountsByCountry = useMemo(() => {
    const counts: Record<string, number> = {};
    projects.forEach((project) => {
      const seen = new Set(project.country_list.length ? project.country_list : [project.country]);
      seen.forEach((country) => {
        if (!country || country.toLowerCase() === "unknown") return;
        counts[country] = (counts[country] ?? 0) + 1;
      });
    });
    return counts;
  }, [projects]);

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    const next = projects.filter((project) => {
      if (query) {
        const matchesSearch =
          project.project_id.toLowerCase().includes(query) ||
          project.diseases.some((disease) => disease.toLowerCase().includes(query));
        if (!matchesSearch) return false;
      }

      if (countryFilter) {
        const countries = new Set(project.country_list.length ? project.country_list : [project.country]);
        if (!countries.has(countryFilter)) return false;
      }

      if (diseaseFilter && !project.diseases.some((disease) => disease.toLowerCase() === diseaseFilter.toLowerCase())) {
        return false;
      }

      if (controlFilter === "yes" && !project.has_control) return false;
      if (controlFilter === "no" && project.has_control) return false;
      return true;
    });

    next.sort((a, b) => {
      if (sortBy === "samples") return b.sample_count - a.sample_count;
      if (sortBy === "diseases") return b.n_diseases - a.n_diseases || b.sample_count - a.sample_count;
      if (sortBy === "year") return (b.year ?? -1) - (a.year ?? -1) || b.sample_count - a.sample_count;
      return b.nc_count - a.nc_count || b.sample_count - a.sample_count;
    });

    return next;
  }, [controlFilter, countryFilter, diseaseFilter, projects, search, sortBy]);

  useEffect(() => {
    setPage(0);
  }, [search, countryFilter, diseaseFilter, controlFilter, sortBy]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageData = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const loadDetail = async (projectId: string) => {
    if (projectDetail[projectId] || detailLoading[projectId]) return;
    setDetailLoading((prev) => ({ ...prev, [projectId]: true }));
    try {
      const detail = await cachedFetch<ProjectDetailResult>(
        `${API_BASE}/api/project-detail?project_id=${encodeURIComponent(projectId)}`,
      );
      setProjectDetail((prev) => ({ ...prev, [projectId]: detail }));
    } catch {
      setProjectDetail((prev) => ({ ...prev, [projectId]: null }));
    } finally {
      setDetailLoading((prev) => ({ ...prev, [projectId]: false }));
    }
  };

  const toggleExpanded = (projectId: string) => {
    setExpandedProject((prev) => (prev === projectId ? null : projectId));
    if (expandedProject !== projectId) {
      void loadDetail(projectId);
    }
  };

  const toggleSelectedProject = (project: StudyProjectInfo) => {
    if (!project.has_control) return;
    setSelectedProjects((prev) => (
      prev.includes(project.project_id)
        ? prev.filter((item) => item !== project.project_id)
        : [...prev, project.project_id]
    ));
  };

  const exportFiltered = () => {
    exportTable(
      filtered.map((project) => ({
        Project_ID: project.project_id,
        Year: project.year ?? "",
        Country: project.country,
        Samples: project.sample_count,
        NC_Samples: project.nc_count,
        Disease_Samples: project.disease_count,
        Has_Control: project.has_control ? "yes" : "no",
        Region_16S_Est: project.region_16s,
        Instrument: project.instrument,
        Diseases: project.diseases.join("; "),
      })),
      `studies_filtered_${Date.now()}`,
    );
  };

  const openCrossStudy = () => {
    const payload = JSON.stringify({
      projectIds: selectedProjects,
      disease: diseaseFilter,
    });
    localStorage.setItem("crossStudyPreselect", payload);
    navigate("/compare?tab=crossstudy");
  };

  const resetFilters = () => {
    setSearch("");
    setCountryFilter("");
    setDiseaseFilter("");
    setControlFilter("all");
    setSortBy("samples");
    setPage(0);
  };

  return (
    <div className={css.page}>
      <div className={css.hero}>
        <Link to="/" className={css.back}>{t("compare.back")}</Link>
        <h1 className={css.title}>{t("studies.title")}</h1>
        <p className={css.subtitle}>{locale === "zh" ? "把项目浏览、数据溯源和跨研究入口放在同一个工作台里。" : "A project browser for cohort provenance, study metadata, and direct cross-study launch."}</p>
      </div>

      {error ? <div className={css.error}>{error}</div> : null}

      <section className={css.section}>
        <div className={css.statGrid}>
          <StatCard label={locale === "zh" ? "项目数" : "Projects"} value={summary.total_projects} />
          <StatCard label={locale === "zh" ? "样本数" : "Samples"} value={summary.total_samples} />
          <StatCard label="NC" value={summary.total_nc} accent="green" />
          <StatCard label={locale === "zh" ? "疾病样本" : "Disease"} value={summary.total_disease} accent="orange" />
          <StatCard label={locale === "zh" ? "国家数" : "Countries"} value={summary.n_countries} />
          <StatCard label={locale === "zh" ? "疾病数" : "Diseases"} value={summary.n_diseases} />
        </div>
      </section>

      <section className={css.section}>
        <div className={css.panelGrid}>
          <div className={css.panel}>
            <div className={css.panelHeader}>
              <div>
                <h2>{locale === "zh" ? "项目地图" : "Projects Map"}</h2>
                <p>{locale === "zh" ? "按国家着色，点击国家可过滤表格。" : "Colored by project count per country. Click to filter the table."}</p>
              </div>
              <button type="button" className={css.ghostBtn} onClick={() => setCountryFilter("")}>
                {locale === "zh" ? "清空国家筛选" : "Clear country filter"}
              </button>
            </div>
            {loading ? (
              <div className={css.loading}>{locale === "zh" ? "正在加载地图…" : "Loading map..."}</div>
            ) : (
              <StudiesMap counts={projectCountsByCountry} selectedCountry={countryFilter} onSelectCountry={setCountryFilter} />
            )}
          </div>

          <div className={css.panel}>
            <div className={css.panelHeader}>
              <div>
                <h2>{locale === "zh" ? "年份时间轴" : "Timeline"}</h2>
                <p>{locale === "zh" ? "样本规模和项目增长共用一条时间轴。" : "Sample growth and project growth on a shared year axis."}</p>
              </div>
              <span className={css.metaText}>
                {summary.year_range.length === 2 ? `${summary.year_range[0]}–${summary.year_range[1]}` : "—"}
              </span>
            </div>
            {loading ? (
              <div className={css.loading}>{locale === "zh" ? "正在加载时间轴…" : "Loading timeline..."}</div>
            ) : (
              <TimelineChart timeline={timeline} />
            )}
          </div>
        </div>
      </section>

      <section className={css.section}>
        <div className={css.tableToolbar}>
          <div className={css.filterBar}>
            <input
              className={css.searchInput}
              type="text"
              value={search}
              placeholder={t("studies.searchPlaceholder")}
              onChange={(event) => setSearch(event.target.value)}
            />

            <select className={css.select} value={countryFilter} onChange={(event) => setCountryFilter(event.target.value)}>
              <option value="">{locale === "zh" ? "全部国家" : "All countries"}</option>
              {countryOptions.map((country) => (
                <option key={country} value={country}>
                  {countryName(country, locale)} ({country})
                </option>
              ))}
            </select>

            <input
              list="studies-disease-list"
              className={css.select}
              value={diseaseFilter}
              placeholder={locale === "zh" ? "按疾病过滤" : "Filter by disease"}
              onChange={(event) => setDiseaseFilter(event.target.value)}
            />
            <datalist id="studies-disease-list">
              {diseaseOptions.map((disease) => (
                <option key={disease} value={disease} label={diseaseDisplayNameI18n(disease, locale)} />
              ))}
            </datalist>

            <select className={css.select} value={controlFilter} onChange={(event) => setControlFilter(event.target.value as ControlFilter)}>
              <option value="all">{locale === "zh" ? "NC 过滤：全部" : "NC filter: all"}</option>
              <option value="yes">{locale === "zh" ? "仅含 NC" : "With NC only"}</option>
              <option value="no">{locale === "zh" ? "仅无 NC" : "Without NC only"}</option>
            </select>

            <select className={css.select} value={sortBy} onChange={(event) => setSortBy(event.target.value as SortKey)}>
              <option value="samples">{t("studies.sortSamples")}</option>
              <option value="diseases">{t("studies.sortDiseases")}</option>
              <option value="year">{locale === "zh" ? "按年份" : "Sort by year"}</option>
              <option value="nc">{locale === "zh" ? "按 NC 样本" : "Sort by NC samples"}</option>
            </select>
          </div>

          <div className={css.toolbarMeta}>
            <span>{filtered.length.toLocaleString("en-US")} / {projects.length.toLocaleString("en-US")} {t("studies.projects")}</span>
            <button type="button" className={css.ghostBtn} onClick={resetFilters}>
              {locale === "zh" ? "重置筛选" : "Reset filters"}
            </button>
            <button type="button" className={css.exportBtn} onClick={exportFiltered}>
              {locale === "zh" ? "导出筛选结果" : "Export filtered CSV"}
            </button>
          </div>
        </div>

        <div className={css.tableWrap}>
          {loading ? (
            <div className={css.loading}>{locale === "zh" ? "正在加载项目表…" : "Loading projects..."}</div>
          ) : filtered.length === 0 ? (
            <div className={css.loading}>{locale === "zh" ? "当前筛选没有命中项目。" : "No projects match the current filters."}</div>
          ) : (
            <table className={css.table}>
              <thead>
                <tr>
                  <th aria-label="expand" />
                  <th>{t("studies.projectId")}</th>
                  <th>{locale === "zh" ? "年份" : "Year"}</th>
                  <th>{locale === "zh" ? "国家" : "Country"}</th>
                  <th>{t("studies.samples")}</th>
                  <th>NC</th>
                  <th>{locale === "zh" ? "疾病样本" : "Disease"}</th>
                  <th>{locale === "zh" ? "16S 区域 (est.)" : "16S Region (est.)"}</th>
                  <th>{t("studies.diseases")}</th>
                  <th>{locale === "zh" ? "选择" : "Select"}</th>
                </tr>
              </thead>
              <tbody>
                {pageData.map((project) => (
                  <Fragment key={project.project_id}>
                    <tr className={css.dataRow} onClick={() => toggleExpanded(project.project_id)}>
                      <td className={css.expandCell}>{expandedProject === project.project_id ? "−" : "+"}</td>
                      <td>
                        <a
                          href={`https://www.ncbi.nlm.nih.gov/bioproject/${project.project_id}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className={css.projectLink}
                          onClick={(event) => event.stopPropagation()}
                        >
                          {project.project_id}
                        </a>
                      </td>
                      <td>{project.year ?? "—"}</td>
                      <td>
                        <div className={css.countryCell}>
                          <span>{countryName(project.country, locale)}</span>
                          {project.country && project.country.toLowerCase() !== "unknown" ? <small>{project.country}</small> : null}
                        </div>
                      </td>
                      <td>{project.sample_count.toLocaleString("en-US")}</td>
                      <td className={css.ncValue}>{project.nc_count > 0 ? project.nc_count.toLocaleString("en-US") : "—"}</td>
                      <td className={css.diseaseValue}>{project.disease_count > 0 ? project.disease_count.toLocaleString("en-US") : "—"}</td>
                      <td className={css.dimText}>{project.region_16s}</td>
                      <td>
                        <div className={css.diseaseTags}>
                          {project.diseases.slice(0, 4).map((disease) => (
                            <span key={disease} className={css.tag}>
                              {diseaseDisplayNameI18n(disease, locale)}
                            </span>
                          ))}
                          {project.diseases.length > 4 ? <span className={css.tagMore}>+{project.diseases.length - 4}</span> : null}
                        </div>
                      </td>
                      <td onClick={(event) => event.stopPropagation()}>
                        <input
                          type="checkbox"
                          checked={selectedProjects.includes(project.project_id)}
                          disabled={!project.has_control}
                          onChange={() => toggleSelectedProject(project)}
                        />
                      </td>
                    </tr>
                    {expandedProject === project.project_id ? (
                      <tr>
                        <td colSpan={10} className={css.detailRow}>
                          <ProjectDetailPanel
                            detail={projectDetail[project.project_id] ?? null}
                            locale={locale}
                            loading={detailLoading[project.project_id]}
                          />
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className={css.pagination}>
          <button type="button" disabled={page === 0} onClick={() => setPage(0)}>«</button>
          <button type="button" disabled={page === 0} onClick={() => setPage((prev) => Math.max(0, prev - 1))}>‹</button>
          <span>{page + 1} / {totalPages}</span>
          <button type="button" disabled={page + 1 >= totalPages} onClick={() => setPage((prev) => Math.min(totalPages - 1, prev + 1))}>›</button>
          <button type="button" disabled={page + 1 >= totalPages} onClick={() => setPage(totalPages - 1)}>»</button>
        </div>
      </section>

      {selectedProjects.length >= 2 ? (
        <div className={css.floatingBar}>
          <div>
            <strong>{selectedProjects.length}</strong>
            <span>{locale === "zh" ? " 个项目已选中，可直接进入 Cross-study。" : " projects selected for Cross-study."}</span>
          </div>
          <button type="button" className={css.runBtn} onClick={openCrossStudy}>
            {locale === "zh" ? "在 Compare 中打开 Cross-study" : "Open Cross-study in Compare"}
          </button>
        </div>
      ) : null}
    </div>
  );
};

const StatCard = ({ label, value, accent }: { label: string; value: number; accent?: "green" | "orange" }) => (
  <div className={css.statCard} data-accent={accent ?? "default"}>
    <span>{label}</span>
    <strong>{value.toLocaleString("en-US")}</strong>
  </div>
);

export default StudiesPage;
