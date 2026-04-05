import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { cachedFetch } from "@/util/apiCache";
import { countryName, AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import "@/components/tooltip";
import BiomarkerPanel from "./disease/BiomarkerPanel";
import DemoBarChart from "./disease/DemoBarChart";
import LollipopPanel from "./disease/LollipopPanel";
import PrevalenceDotPlot from "./disease/PrevalenceDotPlot";
import StudiesPanel from "./disease/StudiesPanel";
import type { DiseaseListItem, DiseaseProfile, GenusEntry } from "./disease/types";
import classes from "./DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

type TabKey = "profile" | "biomarker" | "lollipop" | "studies";

const formatP = (value: number) => {
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const sigLabel = (value: number) => {
  if (value < 0.001) return "***";
  if (value < 0.01) return "**";
  if (value < 0.05) return "*";
  return "ns";
};

const DiseasePage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseListItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("all");
  const [sortMode, setSortMode] = useState<"count" | "alpha">("count");
  const [selected, setSelected] = useState<string | null>(null);
  const [profile, setProfile] = useState<DiseaseProfile | null>(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<TabKey>("profile");

  useEffect(() => {
    cachedFetch<{ diseases: DiseaseListItem[] }>(`${API_BASE}/api/disease-list`)
      .then((data) => setDiseases(data.diseases ?? []))
      .catch(() => {});
    cachedFetch<Record<string, string>>(`${API_BASE}/api/disease-names-zh`)
      .then(setDiseaseZh)
      .catch(() => {});
  }, []);

  const diseaseName = useCallback(
    (name: string) => (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : diseaseDisplayNameI18n(name, locale),
    [diseaseZh, locale],
  );

  const categories = useMemo(() => {
    const values = diseases
      .map((item) => ({
        key: item.category?.trim() ?? "",
        label: locale === "zh" ? (item.category_zh?.trim() || item.category?.trim() || "") : (item.category?.trim() || ""),
      }))
      .filter((item) => item.key);
    const dedup = new Map<string, string>();
    values.forEach((item) => {
      if (!dedup.has(item.key)) dedup.set(item.key, item.label);
    });
    return Array.from(dedup.entries()).map(([key, label]) => ({ key, label }));
  }, [diseases, locale]);

  const filteredDiseases = useMemo(() => {
    let list = diseases;
    const query = search.trim().toLowerCase();
    if (query) {
      list = list.filter((item) => {
        const zh = diseaseZh[item.name] ?? item.standard_name_zh ?? "";
        return (
          item.name.toLowerCase().includes(query)
          || zh.toLowerCase().includes(query)
          || diseaseDisplayNameI18n(item.name, "en").toLowerCase().includes(query)
        );
      });
    }
    if (categoryFilter !== "all") {
      list = list.filter((item) => item.category === categoryFilter);
    }
    const sorted = [...list];
    if (sortMode === "count") {
      sorted.sort((a, b) => b.sample_count - a.sample_count);
    } else {
      sorted.sort((a, b) => diseaseName(a.name).localeCompare(diseaseName(b.name)));
    }
    return sorted;
  }, [categoryFilter, diseaseName, diseaseZh, diseases, search, sortMode]);

  const selectDisease = useCallback((name: string) => {
    setSelected(name);
    setActiveTab("profile");
    setLoading(true);
    setProfile(null);
    cachedFetch<DiseaseProfile>(`${API_BASE}/api/disease-profile?disease=${encodeURIComponent(name)}&top_n=40`)
      .then(setProfile)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("disease.back")}</Link>
        <h1>{t("disease.title")}</h1>
        <p>{t("disease.subtitle")}</p>
      </div>

      <div className={classes.layout}>
        <aside className={classes.sidebar}>
          <div className={classes.searchBox}>
            <input
              type="text"
              placeholder={t("disease.searchPlaceholder")}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              className={classes.searchInput}
            />
            <div className={classes.sidebarControls}>
              <div className={classes.field}>
                <label>{t("disease.sidebar.category")}</label>
                <select
                  className={classes.inlineSelect}
                  value={categoryFilter}
                  onChange={(event) => setCategoryFilter(event.target.value)}
                >
                  <option value="all">{locale === "zh" ? "全部类别" : "All categories"}</option>
                  {categories.map((category) => (
                    <option key={category.key} value={category.key}>{category.label}</option>
                  ))}
                </select>
              </div>
              <div className={classes.field}>
                <label>{t("disease.sidebar.sortBy")}</label>
                <div className={classes.sortTabs}>
                  <button
                    className={sortMode === "count" ? classes.sortTabActive : classes.sortTab}
                    onClick={() => setSortMode("count")}
                  >
                    {t("disease.sidebar.sortCount")}
                  </button>
                  <button
                    className={sortMode === "alpha" ? classes.sortTabActive : classes.sortTab}
                    onClick={() => setSortMode("alpha")}
                  >
                    {t("disease.sidebar.sortAlpha")}
                  </button>
                </div>
              </div>
            </div>
          </div>

          <div className={classes.diseaseList}>
            {filteredDiseases.map((item) => (
              <button
                key={item.name}
                className={selected === item.name ? classes.diseaseItemActive : classes.diseaseItem}
                onClick={() => selectDisease(item.name)}
              >
                <div className={classes.diseaseMeta}>
                  <span>{diseaseName(item.name)}</span>
                  {(locale === "zh" ? item.category_zh : item.category) && (
                    <span className={classes.diseaseCategory}>
                      {locale === "zh" ? item.category_zh : item.category}
                    </span>
                  )}
                </div>
                <span className={classes.diseaseCount}>{item.sample_count.toLocaleString("en")}</span>
              </button>
            ))}
          </div>
        </aside>

        <main className={classes.mainPanel}>
          {!selected && !loading && <div className={classes.selectHint}>{t("disease.selectHint")}</div>}
          {loading && <div className={classes.loading}>{t("search.searching")}</div>}

          {selected && !loading && (
            <>
              <div className={classes.tabs}>
                <button className={activeTab === "profile" ? classes.tabActive : classes.tab} onClick={() => setActiveTab("profile")}>
                  {t("disease.tabProfile")}
                </button>
                <button className={activeTab === "biomarker" ? classes.tabActive : classes.tab} onClick={() => setActiveTab("biomarker")}>
                  {t("disease.tabBiomarker")}
                </button>
                <button className={activeTab === "lollipop" ? classes.tabActive : classes.tab} onClick={() => setActiveTab("lollipop")}>
                  {t("disease.tabLollipop")}
                </button>
                <button className={activeTab === "studies" ? classes.tabActive : classes.tab} onClick={() => setActiveTab("studies")}>
                  {t("disease.tabStudies")}
                </button>
              </div>

              {activeTab === "profile" && profile && <ProfileView profile={profile} diseaseName={diseaseName} locale={locale} />}
              {activeTab === "biomarker" && <BiomarkerPanel disease={selected} />}
              {activeTab === "lollipop" && <LollipopPanel disease={selected} />}
              {activeTab === "studies" && <StudiesPanel disease={selected} />}
            </>
          )}
        </main>
      </div>
    </div>
  );
};

interface ProfileViewProps {
  profile: DiseaseProfile;
  diseaseName: (name: string) => string;
  locale: string;
}

const ProfileView = ({ profile, diseaseName, locale }: ProfileViewProps) => {
  const { t } = useI18n();
  const chartRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!chartRef.current || profile.top_genera.length === 0) return;
    drawComparisonChart(chartRef.current, profile.top_genera.slice(0, 20), locale, diseaseName(profile.disease));
  }, [profile, locale, diseaseName]);

  const exportProfileCsv = () => {
    exportTable(
      profile.top_genera.map((item) => ({
        Genus: item.genus,
        Phylum: item.phylum,
        Disease_Mean: item.disease_mean,
        Control_Mean: item.control_mean,
        Log2FC: item.log2fc,
        Adjusted_P: item.adjusted_p,
        Disease_Prevalence: item.disease_prevalence,
        Control_Prevalence: item.control_prevalence,
      })),
      `disease_${profile.disease}_profile`,
    );
  };

  return (
    <div>
      <div className={classes.profileHeader}>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.sample_count.toLocaleString("en")}</span>
          <span className={classes.statLabel}>{diseaseName(profile.disease)} {t("disease.samples")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.control_count.toLocaleString("en")}</span>
          <span className={classes.statLabel}>{t("disease.controlSamples")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.n_studies}</span>
          <span className={classes.statLabel}>{t("disease.studies.nProjects")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.top_genera.length}</span>
          <span className={classes.statLabel}>{t("disease.topGenera")}</span>
        </div>
      </div>

      <div className={classes.profileMeta}>
        {(locale === "zh" ? profile.category_zh : profile.category) && <span className={classes.metaChip}>{locale === "zh" ? profile.category_zh : profile.category}</span>}
        {profile.standard_name && <span className={classes.metaChip}>{t("disease.standardName")}: {profile.standard_name}</span>}
        {profile.mesh_id && <a className={classes.metaChipLink} href={`https://meshb.nlm.nih.gov/record/ui?ui=${profile.mesh_id}`} target="_blank" rel="noreferrer">{t("disease.meshId")}: {profile.mesh_id}</a>}
        {profile.icd10 && <span className={classes.metaChip}>{t("disease.icd10")}: {profile.icd10}</span>}
      </div>

      <div className={classes.chartCard}>
        <div className={classes.cardHeader}>
          <h3>{t("disease.topGenera")}</h3>
          <div className={classes.exportActions}>
            <button onClick={exportProfileCsv}>{t("export.csv")}</button>
            <button onClick={() => chartRef.current && exportSVG(chartRef.current, `disease_${profile.disease}_comparison`)}>{t("export.svg")}</button>
            <button onClick={() => chartRef.current && exportPNG(chartRef.current, `disease_${profile.disease}_comparison`)}>{t("export.png")}</button>
          </div>
        </div>
        <svg ref={chartRef} className={classes.chart} />
      </div>

      <div className={classes.chartCard}>
        <h3>{t("disease.prevalenceDot.title")}</h3>
        <PrevalenceDotPlot data={profile.top_genera} locale={locale} />
      </div>

      <div className={classes.chartCard}>
        <h3>{t("disease.topGenera")} / {t("disease.log2fc")}</h3>
        <table className={classes.generaTable}>
          <thead>
            <tr>
              <th>{t("disease.genus")}</th>
              <th>{t("disease.genera.phylum")}</th>
              <th>{t("disease.diseaseMean")}</th>
              <th>{t("disease.controlMean")}</th>
              <th>{t("disease.log2fc")}</th>
              <th>{t("disease.genera.adjP")}</th>
              <th>{t("disease.genera.sig")}</th>
              <th>{t("disease.genera.prevD")}</th>
              <th>{t("disease.genera.prevC")}</th>
            </tr>
          </thead>
          <tbody>
            {profile.top_genera.slice(0, 20).map((item) => (
              <tr key={item.genus}>
                <td>
                  <Link to={`/species/${encodeURIComponent(item.genus)}`} className={classes.genusLink}>
                    {item.genus}
                  </Link>
                </td>
                <td><span className={classes.phylumBadge}>{item.phylum}</span></td>
                <td>{item.disease_mean.toFixed(3)}%</td>
                <td>{item.control_mean.toFixed(3)}%</td>
                <td className={item.log2fc > 0 ? classes.enriched : classes.depleted}>
                  {item.log2fc > 0 ? "+" : ""}{item.log2fc.toFixed(2)}
                </td>
                <td>{formatP(item.adjusted_p)}</td>
                <td><span className={classes.sigBadge}>{sigLabel(item.adjusted_p)}</span></td>
                <td>{(item.disease_prevalence * 100).toFixed(1)}%</td>
                <td>{(item.control_prevalence * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className={classes.demoGrid}>
        <div className={classes.chartCard}>
          <h3>{t("disease.bySex")}</h3>
          <DemoBarChart data={profile.by_sex} locale={locale} formatter={(name) => locale === "zh" ? (SEX_ZH[name] ?? name) : name} />
        </div>
        <div className={classes.chartCard}>
          <h3>{t("disease.byAgeGroup")}</h3>
          <DemoBarChart data={profile.by_age_group} locale={locale} orientation="vertical" formatter={(name) => locale === "zh" ? (AGE_GROUP_ZH[name] ?? name.replace(/_/g, " ")) : name.replace(/_/g, " ")} />
        </div>
        <div className={classes.chartCard}>
          <h3>{t("disease.byCountry")}</h3>
          <DemoBarChart data={profile.by_country.slice(0, 10)} locale={locale} formatter={(name) => countryName(name, locale)} />
        </div>
      </div>
    </div>
  );
};

function drawComparisonChart(
  svgEl: SVGSVGElement,
  data: GenusEntry[],
  locale: string,
  diseaseLabel: string,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const margin = { top: 16, right: 28, bottom: 40, left: 176 };
  const width = 880;
  const height = Math.max(360, data.length * 26 + margin.top + margin.bottom);
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;

  svg.attr("viewBox", `0 0 ${width} ${height}`);
  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const maxValue = d3.max(data, (item) => Math.max(item.disease_mean, item.control_mean)) ?? 1;
  const x = d3.scaleLinear().domain([0, maxValue * 1.15]).range([0, innerWidth]);
  const y = d3.scaleBand().domain(data.map((item) => item.genus)).range([0, innerHeight]).padding(0.22);
  const barHeight = y.bandwidth() / 2;

  root.append("g")
    .call(d3.axisLeft(y).tickFormat((genus) => genus.length > 18 ? `${genus.slice(0, 16)}…` : genus))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("font-style", "italic");

  root.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(d3.axisBottom(x).ticks(5).tickFormat((value) => `${Number(value).toFixed(2)}%`))
    .attr("font-size", 10);

  root.selectAll(".disease-bar")
    .data(data)
    .join("rect")
    .attr("class", "disease-bar")
    .attr("x", 0)
    .attr("y", (item) => y(item.genus) ?? 0)
    .attr("width", (item) => x(item.disease_mean))
    .attr("height", barHeight)
    .attr("rx", 3)
    .attr("fill", "#f97316")
    .attr("data-tooltip", (item) =>
      renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "菌属" : "Genus"}</span><span><i>{item.genus}</i></span>
          <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{item.phylum}</span>
          <span>{locale === "zh" ? "疾病均值" : "Disease mean"}</span><span>{item.disease_mean.toFixed(3)}%</span>
          <span>{locale === "zh" ? "疾病流行率" : "Disease prevalence"}</span><span>{(item.disease_prevalence * 100).toFixed(1)}%</span>
          <span>adj.p</span><span>{formatP(item.adjusted_p)}</span>
        </div>,
      ),
    );

  root.selectAll(".control-bar")
    .data(data)
    .join("rect")
    .attr("class", "control-bar")
    .attr("x", 0)
    .attr("y", (item) => (y(item.genus) ?? 0) + barHeight)
    .attr("width", (item) => x(item.control_mean))
    .attr("height", barHeight)
    .attr("rx", 3)
    .attr("fill", "#14b8a6")
    .attr("data-tooltip", (item) =>
      renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "菌属" : "Genus"}</span><span><i>{item.genus}</i></span>
          <span>{locale === "zh" ? "对照均值" : "Control mean"}</span><span>{item.control_mean.toFixed(3)}%</span>
          <span>{locale === "zh" ? "对照流行率" : "Control prevalence"}</span><span>{(item.control_prevalence * 100).toFixed(1)}%</span>
          <span>log2FC</span><span>{item.log2fc.toFixed(3)}</span>
          <span>adj.p</span><span>{formatP(item.adjusted_p)}</span>
        </div>,
      ),
    );

  const legend = svg.append("g").attr("transform", `translate(${margin.left + 8}, ${height - 10})`);
  legend.append("rect").attr("width", 12).attr("height", 8).attr("rx", 2).attr("fill", "#f97316");
  legend.append("text").attr("x", 18).attr("y", 7).attr("fill", "currentColor").attr("font-size", 10).text(diseaseLabel);
  legend.append("rect").attr("x", 132).attr("width", 12).attr("height", 8).attr("rx", 2).attr("fill", "#14b8a6");
  legend.append("text").attr("x", 150).attr("y", 7).attr("fill", "currentColor").attr("font-size", 10).text(locale === "zh" ? "健康对照" : "Healthy controls");
}

export default DiseasePage;
