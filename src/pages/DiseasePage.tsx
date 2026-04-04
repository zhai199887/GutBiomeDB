/**
 * DiseasePage.tsx — Disease Browser
 * 疾病浏览器：左侧疾病列表 + 右侧 Top20 属丰度画像 & 健康对照比较
 * Ref: GMrepo disease-species browser
 */
import { useEffect, useRef, useState, useCallback } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayName, diseaseDisplayNameI18n } from "@/util/diseaseNames";
import "@/components/tooltip";
import BiomarkerPanel from "./disease/BiomarkerPanel";
import LollipopPanel from "./disease/LollipopPanel";
import classes from "./DiseasePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface DiseaseItem {
  name: string;
  sample_count: number;
}

interface GenusEntry {
  genus: string;
  disease_mean: number;
  disease_prevalence: number;
  control_mean: number;
  control_prevalence: number;
  log2fc: number;
}

interface DemoEntry {
  name: string;
  count: number;
}

interface DiseaseProfile {
  disease: string;
  sample_count: number;
  control_count: number;
  standard_name?: string;
  standard_name_zh?: string;
  abbreviation?: string;
  mesh_id?: string;
  icd10?: string;
  category?: string;
  category_zh?: string;
  top_genera: GenusEntry[];
  by_country: DemoEntry[];
  by_age_group: DemoEntry[];
  by_sex: DemoEntry[];
}

const DiseasePage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [filtered, setFiltered] = useState<DiseaseItem[]>([]);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [profile, setProfile] = useState<DiseaseProfile | null>(null);
  const [loading, setLoading] = useState(false);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [activeTab, setActiveTab] = useState<"profile" | "biomarker" | "lollipop">("profile");

  // Load disease list + Chinese names / 加载疾病列表 + 中文名
  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`)
      .then((r) => r.json())
      .then((data) => {
        setDiseases(data.diseases ?? []);
        setFiltered(data.diseases ?? []);
      })
      .catch(() => {});
    fetch(`${API_BASE}/api/disease-names-zh`)
      .then((r) => r.json())
      .then(setDiseaseZh)
      .catch(() => {});
  }, []);

  // Helper: translate disease name / 翻译疾病名（优先用后端中文名，fallback 到内嵌映射）
  const dName = (name: string) => (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : diseaseDisplayNameI18n(name, locale);

  // Filter diseases (search both English and Chinese names) / 筛选疾病（支持中英文搜索）
  useEffect(() => {
    if (!search.trim()) {
      setFiltered(diseases);
    } else {
      const q = search.trim().toLowerCase();
      setFiltered(diseases.filter((d) =>
        d.name.toLowerCase().includes(q) ||
        (diseaseZh[d.name] ?? "").toLowerCase().includes(q)
      ));
    }
  }, [search, diseases, diseaseZh]);

  // Select disease / 选择疾病
  const selectDisease = useCallback((name: string) => {
    setSelected(name);
    setActiveTab("profile");
    setLoading(true);
    setProfile(null);
    fetch(`${API_BASE}/api/disease-profile?disease=${encodeURIComponent(name)}`)
      .then((r) => {
        if (!r.ok) throw new Error("Not found");
        return r.json();
      })
      .then((data: DiseaseProfile) => setProfile(data))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className={classes.page}>
      {/* Top bar */}
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("disease.back")}</Link>
        <h1>{t("disease.title")}</h1>
        <p>{t("disease.subtitle")}</p>
      </div>

      {/* Two-column layout */}
      <div className={classes.layout}>
        {/* Sidebar — disease list */}
        <div className={classes.sidebar}>
          <div className={classes.searchBox}>
            <input
              type="text"
              placeholder={t("disease.searchPlaceholder")}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className={classes.searchInput}
            />
          </div>
          <div className={classes.diseaseList}>
            {filtered.map((d) => (
              <button
                key={d.name}
                className={selected === d.name ? classes.diseaseItemActive : classes.diseaseItem}
                onClick={() => selectDisease(d.name)}
              >
                <span>{dName(d.name)}</span>
                <span className={classes.diseaseCount}>
                  {d.sample_count.toLocaleString("en")}
                </span>
              </button>
            ))}
          </div>
        </div>

        {/* Main panel — profile */}
        <div className={classes.mainPanel}>
          {!selected && !loading && (
            <div className={classes.selectHint}>{t("disease.selectHint")}</div>
          )}

          {loading && (
            <div className={classes.loading}>{t("search.searching")}</div>
          )}

          {/* Tab 栏 — 选中疾病后显示 */}
          {selected && !loading && (
            <div className={classes.tabs}>
              <button
                className={activeTab === "profile" ? classes.tabActive : classes.tab}
                onClick={() => setActiveTab("profile")}
              >
                {t("disease.tabProfile") ?? "画像"}
              </button>
              <button
                className={activeTab === "biomarker" ? classes.tabActive : classes.tab}
                onClick={() => setActiveTab("biomarker")}
              >
                {t("disease.tabBiomarker") ?? "标志物"}
              </button>
              <button
                className={activeTab === "lollipop" ? classes.tabActive : classes.tab}
                onClick={() => setActiveTab("lollipop")}
              >
                {t("disease.tabLollipop") ?? "差异丰度"}
              </button>
            </div>
          )}

          {/* Tab 内容 */}
          {activeTab === "profile" && profile && <ProfileView profile={profile} dName={dName} locale={locale} />}
          {activeTab === "biomarker" && selected && <BiomarkerPanel disease={selected} />}
          {activeTab === "lollipop" && selected && <LollipopPanel disease={selected} />}
        </div>
      </div>
    </div>
  );
};

export default DiseasePage;

// ── Profile visualization / 画像可视化 ─────────────────────────────────────

const ProfileView = ({ profile, dName, locale }: { profile: DiseaseProfile; dName: (n: string) => string; locale: string }) => {
  const { t } = useI18n();
  const barRef = useRef<SVGSVGElement>(null);

  // Draw comparison bar chart / 绘制对比柱状图
  useEffect(() => {
    if (!barRef.current || profile.top_genera.length === 0) return;
    drawComparisonChart(barRef.current, profile.top_genera.slice(0, 20));
  }, [profile]);

  const exportProfileCsv = () => {
    if (!profile) return;
    exportTable(
      profile.top_genera.map((g: GenusEntry) => ({
        Genus: g.genus,
        Disease_Mean: g.disease_mean,
        Control_Mean: g.control_mean,
        Log2FC: g.log2fc,
        Disease_Prevalence: g.disease_prevalence,
        Control_Prevalence: g.control_prevalence,
      })),
      `disease_${profile.disease}_top_genera`,
    );
  };

  const exportProfileChart = (type: "svg" | "png") => {
    const svg = barRef.current;
    if (!svg) return;
    type === "svg"
      ? exportSVG(svg, `disease_${profile?.disease}_chart`)
      : exportPNG(svg, `disease_${profile?.disease}_chart`);
  };

  return (
    <div>
      {/* Stats header */}
      <div className={classes.profileHeader}>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.sample_count.toLocaleString("en")}</span>
          <span className={classes.statLabel}>
            {dName(profile.disease)}
            {profile.standard_name && profile.standard_name !== profile.disease && (
              <span style={{ fontSize: "0.85rem", color: "#666", marginLeft: "0.5rem" }}>
                ({profile.standard_name})
              </span>
            )}
            {" "}{t("disease.samples")}
          </span>
          <span style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", marginTop: "0.3rem", justifyContent: "center" }}>
            {profile.category && (
              <span style={{
                fontSize: "0.75rem", padding: "0.15rem 0.5rem",
                background: "#e8f4f8", borderRadius: "12px", color: "#2980b9",
              }}>
                {locale === "zh" ? profile.category_zh : profile.category}
              </span>
            )}
            {profile.mesh_id && (
              <a href={`https://meshb.nlm.nih.gov/record/ui?ui=${profile.mesh_id}`}
                 target="_blank" rel="noopener noreferrer"
                 style={{ fontSize: "0.75rem", color: "#888", textDecoration: "none" }}>
                MeSH: {profile.mesh_id}
              </a>
            )}
            {profile.icd10 && (
              <span style={{ fontSize: "0.75rem", color: "#888" }}>
                ICD-10: {profile.icd10}
              </span>
            )}
          </span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.control_count.toLocaleString("en")}</span>
          <span className={classes.statLabel}>{t("disease.controlSamples")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{profile.top_genera.length}</span>
          <span className={classes.statLabel}>{t("disease.topGenera")}</span>
        </div>
      </div>

      {/* Comparison bar chart */}
      <div className={classes.chartCard}>
        <h3>{t("disease.topGenera")}</h3>
        <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.5rem" }}>
          <button onClick={exportProfileCsv} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
          <button onClick={() => exportProfileChart("svg")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
          <button onClick={() => exportProfileChart("png")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
        </div>
        <svg ref={barRef} className={classes.chart} />
      </div>

      {/* Genera table */}
      <div className={classes.chartCard}>
        <h3>{t("disease.topGenera")} — {t("disease.log2fc")}</h3>
        <table className={classes.generaTable}>
          <thead>
            <tr>
              <th>{t("disease.genus")}</th>
              <th>{t("disease.diseaseMean")}</th>
              <th>{t("disease.controlMean")}</th>
              <th>{t("disease.log2fc")}</th>
              <th>{t("disease.prevalence")}</th>
            </tr>
          </thead>
          <tbody>
            {profile.top_genera.slice(0, 20).map((g) => (
              <tr key={g.genus}>
                <td>
                  <Link to={`/species/${encodeURIComponent(g.genus)}`} className={classes.genusLink}>
                    {g.genus}
                  </Link>
                </td>
                <td>{g.disease_mean.toFixed(4)}%</td>
                <td>{g.control_mean.toFixed(4)}%</td>
                <td className={g.log2fc > 0 ? classes.enriched : classes.depleted}>
                  {g.log2fc > 0 ? "+" : ""}{g.log2fc.toFixed(2)}
                </td>
                <td>{(g.disease_prevalence * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Demographics */}
      <div className={classes.demoGrid}>
        {profile.by_sex.length > 0 && (
          <div className={classes.chartCard}>
            <h3>{t("disease.bySex")}</h3>
            <DemoTable data={profile.by_sex} />
          </div>
        )}
        {profile.by_age_group.length > 0 && (
          <div className={classes.chartCard}>
            <h3>{t("disease.byAgeGroup")}</h3>
            <DemoTable data={profile.by_age_group} />
          </div>
        )}
        {profile.by_country.length > 0 && (
          <div className={classes.chartCard}>
            <h3>{t("disease.byCountry")}</h3>
            <DemoTable data={profile.by_country} />
          </div>
        )}
      </div>
    </div>
  );
};

const DemoTable = ({ data }: { data: DemoEntry[] }) => (
  <table className={classes.miniTable}>
    <thead>
      <tr><th>Name</th><th>Count</th></tr>
    </thead>
    <tbody>
      {data.map((d) => (
        <tr key={d.name}>
          <td>{d.name}</td>
          <td>{d.count.toLocaleString("en")}</td>
        </tr>
      ))}
    </tbody>
  </table>
);

// ── Grouped bar chart: disease vs control / 分组柱状图：疾病 vs 对照 ────────

function drawComparisonChart(svgEl: SVGSVGElement, data: GenusEntry[]) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const margin = { top: 10, right: 20, bottom: 30, left: 130 };
  const W = 700, H = Math.max(300, data.length * 28 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const maxVal = d3.max(data, (d) => Math.max(d.disease_mean, d.control_mean)) ?? 0.001;

  const yScale = d3.scaleBand()
    .domain(data.map((d) => d.genus))
    .range([0, iH])
    .padding(0.25);

  const xScale = d3.scaleLinear()
    .domain([0, maxVal * 1.1])
    .range([0, iW]);

  const barH = yScale.bandwidth() / 2;

  // Disease bars / 疾病组柱
  g.selectAll(".bar-disease")
    .data(data)
    .join("rect")
    .attr("x", 0)
    .attr("y", (d) => yScale(d.genus) ?? 0)
    .attr("width", (d) => xScale(d.disease_mean))
    .attr("height", barH)
    .attr("fill", "#ff6b6b")
    .attr("opacity", 0.8)
    .attr("rx", 2)
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>Genus</span><span><i>{d.genus}</i></span>
          <span>Disease Mean</span><span>{d.disease_mean.toFixed(4)}%</span>
          <span>Prevalence</span><span>{(d.disease_prevalence * 100).toFixed(1)}%</span>
          <span>Log₂FC</span><span>{d.log2fc.toFixed(2)}</span>
        </div>
      )
    );

  // Control bars / 对照组柱
  g.selectAll(".bar-control")
    .data(data)
    .join("rect")
    .attr("x", 0)
    .attr("y", (d) => (yScale(d.genus) ?? 0) + barH)
    .attr("width", (d) => xScale(d.control_mean))
    .attr("height", barH)
    .attr("fill", "#4ecdc4")
    .attr("opacity", 0.8)
    .attr("rx", 2)
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>Genus</span><span><i>{d.genus}</i></span>
          <span>Control Mean</span><span>{d.control_mean.toFixed(4)}%</span>
          <span>Control Prevalence</span><span>{(d.control_prevalence * 100).toFixed(1)}%</span>
        </div>
      )
    );

  // Y axis / Y轴
  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat((d) => d.length > 18 ? d.slice(0, 16) + "…" : d))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("font-style", "italic");

  // X axis / X轴
  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5).tickFormat((d) => `${Number(d).toFixed(2)}%`))
    .attr("font-size", 9);

  // Legend / 图例
  const legend = svg.append("g").attr("transform", `translate(${margin.left + 10}, ${H - 8})`);
  legend.append("rect").attr("width", 12).attr("height", 8).attr("fill", "#ff6b6b").attr("opacity", 0.8);
  legend.append("text").attr("x", 16).attr("y", 7).text("Disease").attr("fill", "currentColor").attr("font-size", 10);
  legend.append("rect").attr("x", 80).attr("width", 12).attr("height", 8).attr("fill", "#4ecdc4").attr("opacity", 0.8);
  legend.append("text").attr("x", 96).attr("y", 7).text("Control").attr("fill", "currentColor").attr("font-size", 10);
}
