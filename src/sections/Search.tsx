/**
 * Search.tsx — Species Search Engine
 * 物种搜索引擎：输入属名 → 展示该属在疾病/国家/年龄组中的分布
 * Genus search workspace with disease, country, and age-group profile views
 */
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { countryName, AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
import { formatAbundancePercent, formatAbundanceTick } from "@/pages/species/utils";
import "@/components/tooltip";
import classes from "./Search.module.css";

interface ProfileEntry {
  name: string;
  mean_abundance: number;
  prevalence: number;
  sample_count: number;
}

interface SpeciesProfile {
  genus: string;
  total_samples: number;
  present_samples: number;
  prevalence: number;
  mean_abundance: number;
  by_disease: ProfileEntry[];
  by_country: ProfileEntry[];
  by_age_group: ProfileEntry[];
  by_sex: ProfileEntry[];
}

// Common gut genera for quick selection / 常见肠道菌属快速选择
const COMMON_GENERA = [
  "Bacteroides", "Prevotella", "Faecalibacterium", "Bifidobacterium",
  "Lactobacillus", "Roseburia", "Blautia", "Ruminococcus",
  "Akkermansia", "Clostridium", "Eubacterium", "Streptococcus",
  "Enterococcus", "Escherichia", "Fusobacterium", "Coprococcus",
  "Lachnospira", "Dorea", "Alistipes", "Parabacteroides",
  "Megamonas", "Dialister", "Veillonella", "Sutterella",
  "Haemophilus", "Klebsiella", "Collinsella", "Desulfovibrio",
  "Megasphaera", "Phascolarctobacterium",
];

const HISTORY_KEY = "search_history";
function getHistory(): string[] {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]"); } catch { return []; }
}
function saveHistory(q: string) {
  const prev = getHistory();
  const updated = [q, ...prev.filter(x => x !== q)].slice(0, 10);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(updated));
}

const Search = () => {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [history, setHistory] = useState<string[]>(getHistory);
  const [profile, setProfile] = useState<SpeciesProfile | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  // Autocomplete / 自动补全
  useEffect(() => {
    if (query.trim().length < 2) {
      setSuggestions([]);
      return;
    }
    const timer = setTimeout(() => {
      fetch(`${API_BASE}/api/species-search?q=${encodeURIComponent(query.trim())}`)
        .then((r) => r.json())
        .then((data) => {
          setSuggestions(data.results ?? []);
          setShowSuggestions(true);
        })
        .catch(() => setSuggestions([]));
    }, 300);
    return () => clearTimeout(timer);
  }, [query]);

  // Search handler / 搜索处理
  const doSearch = useCallback((genus: string) => {
    if (!genus.trim()) return;
    setLoading(true);
    setError(null);
    setProfile(null);
    setShowSuggestions(false);
    setQuery(genus);

    cachedFetch<SpeciesProfile>(`${API_BASE}/api/species-profile?genus=${encodeURIComponent(genus.trim())}`)
      .then((data) => {
        setProfile(data);
        saveHistory(genus.trim());
        setHistory(getHistory());
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Close suggestions on outside click / 点击外部关闭建议
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(e.target as Node) &&
          inputRef.current && !inputRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
        setShowHistory(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <section>
      <h2>{t("search.title")}</h2>
      <p style={{ color: "var(--light-gray)", marginBottom: "1.5rem" }}>
        {t("search.speciesHint")}
      </p>

      {/* Search bar / 搜索栏 */}
      <div className={classes.searchContainer}>
        <div className={classes.searchBox}>
          <input
            ref={inputRef}
            type="text"
            placeholder={t("search.speciesPlaceholder")}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") doSearch(query);
            }}
            onFocus={() => {
              if (suggestions.length > 0) setShowSuggestions(true);
              else if (!query.trim() && history.length > 0) setShowHistory(true);
            }}
            className={classes.searchInput}
          />
          <button
            className={classes.searchBtn}
            onClick={() => doSearch(query)}
            disabled={loading}
          >
            {loading ? "..." : t("search.go")}
          </button>
        </div>

        {/* Autocomplete suggestions / 自动补全建议 */}
        {showSuggestions && suggestions.length > 0 && (
          <div ref={suggestionsRef} className={classes.suggestions}>
            {suggestions.map((s) => (
              <button
                key={s}
                className={classes.suggestionItem}
                onClick={() => doSearch(s)}
              >
                <i>{s}</i>
              </button>
            ))}
          </div>
        )}

        {/* 搜索历史 */}
        {showHistory && !showSuggestions && history.length > 0 && (
          <div ref={suggestionsRef} className={classes.suggestions}>
            <div className={classes.historyHeader}>
              <span>{t("search.recentSearches")}</span>
              <button className={classes.clearBtn} onClick={() => {
                localStorage.removeItem(HISTORY_KEY);
                setHistory([]);
                setShowHistory(false);
              }}>{t("search.clearHistory")}</button>
            </div>
            {history.map((h) => (
              <button key={h} className={classes.suggestionItem} onClick={() => { setShowHistory(false); doSearch(h); }}>
                {h}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Quick genus picker / 常用菌属快速选择 */}
      <div className={classes.quickPicker}>
        <label className={classes.quickLabel}>{t("search.quickSelect")}</label>
        <select
          className={classes.quickSelect}
          value=""
          onChange={(e) => {
            if (e.target.value) doSearch(e.target.value);
          }}
        >
          <option value="">{t("search.selectGenus")}</option>
          {COMMON_GENERA.map((g) => (
            <option key={g} value={g}>{g}</option>
          ))}
        </select>
      </div>

      {/* Error / 错误信息 */}
      {error && (
        <div className={classes.errorMsg}>
          {error === "Genus not found" ? t("search.notFound") : error}
        </div>
      )}

      {/* Loading / 加载中 */}
      {loading && (
        <div className={classes.loadingMsg}>{t("search.searching")}</div>
      )}

      {/* Species profile / 物种画像 */}
      {profile && <SpeciesProfileView profile={profile} />}
    </section>
  );
};

export default Search;

// ── Species profile visualization / 物种画像可视化 ──────────────────────────

const SpeciesProfileView = ({ profile }: { profile: SpeciesProfile }) => {
  const { t, locale } = useI18n();
  const diseaseRef = useRef<SVGSVGElement>(null);
  const countryRef = useRef<SVGSVGElement>(null);
  const ageRef = useRef<SVGSVGElement>(null);
  const rankProfileEntries = useCallback((entries: ProfileEntry[]) => (
    [...entries].sort((left, right) => (
      right.mean_abundance - left.mean_abundance
      || right.prevalence - left.prevalence
      || right.sample_count - left.sample_count
      || left.name.localeCompare(right.name)
    ))
  ), []);

  const diseaseEntries = useMemo(() => {
    const ranked = rankProfileEntries(profile.by_disease);
    const informative = ranked.filter((item) => item.mean_abundance > 0 || item.prevalence > 0);
    return (informative.length > 0 ? informative : ranked).slice(0, 28);
  }, [profile.by_disease, rankProfileEntries]);

  const countryEntries = useMemo(
    () => rankProfileEntries(profile.by_country).slice(0, 24),
    [profile.by_country, rankProfileEntries],
  );

  // Draw disease bar chart / 绘制疾病丰度柱状图
  useEffect(() => {
    if (!diseaseRef.current || diseaseEntries.length === 0) return;
    drawBarChart(diseaseRef.current, diseaseEntries, "var(--primary)", locale, "disease");
  }, [diseaseEntries, locale]);

  // Draw country bar chart / 绘制国家丰度柱状图
  useEffect(() => {
    if (!countryRef.current || countryEntries.length === 0) return;
    drawBarChart(countryRef.current, countryEntries, "var(--secondary)", locale, "country");
  }, [countryEntries, locale]);

  // Draw age group bar chart / 绘制年龄组丰度柱状图
  useEffect(() => {
    if (!ageRef.current || profile.by_age_group.length === 0) return;
    drawBarChart(ageRef.current, profile.by_age_group, "#4ecdc4", locale, "age");
  }, [profile, locale]);

  const exportSearchCsv = () => {
    const allData = [
      ...profile.by_disease.map((d) => ({ Category: "Disease", Name: d.name, Mean_Abundance: d.mean_abundance, Prevalence: d.prevalence, Sample_Count: d.sample_count })),
      ...profile.by_country.map((d) => ({ Category: "Country", Name: d.name, Mean_Abundance: d.mean_abundance, Prevalence: d.prevalence, Sample_Count: d.sample_count })),
      ...profile.by_age_group.map((d) => ({ Category: "Age_Group", Name: d.name, Mean_Abundance: d.mean_abundance, Prevalence: d.prevalence, Sample_Count: d.sample_count })),
      ...profile.by_sex.map((d) => ({ Category: "Sex", Name: d.name, Mean_Abundance: d.mean_abundance, Prevalence: d.prevalence, Sample_Count: d.sample_count })),
    ];
    exportTable(allData, `species_${profile.genus}_${Date.now()}`);
  };

  const exportSearchChart = (ref: React.RefObject<SVGSVGElement | null>, name: string, type: "svg" | "png") => {
    const svg = ref.current;
    if (!svg) return;
    type === "svg"
      ? exportSVG(svg, `species_${profile.genus}_${name}_${Date.now()}`)
      : exportPNG(svg, `species_${profile.genus}_${name}_${Date.now()}`);
  };

  return (
    <div className={classes.profileContainer}>
      {/* Header card / 物种信息卡 */}
      <div className={classes.profileHeader}>
        <h3 className={classes.genusName}>
          <Link to={`/species/${encodeURIComponent(profile.genus)}`} style={{ color: "inherit", textDecoration: "none" }}>
            <i>{profile.genus}</i> →
          </Link>
        </h3>
        <div className={classes.statsRow}>
          <div className={classes.statCard}>
            <span className={classes.statValue}>{profile.total_samples.toLocaleString("en")}</span>
            <span className={classes.statLabel}>{t("search.totalSamples")}</span>
          </div>
          <div className={classes.statCard}>
            <span className={classes.statValue}>{profile.present_samples.toLocaleString("en")}</span>
            <span className={classes.statLabel}>{t("search.presentIn")}</span>
          </div>
          <div className={classes.statCard}>
            <span className={classes.statValue}>{(profile.prevalence * 100).toFixed(1)}%</span>
            <span className={classes.statLabel}>{t("search.prevalence")}</span>
          </div>
          <div className={classes.statCard}>
            <span className={classes.statValue}>{formatAbundancePercent(profile.mean_abundance)}</span>
            <span className={classes.statLabel}>{t("search.meanAbundance")}</span>
          </div>
        </div>
      </div>

      {/* Export buttons */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem" }}>
        <button onClick={exportSearchCsv} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
        <button onClick={() => exportSearchChart(diseaseRef, "disease", "svg")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.svg")}</button>
        <button onClick={() => exportSearchChart(diseaseRef, "disease", "png")} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.png")}</button>
      </div>

      {/* Charts grid / 图表网格 */}
      <div className={classes.chartsGrid}>
        {profile.by_disease.length > 0 && (
          <div className={classes.chartBlock}>
            <h4>{t("search.byDisease")}</h4>
            <svg ref={diseaseRef} className={classes.profileChart} />
          </div>
        )}
        {profile.by_country.length > 0 && (
          <div className={classes.chartBlock}>
            <h4>{t("search.byCountry")}</h4>
            <svg ref={countryRef} className={classes.profileChart} />
          </div>
        )}
        {profile.by_age_group.length > 0 && (
          <div className={classes.chartBlock}>
            <h4>{t("search.byAgeGroup")}</h4>
            <svg ref={ageRef} className={classes.profileChart} />
          </div>
        )}

        {/* Sex distribution as simple table / 性别分布简表 */}
        {profile.by_sex.length > 0 && (
          <div className={classes.chartBlock}>
            <h4>{t("search.bySex")}</h4>
            <table className={classes.sexTable}>
              <thead>
                <tr>
                  <th>{t("filter.sex")}</th>
                  <th>{t("search.meanAbundance")}</th>
                  <th>{t("search.prevalence")}</th>
                  <th>n</th>
                </tr>
              </thead>
              <tbody>
                {profile.by_sex.map((s) => (
                  <tr key={s.name}>
                    <td>{s.name}</td>
                    <td>{formatAbundancePercent(s.mean_abundance)}</td>
                    <td>{(s.prevalence * 100).toFixed(1)}%</td>
                    <td>{s.sample_count.toLocaleString("en")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

// ── D3 horizontal bar chart / D3 水平柱状图 ─────────────────────────────────

const translateLabel = (name: string, locale: string, type: string): string => {
  if (type === "disease") return diseaseDisplayNameI18n(name, locale);
  if (type === "country") return countryName(name, locale);
  if (type === "age") return locale === "zh" ? (AGE_GROUP_ZH[name] ?? name.replace(/_/g, " ")) : name.replace(/_/g, " ");
  if (type === "sex") return locale === "zh" ? (SEX_ZH[name] ?? name) : name;
  return name;
};

function drawBarChart(svgEl: SVGSVGElement, data: ProfileEntry[], color: string, locale = "en", nameType = "") {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const isDisease = nameType === "disease";
  const leftM = isDisease ? 430 : nameType === "country" ? 260 : 220;
  const margin = { top: 16, right: 120, bottom: 42, left: leftM };
  const W = 1400;
  const H = Math.max(220, data.length * 30 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const maxValue = Math.max(d3.max(data, (d) => d.mean_abundance) ?? 0, 0.000001);
  const xScale = d3.scaleLinear()
    .domain([0, maxValue * 1.12])
    .range([0, iW]);
  const yScale = d3.scaleBand()
    .domain(data.map((d) => d.name))
    .range([0, iH])
    .padding(0.18);

  const rootStyles = getComputedStyle(document.documentElement);
  const resolvedColor = color.startsWith("var(")
    ? rootStyles.getPropertyValue(color.slice(4, -1)).trim() || "#e23fff"
    : color;

  g.selectAll(".bar")
    .data(data)
    .join("rect")
    .attr("class", "bar")
    .attr("x", 0)
    .attr("y", (d) => yScale(d.name) ?? 0)
    .attr("width", (d) => xScale(d.mean_abundance))
    .attr("height", yScale.bandwidth())
    .attr("fill", resolvedColor)
    .attr("opacity", 0.8)
    .attr("rx", 2)
    .attr("data-tooltip", (d) =>
      renderToString(
        <div className="tooltip-table">
          <span>{locale === "zh" ? "名称" : "Name"}</span><span>{translateLabel(d.name, locale, nameType)}</span>
          <span>{locale === "zh" ? "平均丰度" : "Abundance"}</span><span>{formatAbundancePercent(d.mean_abundance)}</span>
          <span>{locale === "zh" ? "检出率" : "Prevalence"}</span><span>{(d.prevalence * 100).toFixed(1)}%</span>
          <span>{locale === "zh" ? "样本数" : "Samples"}</span><span>{d.sample_count.toLocaleString("en")}</span>
        </div>
      )
    );

  g.selectAll(".val-label")
    .data(data)
    .join("text")
    .attr("x", (d) => xScale(d.mean_abundance) + 4)
    .attr("y", (d) => (yScale(d.name) ?? 0) + yScale.bandwidth() / 2 + 1)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 11)
    .attr("fill", "currentColor")
    .text((d) => formatAbundancePercent(d.mean_abundance));

  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat((d) => {
      const translated = translateLabel(d, locale, nameType);
      const limit = isDisease ? 68 : nameType === "country" ? 34 : 28;
      return translated.length > limit ? translated.slice(0, limit - 2) + "…" : translated;
    }))
    .attr("font-size", 12);

  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(5).tickFormat((d) => formatAbundanceTick(Number(d))))
    .attr("font-size", 11);
}
