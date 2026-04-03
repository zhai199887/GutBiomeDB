/**
 * Search.tsx — Species Search Engine
 * 物种搜索引擎：输入属名 → 展示该属在疾病/国家/年龄组中的分布
 * Inspired by ResMicroDb species search + GMrepo disease-species profiles
 */
import { useEffect, useRef, useState, useCallback } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import { exportSVG, exportPNG } from "@/util/chartExport";
import "@/components/tooltip";
import classes from "./Search.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

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

const Search = () => {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
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

    fetch(`${API_BASE}/api/species-profile?genus=${encodeURIComponent(genus.trim())}`)
      .then((r) => {
        if (!r.ok) throw new Error(r.status === 404 ? "Genus not found" : "Server error");
        return r.json();
      })
      .then((data: SpeciesProfile) => setProfile(data))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Close suggestions on outside click / 点击外部关闭建议
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(e.target as Node) &&
          inputRef.current && !inputRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
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
            onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
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
  const { t } = useI18n();
  const diseaseRef = useRef<SVGSVGElement>(null);
  const countryRef = useRef<SVGSVGElement>(null);
  const ageRef = useRef<SVGSVGElement>(null);

  // Draw disease bar chart / 绘制疾病丰度柱状图
  useEffect(() => {
    if (!diseaseRef.current || profile.by_disease.length === 0) return;
    drawBarChart(diseaseRef.current, profile.by_disease.slice(0, 20), "var(--primary)");
  }, [profile]);

  // Draw country bar chart / 绘制国家丰度柱状图
  useEffect(() => {
    if (!countryRef.current || profile.by_country.length === 0) return;
    drawBarChart(countryRef.current, profile.by_country.slice(0, 20), "var(--secondary)");
  }, [profile]);

  // Draw age group bar chart / 绘制年龄组丰度柱状图
  useEffect(() => {
    if (!ageRef.current || profile.by_age_group.length === 0) return;
    drawBarChart(ageRef.current, profile.by_age_group, "#4ecdc4");
  }, [profile]);

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
            <span className={classes.statValue}>{(profile.mean_abundance * 100).toFixed(4)}%</span>
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
                    <td>{(s.mean_abundance * 100).toFixed(4)}%</td>
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

function drawBarChart(svgEl: SVGSVGElement, data: ProfileEntry[], color: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const margin = { top: 10, right: 60, bottom: 30, left: 130 };
  const W = 520, H = Math.max(180, data.length * 22 + margin.top + margin.bottom);
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const xScale = d3.scaleLinear()
    .domain([0, d3.max(data, (d) => d.mean_abundance) ?? 0.001])
    .range([0, iW]);
  const yScale = d3.scaleBand()
    .domain(data.map((d) => d.name))
    .range([0, iH])
    .padding(0.2);

  // Resolve CSS variable to hex / 将 CSS 变量解析为十六进制
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
          <span>Name</span><span>{d.name}</span>
          <span>Abundance</span><span>{(d.mean_abundance * 100).toFixed(4)}%</span>
          <span>Prevalence</span><span>{(d.prevalence * 100).toFixed(1)}%</span>
          <span>Samples</span><span>{d.sample_count.toLocaleString("en")}</span>
        </div>
      )
    );

  // Value labels / 数值标签
  g.selectAll(".val-label")
    .data(data)
    .join("text")
    .attr("x", (d) => xScale(d.mean_abundance) + 4)
    .attr("y", (d) => (yScale(d.name) ?? 0) + yScale.bandwidth() / 2 + 1)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 9)
    .attr("fill", "currentColor")
    .text((d) => `${(d.mean_abundance * 100).toFixed(3)}%`);

  // Y axis / Y轴
  g.append("g")
    .call(d3.axisLeft(yScale).tickFormat((d) => d.length > 16 ? d.slice(0, 14) + "…" : d))
    .attr("font-size", 10);

  // X axis / X轴
  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).ticks(4).tickFormat((d) => `${(Number(d) * 100).toFixed(2)}%`))
    .attr("font-size", 9);
}
