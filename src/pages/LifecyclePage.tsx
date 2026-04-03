/**
 * LifecyclePage.tsx — Lifecycle Microbiome Atlas
 * 全生命周期微生物组图谱：D3 堆叠面积图
 */
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { countryName, AGE_GROUP_ZH } from "@/util/countries";
import classes from "./LifecyclePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const GENUS_COLORS = [
  "#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
  "#1abc9c", "#e67e22", "#34495e", "#16a085", "#c0392b",
  "#2980b9", "#27ae60", "#d35400", "#8e44ad", "#2c3e50",
  "#95a5a6",
];

interface LifecycleData {
  disease: string;
  country: string;
  total_samples: number;
  genera: string[];
  data: Record<string, any>[];
  transitions: { from: string; to: string; genus: string; change: number; direction: string }[];
}

interface DiseaseItem { name: string; sample_count: number; }

const LifecyclePage = () => {
  const { t, locale } = useI18n();
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});
  const [countries, setCountries] = useState<string[]>([]);
  const [disease, setDisease] = useState("");
  const [country, setCountry] = useState("");
  const [data, setData] = useState<LifecycleData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const svgRef = useRef<SVGSVGElement>(null);

  const dName = (n: string) => (locale === "zh" && diseaseZh[n]) ? diseaseZh[n] : n;
  const agName = (n: string) => locale === "zh" ? (AGE_GROUP_ZH[n] ?? n.replace(/_/g, " ")) : n.replace(/_/g, " ");

  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`).then(r => r.json()).then(d => setDiseases(d.diseases ?? [])).catch(() => {});
    fetch(`${API_BASE}/api/disease-names-zh`).then(r => r.json()).then(setDiseaseZh).catch(() => {});
    fetch(`${API_BASE}/api/filter-options`).then(r => r.json()).then(d => setCountries(d.countries ?? [])).catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams();
    if (disease) params.set("disease", disease);
    if (country) params.set("country", country);
    fetch(`${API_BASE}/api/lifecycle?${params}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((d: LifecycleData) => setData(d))
      .catch((e) => setError(locale === "zh" ? "后端未启动或连接失败" : `API error: ${e.message}`))
      .finally(() => setLoading(false));
  }, [disease, country]);

  useEffect(() => {
    if (!svgRef.current || !data || data.data.length === 0) return;
    drawStackedArea(svgRef.current, data, locale);
  }, [data, locale]);

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("lifecycle.back")}</Link>
        <h1>{t("lifecycle.title")}</h1>
        <p>{t("lifecycle.subtitle")}</p>
      </div>

      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("lifecycle.filterDisease")}</label>
          <select className={classes.select} value={disease} onChange={e => setDisease(e.target.value)}>
            <option value="">{t("lifecycle.allDiseases")}</option>
            {diseases.slice(0, 100).map(d => (
              <option key={d.name} value={d.name}>{dName(d.name)}</option>
            ))}
          </select>
        </div>
        <div className={classes.field}>
          <label>{t("lifecycle.filterCountry")}</label>
          <select className={classes.select} value={country} onChange={e => setCountry(e.target.value)}>
            <option value="">{t("lifecycle.allCountries")}</option>
            {countries.map(c => (
              <option key={c} value={c}>{countryName(c, locale)} ({c})</option>
            ))}
          </select>
        </div>
      </div>

      {loading && <div className={classes.loading}>{t("search.searching")}</div>}

      {error && <div className={classes.error}>{error}</div>}

      {data && data.data.length > 0 && (
        <>
          <div className={classes.chartCard}>
            <h3>{t("lifecycle.stackedArea")} ({data.total_samples.toLocaleString()} samples)</h3>
            <svg ref={svgRef} className={classes.chart} />
            <div className={classes.legend}>
              {data.genera.map((g, i) => (
                <div key={g} className={classes.legendItem}>
                  <span className={classes.legendDot} style={{ background: GENUS_COLORS[i % GENUS_COLORS.length] }} />
                  <span>{g === "Other" ? (locale === "zh" ? "其他" : "Other") : g}</span>
                </div>
              ))}
            </div>
          </div>

          {data.transitions.length > 0 && (
            <div className={classes.transitionCard}>
              <h3>{t("lifecycle.transition")}</h3>
              <ul className={classes.transitionList}>
                {data.transitions.map((tr, i) => (
                  <li key={i}>
                    <strong>{agName(tr.from)}</strong> → <strong>{agName(tr.to)}</strong>:&nbsp;
                    <i>{tr.genus}</i>&nbsp;
                    <span className={tr.direction === "increase" ? classes.increase : classes.decrease}>
                      {tr.direction === "increase" ? "↑" : "↓"} {tr.change.toFixed(2)}%
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default LifecyclePage;

function drawStackedArea(svgEl: SVGSVGElement, lifecycle: LifecycleData, locale: string) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const { genera, data } = lifecycle;

  const margin = { top: 20, right: 20, bottom: 60, left: 50 };
  const W = 800, H = 400;
  svg.attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  const iW = W - margin.left - margin.right;
  const iH = H - margin.top - margin.bottom;

  const ageGroups = data.map(d => d.age_group);

  const xScale = d3.scalePoint<string>()
    .domain(ageGroups)
    .range([0, iW])
    .padding(0.1);

  const yScale = d3.scaleLinear().domain([0, 100]).range([iH, 0]);

  const colorScale = d3.scaleOrdinal<string>()
    .domain(genera)
    .range(GENUS_COLORS);

  const AGE_GROUP_ZH_MAP: Record<string, string> = {
    Infant: "婴儿", Child: "儿童", Adolescent: "青少年", Adult: "成人",
    Older_Adult: "老年人", Centenarian: "百岁老人", Oldest_Old: "高龄老人", Unknown: "未知",
  };
  const agLabel = (ag: string) => locale === "zh" ? (AGE_GROUP_ZH_MAP[ag] ?? ag.replace(/_/g, " ")) : ag.replace(/_/g, " ");

  const stack = d3.stack<Record<string, any>>()
    .keys(genera)
    .order(d3.stackOrderNone)
    .offset(d3.stackOffsetNone);

  const series = stack(data);

  const area = d3.area<d3.SeriesPoint<Record<string, any>>>()
    .x(d => xScale(d.data.age_group) ?? 0)
    .y0(d => yScale(d[0]))
    .y1(d => yScale(d[1]))
    .curve(d3.curveMonotoneX);

  g.selectAll(".layer")
    .data(series)
    .join("path")
    .attr("d", area)
    .attr("fill", d => colorScale(d.key))
    .attr("opacity", 0.85);

  g.append("g")
    .attr("transform", `translate(0,${iH})`)
    .call(d3.axisBottom(xScale).tickFormat(d => agLabel(d)))
    .attr("font-size", 10)
    .selectAll("text")
    .attr("transform", "rotate(-25)")
    .style("text-anchor", "end");

  g.append("g")
    .call(d3.axisLeft(yScale).ticks(5).tickFormat(d => `${d}%`))
    .attr("font-size", 10);

  g.append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", -iH / 2).attr("y", -35)
    .attr("text-anchor", "middle").attr("fill", "currentColor").attr("font-size", 11)
    .text(locale === "zh" ? "相对丰度 (%)" : "Relative Abundance (%)");
}
