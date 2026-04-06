import { useEffect, useMemo, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { exportSVG, exportPNG } from "@/util/chartExport";
import { exportTable } from "@/util/export";

import type { ProfileEntry, SpeciesProfile } from "./types";
import { formatPValue, formatPrevalence, formatPercent, starsForPValue, translateDimensionName, type DimensionType } from "./utils";

interface ProfilePanelProps {
  profile: SpeciesProfile;
}

type MetricMode = "abundance" | "prevalence";

export default function ProfilePanel({ profile }: ProfilePanelProps) {
  const { locale, t } = useI18n();
  const diseaseRef = useRef<SVGSVGElement>(null);
  const countryRef = useRef<SVGSVGElement>(null);
  const ageRef = useRef<SVGSVGElement>(null);
  const sexRef = useRef<SVGSVGElement>(null);
  const [metric, setMetric] = useState<MetricMode>("abundance");
  const [significantOnly, setSignificantOnly] = useState(false);

  const diseaseRows = useMemo(() => {
    const filtered = significantOnly
      ? profile.by_disease.filter((item) => item.significant)
      : profile.by_disease;
    return filtered.slice(0, 30);
  }, [profile.by_disease, significantOnly]);

  useEffect(() => {
    if (!diseaseRef.current || diseaseRows.length === 0) return;
    drawDiseaseChart(diseaseRef.current, diseaseRows, metric, locale, profile);
  }, [diseaseRows, locale, metric, profile]);

  useEffect(() => {
    if (!countryRef.current || profile.by_country.length === 0) return;
    drawDistributionChart(countryRef.current, profile.by_country.slice(0, 20), metric, locale, "country", "#06b6d4");
  }, [locale, metric, profile.by_country]);

  useEffect(() => {
    if (!ageRef.current || profile.by_age_group.length === 0) return;
    drawDistributionChart(ageRef.current, profile.by_age_group, metric, locale, "age", "#14b8a6");
  }, [locale, metric, profile.by_age_group]);

  useEffect(() => {
    if (!sexRef.current || profile.by_sex.length === 0) return;
    drawDistributionChart(sexRef.current, profile.by_sex, metric, locale, "sex", "#8b5cf6");
  }, [locale, metric, profile.by_sex]);

  const exportDiseaseRows = diseaseRows.map((item) => ({
    disease: item.name,
    mean_abundance: item.mean_abundance,
    median_abundance: item.median_abundance,
    prevalence: item.prevalence,
    sample_count: item.sample_count,
    log2fc: item.log2fc,
    adjusted_p: item.adjusted_p,
    significant: item.significant,
  }));

  return (
    <section className="species-block">
      <div className="species-blockHeader">
        <div>
          <h2>{t("species.tab.profile")}</h2>
          <p>{t("species.profileSubtitle")}</p>
        </div>
        <div className="species-toggleRow">
          <button
            type="button"
            className={metric === "abundance" ? "is-active" : ""}
            onClick={() => setMetric("abundance")}
          >
            {t("species.abundanceBtn")}
          </button>
          <button
            type="button"
            className={metric === "prevalence" ? "is-active" : ""}
            onClick={() => setMetric("prevalence")}
          >
            {t("species.prevalenceBtn")}
          </button>
          <button
            type="button"
            className={significantOnly ? "is-active" : ""}
            onClick={() => setSignificantOnly((value) => !value)}
          >
            {significantOnly ? t("species.showAll") : t("species.sigOnly")}
          </button>
        </div>
      </div>

      <div className="species-summaryPills">
        <span>{`${t("species.phylum")}: ${profile.phylum}`}</span>
        <span>{`${t("search.medianAbundance")}: ${formatPercent(profile.median_abundance, 4)}`}</span>
        <span>{`${t("search.ncMeanAbundance")}: ${formatPercent(profile.nc_mean, 4)}`}</span>
        <span>{`${t("search.ncPrevalence")}: ${formatPrevalence(profile.nc_prevalence)}`}</span>
      </div>

      <div className="species-grid species-gridProfile">
        <ChartCard
          title={t("search.byDisease")}
          subtitle={metric === "abundance" ? t("species.profileDiseaseAbundanceHint") : t("species.profileDiseasePrevalenceHint")}
          svgRef={diseaseRef}
          onExportCsv={() => exportTable(exportDiseaseRows, `${profile.genus}_disease_profile`)}
          onExportSvg={() => diseaseRef.current && exportSVG(diseaseRef.current, `${profile.genus}_disease_profile`)}
          onExportPng={() => diseaseRef.current && exportPNG(diseaseRef.current, `${profile.genus}_disease_profile`)}
        />
        <ChartCard
          title={t("search.byCountry")}
          subtitle={t("species.profileGenericHint")}
          svgRef={countryRef}
          onExportCsv={() => exportTable(profile.by_country.map((item) => ({ ...item })), `${profile.genus}_country_profile`)}
          onExportSvg={() => countryRef.current && exportSVG(countryRef.current, `${profile.genus}_country_profile`)}
          onExportPng={() => countryRef.current && exportPNG(countryRef.current, `${profile.genus}_country_profile`)}
        />
        <ChartCard
          title={t("search.byAgeGroup")}
          subtitle={t("species.profileAgeHint")}
          svgRef={ageRef}
          onExportCsv={() => exportTable(profile.by_age_group.map((item) => ({ ...item })), `${profile.genus}_age_profile`)}
          onExportSvg={() => ageRef.current && exportSVG(ageRef.current, `${profile.genus}_age_profile`)}
          onExportPng={() => ageRef.current && exportPNG(ageRef.current, `${profile.genus}_age_profile`)}
        />
        <ChartCard
          title={t("search.bySex")}
          subtitle={t("species.profileGenericHint")}
          svgRef={sexRef}
          onExportCsv={() => exportTable(profile.by_sex.map((item) => ({ ...item })), `${profile.genus}_sex_profile`)}
          onExportSvg={() => sexRef.current && exportSVG(sexRef.current, `${profile.genus}_sex_profile`)}
          onExportPng={() => sexRef.current && exportPNG(sexRef.current, `${profile.genus}_sex_profile`)}
        />
      </div>
    </section>
  );
}

function ChartCard({
  title,
  subtitle,
  svgRef,
  onExportCsv,
  onExportSvg,
  onExportPng,
}: {
  title: string;
  subtitle: string;
  svgRef: React.RefObject<SVGSVGElement | null>;
  onExportCsv: () => void;
  onExportSvg: () => void;
  onExportPng: () => void;
}) {
  return (
    <article className="species-chartCard">
      <div className="species-chartHeader">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
        <div className="species-exportRow">
          <button type="button" onClick={onExportCsv}>CSV</button>
          <button type="button" onClick={onExportSvg}>SVG</button>
          <button type="button" onClick={onExportPng}>PNG</button>
        </div>
      </div>
      <svg ref={svgRef} className="species-chart" />
    </article>
  );
}

function drawDiseaseChart(
  svgEl: SVGSVGElement,
  data: ProfileEntry[],
  metric: MetricMode,
  locale: string,
  profile: SpeciesProfile,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const rows = [...data];
  const width = 980;
  const height = Math.max(260, rows.length * 28 + 110);
  const margin = { top: 22, right: 130, bottom: 42, left: 250 };
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const accessor = metric === "abundance"
    ? (entry: ProfileEntry) => entry.mean_abundance
    : (entry: ProfileEntry) => entry.prevalence * 100;
  const baseline = metric === "abundance" ? profile.nc_mean : profile.nc_prevalence * 100;
  const maxValue = Math.max(baseline, d3.max(rows, accessor) ?? 0.01);
  const x = d3.scaleLinear().domain([0, maxValue * 1.1 || 1]).range([0, plotWidth]);
  const y = d3.scaleBand().domain(rows.map((row) => row.name)).range([0, plotHeight]).padding(0.18);

  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  root.append("line")
    .attr("x1", x(baseline))
    .attr("x2", x(baseline))
    .attr("y1", 0)
    .attr("y2", plotHeight)
    .attr("stroke", "rgba(255,255,255,0.35)")
    .attr("stroke-width", 1.2)
    .attr("stroke-dasharray", "4,4");

  root.append("text")
    .attr("x", x(baseline))
    .attr("y", -6)
    .attr("text-anchor", "middle")
    .attr("font-size", 10)
    .attr("fill", "currentColor")
    .attr("opacity", 0.72)
    .text(metric === "abundance" ? "NC mean" : "NC prevalence");

  root.selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", 0)
    .attr("y", (row) => y(row.name) ?? 0)
    .attr("width", (row) => x(accessor(row)))
    .attr("height", y.bandwidth())
    .attr("rx", 4)
    .attr("fill", (row) => ((row.log2fc ?? 0) >= 0 ? "#ef4444" : "#3b82f6"))
    .attr("opacity", (row) => (row.significant ? 0.86 : 0.38))
    .attr("data-tooltip", (row) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "疾病" : "Disease"}</span><span>{translateDimensionName(row.name, locale, "disease")}</span>
        <span>{locale === "zh" ? "均值" : "Mean"}</span><span>{formatPercent(row.mean_abundance, 4)}</span>
        <span>{locale === "zh" ? "中位数" : "Median"}</span><span>{formatPercent(row.median_abundance ?? row.mean_abundance, 4)}</span>
        <span>{locale === "zh" ? "流行率" : "Prevalence"}</span><span>{formatPrevalence(row.prevalence)}</span>
        <span>log2FC</span><span>{(row.log2fc ?? 0).toFixed(3)}</span>
        <span>FDR</span><span>{formatPValue(row.adjusted_p)}</span>
        <span>{locale === "zh" ? "样本数" : "Samples"}</span><span>{row.sample_count.toLocaleString("en")}</span>
      </div>,
    ));

  root.selectAll(".species-valueLabel")
    .data(rows)
    .join("text")
    .attr("class", "species-valueLabel")
    .attr("x", (row) => x(accessor(row)) + 6)
    .attr("y", (row) => (y(row.name) ?? 0) + y.bandwidth() / 2 + 1)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 10)
    .attr("fill", "currentColor")
    .text((row) => {
      const value = accessor(row);
      return metric === "abundance" ? `${value.toFixed(3)}%` : `${value.toFixed(1)}%`;
    });

  root.selectAll(".species-sigLabel")
    .data(rows.filter((row) => row.significant))
    .join("text")
    .attr("class", "species-sigLabel")
    .attr("x", (row) => x(accessor(row)) + 68)
    .attr("y", (row) => (y(row.name) ?? 0) + y.bandwidth() / 2 + 1)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 10)
    .attr("fill", "#facc15")
    .text((row) => starsForPValue(row.adjusted_p));

  root.append("g")
    .call(
      d3.axisLeft(y).tickFormat((value) => {
        const label = translateDimensionName(String(value), locale, "disease");
        return label.length > 34 ? `${label.slice(0, 33)}…` : label;
      }),
    )
    .attr("font-size", 11);

  root.append("g")
    .attr("transform", `translate(0,${plotHeight})`)
    .call(d3.axisBottom(x).ticks(5).tickFormat((value) => `${Number(value).toFixed(metric === "abundance" ? 2 : 1)}%`))
    .attr("font-size", 10);
}

function drawDistributionChart(
  svgEl: SVGSVGElement,
  data: ProfileEntry[],
  metric: MetricMode,
  locale: string,
  type: DimensionType,
  color: string,
) {
  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const width = 920;
  const height = Math.max(240, data.length * 28 + 90);
  const margin = { top: 18, right: 90, bottom: 42, left: type === "country" ? 190 : 170 };
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const accessor = metric === "abundance"
    ? (entry: ProfileEntry) => entry.mean_abundance
    : (entry: ProfileEntry) => entry.prevalence * 100;
  const x = d3.scaleLinear().domain([0, (d3.max(data, accessor) ?? 0.01) * 1.1 || 1]).range([0, plotWidth]);
  const y = d3.scaleBand().domain(data.map((row) => row.name)).range([0, plotHeight]).padding(0.2);

  const root = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
  root.selectAll("rect")
    .data(data)
    .join("rect")
    .attr("x", 0)
    .attr("y", (row) => y(row.name) ?? 0)
    .attr("width", (row) => x(accessor(row)))
    .attr("height", y.bandwidth())
    .attr("rx", 4)
    .attr("fill", color)
    .attr("opacity", 0.84)
    .attr("data-tooltip", (row) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "名称" : "Name"}</span><span>{translateDimensionName(row.name, locale, type)}</span>
        <span>{locale === "zh" ? "均值" : "Mean"}</span><span>{formatPercent(row.mean_abundance, 4)}</span>
        <span>{locale === "zh" ? "中位数" : "Median"}</span><span>{formatPercent(row.median_abundance ?? row.mean_abundance, 4)}</span>
        <span>{locale === "zh" ? "流行率" : "Prevalence"}</span><span>{formatPrevalence(row.prevalence)}</span>
        <span>{locale === "zh" ? "样本数" : "Samples"}</span><span>{row.sample_count.toLocaleString("en")}</span>
      </div>,
    ));

  root.selectAll(".species-distributionLabel")
    .data(data)
    .join("text")
    .attr("class", "species-distributionLabel")
    .attr("x", (row) => x(accessor(row)) + 6)
    .attr("y", (row) => (y(row.name) ?? 0) + y.bandwidth() / 2 + 1)
    .attr("dominant-baseline", "middle")
    .attr("font-size", 10)
    .attr("fill", "currentColor")
    .text((row) => {
      const value = accessor(row);
      return metric === "abundance" ? `${value.toFixed(3)}%` : `${value.toFixed(1)}%`;
    });

  root.append("g")
    .call(
      d3.axisLeft(y).tickFormat((value) => {
        const label = translateDimensionName(String(value), locale, type);
        const limit = type === "country" ? 22 : 18;
        return label.length > limit ? `${label.slice(0, limit - 1)}…` : label;
      }),
    )
    .attr("font-size", 11);

  root.append("g")
    .attr("transform", `translate(0,${plotHeight})`)
    .call(d3.axisBottom(x).ticks(4).tickFormat((value) => `${Number(value).toFixed(metric === "abundance" ? 2 : 1)}%`))
    .attr("font-size", 10);
}
