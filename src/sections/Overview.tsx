import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import BarsIcon from "@/assets/bars.svg?react";
import DatabaseIcon from "@/assets/database.svg?react";
import EarthIcon from "@/assets/earth.svg?react";
import MicroscopeIcon from "@/assets/microscope.svg?react";
import PackageIcon from "@/assets/package.svg?react";
import Placeholder from "@/components/Placeholder";
import { useI18n } from "@/i18n";
import { type ApiStats, useData } from "@/data";
import { formatNumber } from "@/util/string";
import classes from "./Overview.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const Overview = () => {
  const { t } = useI18n();
  const summary = useData((state) => state.summary);
  const abundance = useData((state) => state.abundance);

  // API stats (if backend running) / 后端统计数据（后端启动后可用）
  const [apiStats, setApiStats] = useState<ApiStats | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/api/data-stats`)
      .then((r) => r.json())
      .then(setApiStats)
      .catch(() => { /* backend not running, ignore */ });
  }, []);

  const countries = apiStats?.total_countries ?? (summary
    ? Object.keys(summary.country_counts).filter((k) => k !== "unknown").length
    : undefined);

  const diseases = apiStats?.total_diseases
    ?? (summary as any)?.total_unique_diseases
    ?? (summary
      ? Object.keys(summary.disease_counts).filter((k) => k !== "unknown" && k !== "NC").length
      : undefined);

  const FEATURE_CARDS = [
    {
      to: "/phenotype",
      title: t("feature.phenotype.title"),
      desc: t("feature.phenotype.desc"),
      color: "var(--secondary)",
    },
    {
      to: "/disease",
      title: t("feature.disease.title"),
      desc: t("feature.disease.desc"),
      color: "#ff6b6b",
    },
    {
      to: "/compare",
      title: t("feature.compare.title"),
      desc: t("feature.compare.desc"),
      color: "var(--primary)",
    },
    {
      to: "/network",
      title: t("feature.network.title"),
      desc: t("feature.network.desc"),
      color: "#4ecdc4",
    },
    {
      to: "/lifecycle",
      title: t("feature.lifecycle.title"),
      desc: t("feature.lifecycle.desc"),
      color: "#a78bfa",
    },
    {
      to: "/similarity",
      title: t("feature.similarity.title"),
      desc: t("feature.similarity.desc"),
      color: "#f59e0b",
    },
    {
      to: "/studies",
      title: t("feature.studies.title"),
      desc: t("feature.studies.desc"),
      color: "#10b981",
    },
    {
      to: "/search",
      title: t("feature.search.title"),
      desc: t("feature.search.desc"),
      color: "#60a5fa",
    },
    {
      to: "/metabolism",
      title: t("feature.metabolism.title"),
      desc: t("feature.metabolism.desc"),
      color: "var(--secondary-light)",
    },
    {
      to: "/download",
      title: t("feature.download.title"),
      desc: t("feature.download.desc"),
      color: "#f97316",
    },
    {
      to: "/api-docs",
      title: t("feature.apiDocs.title"),
      desc: t("feature.apiDocs.desc"),
      color: "#22c55e",
    },
    {
      to: "/about",
      title: t("feature.about.title"),
      desc: t("feature.about.desc"),
      color: "#c084fc",
    },
  ];

  const formatTileValue = (value?: number) =>
    value === undefined ? "…" : formatNumber(value, false);

  const tiles = [
    {
      icon: MicroscopeIcon,
      value: summary?.total_samples ?? apiStats?.total_samples,
      label: t("overview.samples"),
    },
    {
      icon: EarthIcon,
      value: countries,
      label: t("overview.countries"),
    },
    {
      icon: BarsIcon,
      value: diseases,
      label: t("overview.diseaseTypes"),
    },
    {
      icon: PackageIcon,
      value: apiStats?.total_projects ?? summary?.total_projects,
      label: t("overview.projects"),
    },
    {
      icon: DatabaseIcon,
      value: apiStats?.total_genera ?? summary?.total_genera ?? abundance?.total_genera,
      label: t("overview.genera"),
    },
  ];

  return (
    <section>
      <h2>{t("overview.title")}</h2>

      <p>
        {summary
          ? t("overview.intro")
          : t("overview.intro")}
      </p>

      <p className={classes.methodology}>
        <b>{t("overview.methods")}</b>{" "}
        {t("overview.methods.detail")}
      </p>

      {summary ? (
        <div className={classes.tiles}>
          {tiles.map(({ icon, value, label }, index) => {
            const percent = (index / (tiles.length - 1)) * 100;
            const color = `color-mix(in hsl, var(--primary-light), ${percent}% var(--secondary-light))`;
            return (
              <div key={index} className={classes.tile}>
                {icon({ style: { color } })}
                <span className={classes.tileValue}>{formatTileValue(value)}</span>
                <span className={classes.tileLabel}>{label}</span>
              </div>
            );
          })}
        </div>
      ) : (
        <Placeholder height={150}>Loading overview...</Placeholder>
      )}

      {/* Data version badge / 数据版本徽章 */}
      {apiStats?.last_updated && (
        <p className={classes.versionBadge}>
          {t("overview.dataVersion")} <b>{apiStats.version}</b> · {t("overview.lastUpdated")} {apiStats.last_updated}
        </p>
      )}

      <p style={{ textAlign: "center", color: "#888", fontSize: "0.85rem", marginTop: "1rem" }}>
        Data from NCBI SRA, ENA &amp; DDBJ public repositories.
        <Link to="/about" style={{ color: "var(--accent)", marginLeft: "0.5rem" }}>
          Learn more &amp; cite
        </Link>
      </p>

      {/* Quick-access cards / 快速入口卡片 */}
      <div className={classes.featureGrid}>
        {FEATURE_CARDS.map(({ to, title, desc, color }) => (
          <Link key={to} to={to} className={classes.featureCard}>
            <div className={classes.featureAccent} style={{ background: color }} />
            <h3 className={classes.featureTitle} style={{ color }}>{title}</h3>
            <p className={classes.featureDesc}>{desc}</p>
            <span className={classes.featureArrow} style={{ color }}>→</span>
          </Link>
        ))}
      </div>

      <hr />
    </section>
  );
};

export default Overview;
