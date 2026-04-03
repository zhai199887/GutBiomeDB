import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import BarsIcon from "@/assets/bars.svg?react";
import EarthIcon from "@/assets/earth.svg?react";
import MicroscopeIcon from "@/assets/microscope.svg?react";
import Placeholder from "@/components/Placeholder";
import { useI18n } from "@/i18n";
import { useData } from "@/data";
import { formatNumber } from "@/util/string";
import classes from "./Overview.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const Overview = () => {
  const { t } = useI18n();
  const summary = useData((state) => state.summary);

  // API stats (if backend running) / 后端统计数据（后端启动后可用）
  const [apiStats, setApiStats] = useState<{
    last_updated?: string;
    version?: string;
    total_samples?: number;
    total_countries?: number;
    total_diseases?: number;
  } | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/api/data-stats`)
      .then((r) => r.json())
      .then(setApiStats)
      .catch(() => { /* backend not running, ignore */ });
  }, []);

  const countries = summary
    ? Object.keys(summary.country_counts).filter((k) => k !== "unknown").length
    : undefined;

  const regions = summary
    ? Object.keys(summary.region_counts).filter((k) => k !== "unknown").length
    : undefined;

  const diseases = apiStats?.total_diseases
    ?? (summary as any)?.total_unique_diseases
    ?? (summary
      ? Object.keys(summary.disease_counts).filter((k) => k !== "unknown" && k !== "NC").length
      : undefined);

  // Feature cards with i18n / 功能卡片国际化
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
      to: "/metabolism",
      title: t("feature.metabolism.title"),
      desc: t("feature.metabolism.desc"),
      color: "var(--secondary-light)",
    },
  ];

  const tiles = [
    {
      icon: MicroscopeIcon,
      text: (
        <>
          {formatNumber(summary?.total_samples, false)} {t("overview.samples")}
        </>
      ),
    },
    {
      icon: EarthIcon,
      text: (
        <>
          {formatNumber(countries)} {t("overview.countries")}
          <br />
          {formatNumber(regions)} {t("overview.regions")}
        </>
      ),
    },
    {
      icon: BarsIcon,
      text: (
        <>
          {formatNumber(diseases)} {t("overview.diseaseTypes")}
        </>
      ),
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
          {tiles.map(({ icon, text }, index) => {
            const percent = (index / (tiles.length - 1)) * 100;
            const color = `color-mix(in hsl, var(--primary-light), ${percent}% var(--secondary-light))`;
            return (
              <div key={index} className={classes.tile}>
                {icon({ style: { color } })}
                <span>{text}</span>
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
