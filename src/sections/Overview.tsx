import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import BarsIcon from "@/assets/bars.svg?react";
import EarthIcon from "@/assets/earth.svg?react";
import MicroscopeIcon from "@/assets/microscope.svg?react";
import Placeholder from "@/components/Placeholder";
import { useData } from "@/data";
import { formatNumber } from "@/util/string";
import classes from "./Overview.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

// Quick-access feature cards / 快速入口功能卡片
const FEATURE_CARDS = [
  {
    to: "/phenotype",
    title: "Phenotype Explorer",
    desc: "Compare microbiome composition across age groups, sexes, and diseases",
    color: "var(--secondary)",
  },
  {
    to: "/compare",
    title: "Differential Analysis",
    desc: "Statistical comparison between two user-defined sample groups (differential abundance, volcano plot, PCoA)",
    color: "var(--primary)",
  },
  {
    to: "/metabolism",
    title: "Metabolic Functions",
    desc: "Browse microbiota by metabolic role: SCFAs, bile acids, tryptophan, TMAO, and more",
    color: "var(--secondary-light)",
  },
] as const;

const Overview = () => {
  const summary = useData((state) => state.summary);

  // API stats (if backend running) / 后端统计数据（后端启动后可用）
  const [apiStats, setApiStats] = useState<{
    last_updated?: string;
    version?: string;
    total_species?: number;
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

  const diseases = summary
    ? Object.keys(summary.disease_counts).filter((k) => k !== "unknown").length
    : undefined;

  const tiles = [
    {
      icon: MicroscopeIcon,
      text: (
        <>
          {formatNumber(summary?.total_samples, false)} samples
        </>
      ),
    },
    {
      icon: EarthIcon,
      text: (
        <>
          {formatNumber(countries)} countries
          <br />
          {formatNumber(regions)} regions
        </>
      ),
    },
    {
      icon: BarsIcon,
      text: (
        <>
          {formatNumber(diseases)} disease types
          <br />
          (curatedMetagenomicData)
        </>
      ),
    },
  ];

  return (
    <section>
      <h2>Overview</h2>

      <p>
        This platform lets you explore{" "}
        {summary
          ? "over " + formatNumber(
              Math.floor((summary.total_samples || 0) / 10000) * 10000,
            )
          : "thousands of"}{" "}
        publicly available human gut microbiome samples, annotated with age,
        sex, and disease metadata.
      </p>

      <p className={classes.methodology}>
        <b>Data &amp; Methods:</b>{" "}
        Samples sourced from the{" "}
        <a href="https://bioconductor.org/packages/curatedMetagenomicData" target="_blank" rel="noreferrer">
          curatedMetagenomicData
        </a>{" "}
        compendium (Pasolli <i>et al.</i>, 2017). Taxonomic profiles generated with MetaPhlAn.
        Differential abundance uses Wilcoxon rank-sum test with Benjamini–Hochberg FDR correction (adj. p &lt; 0.05).
        Beta diversity computed as Bray–Curtis dissimilarity, visualised by PCoA.
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
          Data version: <b>{apiStats.version}</b> · Last updated: {apiStats.last_updated}
        </p>
      )}

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
