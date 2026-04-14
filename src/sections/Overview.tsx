import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import BarsIcon from "@/assets/bars.svg?react";
import DatabaseIcon from "@/assets/database.svg?react";
import EarthIcon from "@/assets/earth.svg?react";
import MicroscopeIcon from "@/assets/microscope.svg?react";
import PackageIcon from "@/assets/package.svg?react";
import Placeholder from "@/components/Placeholder";
import { type ApiStats, useData } from "@/data";
import { useI18n } from "@/i18n";
import { API_BASE } from "@/util/apiBase";
import { formatNumber } from "@/util/string";

import classes from "./Overview.module.css";

const Overview = () => {
  const { t } = useI18n();
  const summary = useData((state) => state.summary);
  const abundance = useData((state) => state.abundance);
  const [apiStats, setApiStats] = useState<ApiStats | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/api/data-stats`)
      .then((response) => response.json())
      .then(setApiStats)
      .catch(() => {});
  }, []);

  const countries = apiStats?.total_countries ?? summary?.total_unique_countries ?? (summary
    ? Object.keys(summary.country_counts).filter((key) => key !== "unknown").length
    : undefined);

  const diseases = apiStats?.total_condition_categories
    ?? summary?.total_condition_categories
    ?? apiStats?.total_diseases
    ?? summary?.total_unique_diseases
    ?? (summary
      ? Object.keys(summary.disease_counts).filter((key) => key !== "unknown" && key !== "NC").length
      : undefined);

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

  const featureGroups = useMemo(
    () => [
      {
        title: t("overview.group.discovery.title"),
        cards: [
          {
            to: "/phenotype",
            title: t("feature.phenotype.title"),
            desc: t("feature.phenotype.desc"),
            color: "#5f8cff",
          },
          {
            to: "/disease",
            title: t("feature.disease.title"),
            desc: t("feature.disease.desc"),
            color: "#ff6b6b",
          },
          {
            to: "/search",
            title: t("feature.search.title"),
            desc: t("feature.search.desc"),
            color: "#7ee8ff",
          },
          {
            to: "/studies",
            title: t("feature.studies.title"),
            desc: t("feature.studies.desc"),
            color: "#33d17a",
          },
          {
            to: "/metabolism",
            title: t("feature.metabolism.title"),
            desc: t("feature.metabolism.desc"),
            color: "#8d8cff",
          },
        ],
      },
      {
        title: t("overview.group.analysis.title"),
        cards: [
          {
            to: "/compare",
            title: t("feature.compare.title"),
            desc: t("feature.compare.desc"),
            color: "#db53ff",
          },
          {
            to: "/network",
            title: t("feature.network.title"),
            desc: t("feature.network.desc"),
            color: "#43d6c5",
          },
          {
            to: "/lifecycle",
            title: t("feature.lifecycle.title"),
            desc: t("feature.lifecycle.desc"),
            color: "#b28cff",
          },
          {
            to: "/similarity",
            title: t("feature.similarity.title"),
            desc: t("feature.similarity.desc"),
            color: "#f6a623",
          },
        ],
      },
      {
        title: t("overview.group.resources.title"),
        cards: [
          {
            to: "/download",
            title: t("feature.download.title"),
            desc: t("feature.download.desc"),
            color: "#ff8f4a",
          },
          {
            to: "/api-docs",
            title: t("feature.apiDocs.title"),
            desc: t("feature.apiDocs.desc"),
            color: "#32c96b",
          },
          {
            to: "/about",
            title: t("feature.about.title"),
            desc: t("feature.about.desc"),
            color: "#c084fc",
          },
        ],
      },
    ],
    [t],
  );

  const formatTileValue = (value?: number) => (value === undefined ? "--" : formatNumber(value, false));

  return (
    <section className={classes.section}>
      <div className={classes.headingRow}>
        <div className={classes.headingCopy}>
          <h2>{t("overview.title")}</h2>
          <p>{t("overview.intro")}</p>
        </div>
      </div>

      {summary ? (
        <div className={classes.tiles}>
          {tiles.map(({ icon, value, label }, index) => {
            const percent = (index / (tiles.length - 1)) * 100;
            const color = `color-mix(in hsl, var(--primary-light), ${percent}% var(--secondary-light))`;
            return (
              <div key={label} className={classes.tile}>
                {icon({ style: { color } })}
                <span className={classes.tileValue}>{formatTileValue(value)}</span>
                <span className={classes.tileLabel}>{label}</span>
              </div>
            );
          })}
        </div>
      ) : (
        <Placeholder height={180}>{t("overview.loading")}</Placeholder>
      )}

      <div className={classes.metaRow}>
        <p className={classes.methodology}>
          <strong>{t("overview.methods")}</strong>{" "}
          {t("overview.methods.detail")}
        </p>
        {apiStats?.last_updated ? (
          <p className={classes.versionBadge}>
            {t("overview.dataVersion")} <strong>{apiStats.version}</strong> · {t("overview.lastUpdated")} {apiStats.last_updated}
          </p>
        ) : null}
      </div>

      <p className={classes.sourceNote}>
        {t("overview.sources")}
        <Link to="/about">{t("overview.cite")}</Link>
      </p>

      <div className={classes.featureSections}>
        {featureGroups.map((group) => (
          <section key={group.title} className={classes.featureSection}>
            <header className={classes.featureHeader}>
              <h3>{group.title}</h3>
            </header>

            <div className={classes.featureList}>
              {group.cards.map((card) => (
                <Link key={card.to} to={card.to} className={classes.featureCard}>
                  <span className={classes.featureAccent} style={{ background: card.color }} />
                  <div className={classes.featureBody}>
                    <h4 style={{ color: card.color }}>{card.title}</h4>
                    <p>{card.desc}</p>
                  </div>
                  <span className={classes.featureArrow} style={{ color: card.color }}>→</span>
                </Link>
              ))}
            </div>
          </section>
        ))}
      </div>

      <hr />
    </section>
  );
};

export default Overview;
