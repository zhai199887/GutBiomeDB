import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { phylumColor } from "@/util/phylumColors";

import BiomarkerPanel from "./species/BiomarkerPanel";
import CooccurrencePanel from "./species/CooccurrencePanel";
import InlineGenusSearch from "./species/InlineGenusSearch";
import MetabolismPanel from "./species/MetabolismPanel";
import ProfilePanel from "./species/ProfilePanel";
import type { SpeciesProfile } from "./species/types";
import classes from "./SpeciesPage.module.css";

type TabKey = "profile" | "biomarker" | "cooccurrence";

export default function SpeciesPage() {
  const { locale, t } = useI18n();
  const navigate = useNavigate();
  const { taxon } = useParams<{ taxon: string }>();
  const genus = decodeURIComponent(taxon ?? "");

  const [profile, setProfile] = useState<SpeciesProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState<TabKey>("profile");

  useEffect(() => {
    if (!genus.trim()) return;
    setLoading(true);
    setError("");
    cachedFetch<SpeciesProfile>(`${API_BASE}/api/species-profile?genus=${encodeURIComponent(genus)}`)
      .then((payload) => {
        setProfile(payload);
        setActiveTab("profile");
      })
      .catch((unknownError) => {
        setProfile(null);
        setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
      })
      .finally(() => setLoading(false));
  }, [genus]);

  useEffect(() => {
    if (!genus) return;
    document.title = `${genus} · ${locale === "zh" ? "菌属画像" : "Genus Profile"} · GutBiomeDB`;
  }, [genus, locale]);

  const statCards = useMemo(() => {
    if (!profile) return [];
    return [
      { label: t("search.totalSamples"), value: profile.total_samples.toLocaleString("en") },
      { label: t("search.presentIn"), value: profile.present_samples.toLocaleString("en") },
      { label: t("search.prevalence"), value: `${(profile.prevalence * 100).toFixed(1)}%` },
      { label: t("search.meanAbundance"), value: `${profile.mean_abundance.toFixed(4)}%` },
      { label: t("search.medianAbundance"), value: `${profile.median_abundance.toFixed(4)}%` },
      { label: t("search.ncMeanAbundance"), value: `${profile.nc_mean.toFixed(4)}%` },
      { label: t("search.ncPrevalence"), value: `${(profile.nc_prevalence * 100).toFixed(1)}%` },
    ];
  }, [profile, t]);

  const tabs: Array<{ key: TabKey; label: string }> = [
    { key: "profile", label: t("species.tab.profile") },
    { key: "biomarker", label: t("species.tab.biomarker") },
    { key: "cooccurrence", label: t("species.tab.cooccurrence") },
  ];

  return (
    <div className={classes.page}>
      <div className={classes.header}>
        <button
          type="button"
          className={classes.back}
          onClick={() => {
            if (window.history.length > 1) navigate(-1);
            else navigate("/search");
          }}
        >
          {t("phenotype.back")}
        </button>

        <div className={classes.titleBlock}>
          <div className={classes.titleRow}>
            <h1><i>{genus}</i></h1>
            {profile?.phylum ? (
              <span
                className={classes.phylumBadge}
                style={{
                  borderColor: phylumColor(profile.phylum),
                  color: phylumColor(profile.phylum),
                  background: `${phylumColor(profile.phylum)}22`,
                }}
              >
                {profile.phylum}
              </span>
            ) : null}
          </div>
          <p className={classes.subtitle}>{t("species.profileSubtitle")}</p>
        </div>

        <div className={classes.inlineSearch}>
          <InlineGenusSearch genus={genus} onSelect={(next) => navigate(`/species/${encodeURIComponent(next)}`)} />
        </div>
      </div>

      {loading ? <div className={classes.loading}>{t("search.searching")}</div> : null}
      {!loading && error ? <div className={classes.error}>{error}</div> : null}

      {!loading && profile ? (
        <div className={classes.content}>
          <div className={classes.statsGrid}>
            {statCards.map((card) => (
              <article key={card.label} className={classes.statCard}>
                <div className={classes.statValue}>{card.value}</div>
                <div className={classes.statLabel}>{card.label}</div>
              </article>
            ))}
          </div>

          <div className={classes.tabs}>
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={activeTab === tab.key ? classes.tabActive : classes.tab}
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
              </button>
            ))}
            <Link to="/search" className={classes.secondaryLink}>
              {t("nav.search")}
            </Link>
          </div>

          {activeTab === "profile" ? (
            <>
              <ProfilePanel profile={profile} />
              <MetabolismPanel genus={profile.genus} />
            </>
          ) : null}
          {activeTab === "biomarker" ? <BiomarkerPanel genus={profile.genus} /> : null}
          {activeTab === "cooccurrence" ? <CooccurrencePanel genus={profile.genus} phylum={profile.phylum} /> : null}
        </div>
      ) : null}
    </div>
  );
}