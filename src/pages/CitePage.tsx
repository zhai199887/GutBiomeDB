import { useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import Header from "@/sections/Header";
import Footer from "@/sections/Footer";
import classes from "./CitePage.module.css";

const BIBTEX = `@article{zhai2025gutmicrobiomeatlas,
  title   = {Gut Microbiome Atlas: a comprehensive platform for exploring
             human gut microbiome across diseases, geography, and lifespan},
  author  = {Zhai, Jinxia and Dai, Cong},
  journal = {Manuscript in preparation},
  year    = {2025}
}`;

const STATS = [
  { key: "samples", value: "168,464", label: "Total Samples" },
  { key: "genera", value: "4,680", label: "Genera" },
  { key: "countries", value: "69", label: "Countries" },
  { key: "diseases", value: "217+", label: "Diseases" },
  { key: "stages", value: "8", label: "Life Stages" },
];

const CitePage = () => {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(BIBTEX);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* fallback: ignore */
    }
  };

  return (
    <>
      <Header />
      <div className={classes.container}>
        <Link to="/" className={classes.backLink}>
          &larr; Back to Atlas
        </Link>

        <h1 className={classes.pageTitle}>{t("cite.title")}</h1>

        {/* Data Sources */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.dataSources")}</h2>
          <p className={classes.cardText}>{t("cite.dataSourcesText")}</p>

          <h3 className={classes.cardTitle} style={{ marginTop: "1rem" }}>
            {t("cite.pipeline")}
          </h3>
          <p className={classes.cardText}>{t("cite.pipelineText")}</p>
          <ul className={classes.pipelineList}>
            <li>Quality control: fastp</li>
            <li>Taxonomy classification: Kraken2 + Bracken (genus level)</li>
            <li>Reference database: GTDB r220</li>
            <li>Abundance normalization: Relative abundance (proportion)</li>
          </ul>
        </div>

        {/* Database Statistics */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.stats")}</h2>
          <div className={classes.statsGrid}>
            {STATS.map((s) => (
              <div key={s.key} className={classes.statItem}>
                <span className={classes.statNumber}>{s.value}</span>
                <span className={classes.statLabel}>{s.label}</span>
              </div>
            ))}
          </div>
        </div>

        {/* How to Cite */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.howToCite")}</h2>
          <p className={classes.cardText}>{t("cite.citeText")}</p>
          <p className={classes.citation}>{t("cite.citation")}</p>

          <div className={classes.bibtexWrapper}>
            <pre className={classes.bibtexCode}>{BIBTEX}</pre>
            <button className={classes.copyBtn} onClick={handleCopy}>
              {copied ? t("cite.copied") : t("cite.copyBibtex")}
            </button>
          </div>
        </div>

        {/* Contact */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.contact")}</h2>
          <p className={classes.cardText}>{t("cite.contactText")}</p>
          <ul className={classes.contactList}>
            <li>
              Email:{" "}
              <a href="mailto:daicong19901025@163.com">
                daicong19901025@163.com
              </a>
            </li>
            <li>
              GitHub:{" "}
              <a
                href="https://github.com/zhai199887/gut-microbiome-atlas"
                target="_blank"
                rel="noopener noreferrer"
              >
                github.com/zhai199887/gut-microbiome-atlas
              </a>
            </li>
          </ul>
        </div>

        {/* Disclaimer */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.disclaimer")}</h2>
          <p className={classes.disclaimer}>{t("cite.disclaimerText")}</p>
        </div>
      </div>
      <Footer />
    </>
  );
};

export default CitePage;
