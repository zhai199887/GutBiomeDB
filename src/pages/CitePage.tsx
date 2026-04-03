import { useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import Header from "@/sections/Header";
import Footer from "@/sections/Footer";
import classes from "./CitePage.module.css";

const BIBTEX = `@article{zhai2025gutmicrobiomeatlas,
  title   = {Gut Microbiome Atlas: a comprehensive platform for exploring
             human gut microbiome across diseases, geography, and lifespan},
  author  = {Zhai, Jinxia and Li, Yingjie and Liu, Jiameng and Su, Xinyi
             and Cui, Runze and Zheng, Dianyu and Sun, Yuhan and Yu, Jingsheng
             and Dai, Cong},
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
              <a href="mailto:cdai@cmu.edu.cn">
                cdai@cmu.edu.cn
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

        {/* Data Processing Pipeline */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>Sample Selection & Preprocessing</h2>
          <p className={classes.cardText}>
            Following the search strategy described in a 2025 <i>Cell</i> study,
            we retrieved metadata for all biological samples from the NCBI
            &quot;human gut metagenome&quot; category.
          </p>
          <ol className={classes.pipelineList}>
            <li>Initial retrieval: 245,627 samples (1,437 BioProjects) with library_source = genomic/metagenomic and library_strategy = amplicon</li>
            <li>Excluded BioProjects with &lt;50 samples → 234,875 samples (811 BioProjects)</li>
            <li>Removed 31,887 non-applicable samples (fungi, archaea, mislabeled shotgun/nanopore)</li>
            <li>Removed 31,509 samples from BioProjects with &gt;25% chimeric reads</li>
            <li>Removed 807 samples with zero reads after quality filtering</li>
            <li>Aggregated into a global matrix: <b>168,464 samples × 4,018 taxa</b> (482 BioProjects)</li>
            <li>Quality filtering: removed samples with &lt;10K reads, rare taxa (&lt;1K total reads or &lt;100 samples), samples with &gt;10% unclassified at phylum level</li>
            <li>Final filtered dataset: <b>121,601 samples × 1,514 taxa</b></li>
          </ol>
          <p className={classes.cardText} style={{ fontSize: "0.85rem", color: "#888", marginTop: "0.5rem" }}>
            Note: This platform displays the unfiltered compendium (168,464 samples × 4,680 genera).
            The filtered dataset (121,601 samples) is used for statistical analyses.
          </p>
        </div>

        {/* Version History / Changelog */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>Version History</h2>
          <table className={classes.changelogTable}>
            <thead>
              <tr><th>Version</th><th>Date</th><th>Changes</th></tr>
            </thead>
            <tbody>
              <tr>
                <td><b>v2.0.0</b></td>
                <td>2026-04-04</td>
                <td>
                  API documentation with Python/R/cURL examples; CSV/SVG/PNG export for all analysis pages;
                  Disease ontology mapping (204 diseases with MeSH/ICD-10); Rate limiting &amp; API versioning (/api/v1/);
                  About &amp; Cite page; Data preprocessing documentation
                </td>
              </tr>
              <tr>
                <td><b>v1.0.0</b></td>
                <td>2026-03-28</td>
                <td>
                  Initial release with 8 analysis modules: Phenotype overview, Disease browser, Differential analysis,
                  Metabolism pathway, Species profile, Network visualization, Lifecycle atlas, Sample similarity search.
                  11 D3.js chart types, bilingual (EN/ZH) interface
                </td>
              </tr>
            </tbody>
          </table>
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
