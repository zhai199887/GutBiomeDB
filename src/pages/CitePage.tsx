import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import Header from "@/sections/Header";
import Footer from "@/sections/Footer";
import classes from "./CitePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const BIBTEX = `@unpublished{zhai2025gutmicrobiomeatlas,
  title   = {Gut Microbiome Atlas: a comprehensive platform for exploring
             human gut microbiome across diseases, geography, and lifespan},
  author  = {Zhai, Jinxia and Li, Yingjie and Liu, Jiameng and Su, Xinyi
             and Cui, Runze and Zheng, Dianyu and Sun, Yuhan and Yu, Jingsheng
             and Dai, Cong},
  note    = {Manuscript in preparation},
  year    = {2025}
}`;

const CitePage = () => {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);

  // 动态获取统计数据 / Dynamic stats from API
  const [apiStats, setApiStats] = useState<{
    total_samples?: number;
    total_countries?: number;
    total_diseases?: number;
  } | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    fetch(`${API_BASE}/api/data-stats`, { signal: controller.signal })
      .then((r) => r.json())
      .then(setApiStats)
      .catch(() => {});
    return () => controller.abort();
  }, []);

  const STATS = [
    { key: "samples", value: apiStats?.total_samples?.toLocaleString() ?? "168,464", labelKey: "cite.statSamples" as const },
    { key: "genera", value: "4,680", labelKey: "cite.statGenera" as const },
    { key: "countries", value: apiStats?.total_countries?.toString() ?? "66", labelKey: "cite.statCountries" as const },
    { key: "diseases", value: apiStats?.total_diseases ? `${apiStats.total_diseases}+` : "217+", labelKey: "cite.statDiseases" as const },
    { key: "stages", value: "8", labelKey: "cite.statStages" as const },
  ];

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(BIBTEX);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // fallback for HTTP / older browsers
      const textarea = document.createElement("textarea");
      textarea.value = BIBTEX;
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <>
      <Header />
      <div className={classes.container}>
        <Link to="/" className={classes.backLink}>
          {t("cite.backToAtlas")}
        </Link>

        <h1 className={classes.pageTitle}>{t("cite.title")}</h1>

        {/* Data Sources / 数据来源 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.dataSources")}</h2>
          <p className={classes.cardText}>{t("cite.dataSourcesText")}</p>

          <h3 className={classes.cardTitle} style={{ marginTop: "1rem" }}>
            {t("cite.pipeline")}
          </h3>
          <p className={classes.cardText}>{t("cite.pipelineText")}</p>
          <ul className={classes.pipelineList}>
            <li>{t("cite.pipelineQC")}</li>
            <li>{t("cite.pipelineTaxonomy")}</li>
            <li>{t("cite.pipelineRef")}</li>
            <li>{t("cite.pipelineNorm")}</li>
          </ul>
        </div>

        {/* Database Statistics / 数据库统计 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.stats")}</h2>
          <div className={classes.statsGrid}>
            {STATS.map((s) => (
              <div key={s.key} className={classes.statItem}>
                <span className={classes.statNumber}>{s.value}</span>
                <span className={classes.statLabel}>{t(s.labelKey)}</span>
              </div>
            ))}
          </div>
        </div>

        {/* How to Cite / 如何引用 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.howToCite")}</h2>
          <p className={classes.cardText}>{t("cite.citeText")}</p>
          <p className={classes.citation}>{t("cite.citation")}</p>

          <div className={classes.bibtexWrapper}>
            <pre className={classes.bibtexCode}>{BIBTEX}</pre>
            <button
              className={classes.copyBtn}
              onClick={handleCopy}
              aria-label={t("cite.copyBibtex")}
            >
              {copied ? t("cite.copied") : t("cite.copyBibtex")}
            </button>
          </div>
        </div>

        {/* Contact / 联系方式 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.contact")}</h2>
          <p className={classes.cardText}>{t("cite.contactText")}</p>
          <ul className={classes.contactList}>
            <li>
              {t("cite.emailLabel")}:{" "}
              <a href="mailto:cdai@cmu.edu.cn">cdai@cmu.edu.cn</a>
            </li>
            <li>
              {t("cite.githubLabel")}:{" "}
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

        {/* Data Processing Pipeline / 数据预处理流程 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.preprocessing")}</h2>
          <p className={classes.cardText}>{t("cite.preprocessingIntro")}</p>
          <ol className={classes.pipelineList}>
            <li>{t("cite.prepStep1")}</li>
            <li>{t("cite.prepStep2")}</li>
            <li>{t("cite.prepStep3")}</li>
            <li>{t("cite.prepStep4")}</li>
            <li>{t("cite.prepStep5")}</li>
            <li><b>{t("cite.prepStep6")}</b></li>
            <li>{t("cite.prepStep7")}</li>
            <li><b>{t("cite.prepStep8")}</b></li>
          </ol>
          <p className={classes.cardText} style={{ fontSize: "0.85rem", color: "#888", marginTop: "0.5rem" }}>
            {t("cite.prepNote")}
          </p>
        </div>

        {/* Version History / 版本历史 */}
        <div className={classes.card}>
          <h2 className={classes.cardTitle}>{t("cite.versionHistory")}</h2>
          <table className={classes.changelogTable}>
            <caption className="sr-only">{t("cite.versionHistory")}</caption>
            <thead>
              <tr>
                <th scope="col">{t("cite.versionCol")}</th>
                <th scope="col">{t("cite.dateCol")}</th>
                <th scope="col">{t("cite.changesCol")}</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><b>v2.0.0</b></td>
                <td>2026-04-04</td>
                <td>{t("cite.v2Changes")}</td>
              </tr>
              <tr>
                <td><b>v1.0.0</b></td>
                <td>2026-03-28</td>
                <td>{t("cite.v1Changes")}</td>
              </tr>
            </tbody>
          </table>
        </div>

        {/* Disclaimer / 免责声明 */}
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
