/**
 * DownloadPage.tsx — Data Download Center
 * 数据下载中心：仅提供聚合统计数据，不提供原始样本数据
 */
import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import { diseaseShortNameI18n } from "@/util/diseaseNames";
import classes from "./DownloadPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const DownloadPage = () => {
  const { t, locale } = useI18n();
  const [formats, setFormats] = useState<Record<string, string>>({});
  const [diseaseList, setDiseaseList] = useState<string[]>([]);
  const [selectedDisease, setSelectedDisease] = useState("");

  useEffect(() => {
    fetch(`${API_BASE}/api/disease-list`)
      .then((r) => r.json())
      .then((data: { diseases: { name: string }[] }) => {
        setDiseaseList(data.diseases.map((d) => d.name).filter((n) => n !== "NC"));
      })
      .catch(() => {});
  }, []);

  const getFormat = (key: string) => formats[key] ?? "csv";
  const setFormat = (key: string, fmt: string) =>
    setFormats((prev) => ({ ...prev, [key]: fmt }));

  const download = (endpoint: string, key: string) => {
    const fmt = getFormat(key);
    window.open(`${API_BASE}${endpoint}?format=${fmt}`, "_blank");
  };

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("download.back")}</Link>
        <h1>{t("download.title")}</h1>
        <p>{t("download.subtitle")}</p>
      </div>

      <div className={classes.cards}>
        {/* Summary statistics (aggregated, not raw) */}
        <div className={classes.card}>
          <h3>{t("download.summaryTitle")}</h3>
          <p>{t("download.summaryDesc")}</p>
          <div className={classes.formatRow}>
            <select className={classes.formatSelect} value={getFormat("summary")} onChange={(e) => setFormat("summary", e.target.value)}>
              <option value="csv">CSV</option>
              <option value="tsv">TSV</option>
              <option value="json">JSON</option>
            </select>
            <button className={classes.downloadBtn} onClick={() => download("/api/download/summary-stats", "summary")}>
              {t("download.download")}
            </button>
          </div>
        </div>

        {/* Genus list */}
        <div className={classes.card}>
          <h3>{t("download.abundance")}</h3>
          <p>{t("download.genusDesc")}</p>
          <div className={classes.formatRow}>
            <select className={classes.formatSelect} value={getFormat("genus-list")} onChange={(e) => setFormat("genus-list", e.target.value)}>
              <option value="csv">CSV</option>
              <option value="tsv">TSV</option>
              <option value="json">JSON</option>
            </select>
            <button className={classes.downloadBtn} onClick={() => download("/api/download/genus-list", "genus-list")}>
              {t("download.download")}
            </button>
          </div>
        </div>

        {/* Disease profile */}
        <div className={classes.card}>
          <h3>{t("download.diseaseProfile")}</h3>
          <p>{t("download.diseaseProfileDesc")}</p>
          <div className={classes.formatRow}>
            <select
              className={classes.formatSelect}
              value={selectedDisease}
              onChange={(e) => setSelectedDisease(e.target.value)}
            >
              <option value="">{t("download.diseasePlaceholder")}</option>
              {diseaseList.map((d) => (
                <option key={d} value={d}>{diseaseShortNameI18n(d, locale, 40)}</option>
              ))}
            </select>
            <button className={classes.downloadBtn} onClick={() => {
              if (selectedDisease) {
                const fmt = getFormat("disease-profile");
                window.open(`${API_BASE}/api/download/disease-profile?disease=${encodeURIComponent(selectedDisease)}&format=${fmt}`, "_blank");
              }
            }}>
              {t("download.download")}
            </button>
          </div>
        </div>
      </div>

      {/* Code examples */}
      <div className={classes.codeSection}>
        <h2>{t("download.codeExamples")}</h2>

        <div className={classes.codeLabel}>Python</div>
        <div className={classes.codeBlock}>{`import requests

# Get summary statistics
stats = requests.get("${API_BASE}/api/download/summary-stats?format=json").json()
print(f"Total samples: {stats['total_samples']}")

# Get disease profile
profile = requests.get("${API_BASE}/api/disease-profile?disease=obesity").json()
for g in profile['top_genera'][:5]:
    print(f"{g['genus']}: {g['disease_mean']:.2f}% (log2FC={g['log2fc']:.2f})")`}</div>

        <div className={classes.codeLabel}>R</div>
        <div className={classes.codeBlock}>{`library(httr)
library(jsonlite)

# Get summary statistics
stats <- fromJSON(content(
  GET("${API_BASE}/api/download/summary-stats?format=json"), "text"))
cat("Total samples:", stats$total_samples, "\\n")

# Get species profile
profile <- fromJSON(content(
  GET("${API_BASE}/api/species-profile?genus=Bacteroides"), "text"))
cat("Mean abundance:", profile$mean_abundance, "\\n")`}</div>
      </div>

      {/* Swagger API docs link */}
      <div className={classes.codeSection}>
        <h2>{t("download.apiDocs")}</h2>
        <p style={{ color: "var(--gray)", fontSize: "0.9rem" }}>
          {t("download.swaggerDesc")}
        </p>
        <a
          href={`${API_BASE}/docs`}
          target="_blank"
          rel="noopener noreferrer"
          className={classes.downloadBtn}
          style={{ display: "inline-block", textDecoration: "none", marginTop: "0.5rem" }}
        >
          {t("download.openSwagger")}
        </a>
      </div>
    </div>
  );
};

export default DownloadPage;
