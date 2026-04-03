/**
 * DownloadPage.tsx — Data Download Center
 * 数据下载中心：仅提供聚合统计数据，不提供原始样本数据
 */
import { useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import classes from "./DownloadPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const DownloadPage = () => {
  const { t, locale } = useI18n();
  const [formats, setFormats] = useState<Record<string, string>>({});

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
          <h3>{locale === "zh" ? "聚合统计数据" : "Summary Statistics"}</h3>
          <p>{locale === "zh"
            ? "按国家、疾病、年龄组、性别的样本计数统计"
            : "Sample counts by country, disease, age group, and sex"}</p>
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
          <p>{locale === "zh" ? "4,680 个属级分类名称列表" : "4,680 genus-level taxonomy names"}</p>
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
          <p>{locale === "zh"
            ? "选定疾病的 Top 属对比统计（均值、检出率、log2FC）"
            : "Top genera comparison stats for a disease (mean, prevalence, log2FC)"}</p>
          <div className={classes.formatRow}>
            <input
              type="text"
              placeholder={locale === "zh" ? "输入疾病名..." : "Enter disease name..."}
              className={classes.formatSelect}
              id="disease-input"
            />
            <button className={classes.downloadBtn} onClick={() => {
              const input = document.getElementById("disease-input") as HTMLInputElement;
              const disease = input?.value?.trim();
              if (disease) {
                const fmt = getFormat("disease-profile");
                window.open(`${API_BASE}/api/download/disease-profile?disease=${encodeURIComponent(disease)}&format=${fmt}`, "_blank");
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
          {locale === "zh"
            ? "完整的 API 文档可通过 Swagger UI 访问："
            : "Full API documentation available via Swagger UI:"}
        </p>
        <a
          href={`${API_BASE}/docs`}
          target="_blank"
          rel="noopener noreferrer"
          className={classes.downloadBtn}
          style={{ display: "inline-block", textDecoration: "none", marginTop: "0.5rem" }}
        >
          {locale === "zh" ? "打开 Swagger UI" : "Open Swagger UI"}
        </a>
      </div>
    </div>
  );
};

export default DownloadPage;
