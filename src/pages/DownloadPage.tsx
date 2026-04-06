import { useDeferredValue, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { countryName } from "@/util/countries";
import { diseaseShortNameI18n } from "@/util/diseaseNames";

import classes from "./DownloadPage.module.css";

const LOCAL_API_BASE = "http://localhost:8000";
const PUBLIC_API_BASE = "https://32c774a.r12.cpolar.top";
const ACTIVE_API_BASE = import.meta.env.VITE_API_URL ?? LOCAL_API_BASE;
const FORMATS = ["csv", "tsv", "json"] as const;

type DownloadFormat = (typeof FORMATS)[number];

interface DiseaseItem {
  name: string;
  sample_count?: number;
}

interface DiseaseListResponse {
  diseases: DiseaseItem[];
}

interface GenusNamesResponse {
  genera: string[];
}

interface FilterOptionsResponse {
  countries: string[];
}

interface OptionItem {
  value: string;
  label: string;
  hint?: string;
}

const copy = {
  en: {
    back: "Back to Atlas",
    title: "Download",
    subtitle:
      "Reference datasets and aggregated analysis outputs for reproducible reuse. Raw sample-level abundance data are not distributed through this page.",
    warning:
      "All files exported here are aggregated statistics or analysis results generated from the main platform dataset. Use the original BioProject repositories for raw sequencing data.",
    sections: {
      referenceEyebrow: "Reference Datasets",
      referenceTitle: "Core atlas tables",
      referenceText:
        "Starter downloads for local analysis, cohort selection, and reproducible reporting.",
      analysisEyebrow: "Analysis Results",
      analysisTitle: "Module-specific exports",
      analysisText:
        "Download the same aggregated outputs used by Compare, Network, and Lifecycle rather than reconstructing them manually.",
      codeEyebrow: "Code Examples",
      codeTitle: "Local and public access",
      codeText:
        "Use localhost for development and validation. The public cpolar address is convenient for remote demos but is an operational endpoint and may change.",
    },
    cards: {
      summaryTitle: "Summary statistics",
      summaryDesc: "Country, disease, age-group, and sex counts for the full atlas.",
      genusListTitle: "Genus list",
      genusListDesc: "Complete valid genus catalog for templates, validation, and batch workflows.",
      diseaseProfileTitle: "Disease profile",
      diseaseProfileDesc: "Top disease-associated genera and control comparison for one disease context.",
      genusProfileTitle: "Genus profile",
      genusProfileDesc: "Cross-disease descriptive profile for one genus entry point.",
      diffTitle: "Differential results",
      diffDesc: "Disease-vs-control differential abundance table ready for downstream plotting or review.",
      biomarkerTitle: "Biomarker discovery",
      biomarkerDesc: "LEfSe-style biomarker output with effect size and adjusted p-values.",
      cooccurrenceTitle: "Co-occurrence edges",
      cooccurrenceDesc: "Network edge table from the disease or strict-NC co-occurrence view.",
      lifecycleTitle: "Lifecycle atlas",
      lifecycleDesc: "Age-stage abundance trajectories and diversity summaries for lifecycle analysis.",
    },
    labels: {
      format: "Format",
      disease: "Disease",
      genus: "Genus",
      topN: "Top N",
      lda: "LDA threshold",
      minR: "Min |r|",
      country: "Country",
      lifecycleDisease: "Lifecycle disease",
      publicApi: "Public API",
      localApi: "Local API",
      noFilter: "Strict NC / default",
      noCountry: "All countries",
      searchDisease: "Search disease",
      searchGenus: "Search genus",
      openDocs: "Open API docs",
      openSwagger: "Open Swagger UI",
      download: "Download",
      required: "Required",
      optional: "Optional",
    },
    helper: {
      publicNote:
        "Public endpoint note: this cpolar URL reflects the current tunnel target and should not be treated as a permanent paper citation URL.",
      formatNote:
        "Every download response includes generation date, version, and citation guidance in HTTP headers.",
      diffNote: "Exports the statistically ranked result table, not raw abundance matrices.",
      biomarkerNote: "Current backend exports the same marker payload shown in Diseases and Species workspaces.",
      cooccurrenceNote: "Leave disease blank to export the strict-NC co-occurrence network.",
      lifecycleNote: "Leave disease blank to export the healthy lifecycle baseline.",
    },
    envTitle: {
      local: "Local development",
      public: "Public demo",
    },
    emptyState: {
      noMatches: "No matches",
      chooseDisease: "Select a disease first",
      chooseGenus: "Select a genus first",
    },
  },
  zh: {
    back: "返回 Atlas",
    title: "Download",
    subtitle:
      "提供参考数据表和聚合分析结果，便于复现使用。本页不分发原始样本级丰度矩阵。",
    warning:
      "这里导出的都是聚合统计或分析结果，不包含原始样本数据。若需要原始测序数据，应回到对应 BioProject 或原始数据库获取。",
    sections: {
      referenceEyebrow: "Reference Datasets",
      referenceTitle: "核心参考表",
      referenceText: "用于本地分析、队列筛选和复现报告的基础下载入口。",
      analysisEyebrow: "Analysis Results",
      analysisTitle: "模块分析导出",
      analysisText: "直接下载 Compare、Network、Lifecycle 等模块已经计算好的聚合结果，而不是自己重跑一遍。",
      codeEyebrow: "Code Examples",
      codeTitle: "本地与公网访问",
      codeText:
        "开发和验收优先使用 localhost。本页同时给出当前公网 API，但 cpolar 地址属于运维层入口，后续可能变化。",
    },
    cards: {
      summaryTitle: "Summary statistics",
      summaryDesc: "导出全库的国家、疾病、年龄组和性别统计。",
      genusListTitle: "Genus list",
      genusListDesc: "导出完整有效菌属目录，适合模板校验和批量流程。",
      diseaseProfileTitle: "Disease profile",
      diseaseProfileDesc: "导出单个疾病场景下的优势菌属与对照比较结果。",
      genusProfileTitle: "Genus profile",
      genusProfileDesc: "导出单个菌属的跨疾病描述性画像表。",
      diffTitle: "Differential results",
      diffDesc: "导出疾病 vs 对照的差异丰度统计表，便于复查和再作图。",
      biomarkerTitle: "Biomarker discovery",
      biomarkerDesc: "导出 LEfSe 风格的标志物结果，包含效应量和校正 p 值。",
      cooccurrenceTitle: "Co-occurrence edges",
      cooccurrenceDesc: "导出疾病或严格 NC 场景下的共现网络边表。",
      lifecycleTitle: "Lifecycle atlas",
      lifecycleDesc: "导出生命周期轨迹中的年龄阶段丰度和多样性摘要。",
    },
    labels: {
      format: "格式",
      disease: "疾病",
      genus: "菌属",
      topN: "Top N",
      lda: "LDA 阈值",
      minR: "最小 |r|",
      country: "国家",
      lifecycleDisease: "生命周期疾病筛选",
      publicApi: "公网 API",
      localApi: "本地 API",
      noFilter: "严格 NC / 默认",
      noCountry: "全部国家",
      searchDisease: "搜索疾病",
      searchGenus: "搜索菌属",
      openDocs: "打开 API 文档",
      openSwagger: "打开 Swagger UI",
      download: "下载",
      required: "必填",
      optional: "可选",
    },
    helper: {
      publicNote:
        "公网说明：这里展示的是当前 cpolar 地址，方便远程演示，但不应当被当作永久论文链接。",
      formatNote:
        "所有下载响应都会在 HTTP 头里带上生成时间、版本号和引用提示。",
      diffNote: "这里导出的是统计排序后的结果表，不是原始丰度矩阵。",
      biomarkerNote: "导出内容与 Diseases / Species 模块里看到的 marker 结果一致。",
      cooccurrenceNote: "疾病留空时，默认导出严格 NC 场景下的共现网络。",
      lifecycleNote: "疾病留空时，默认导出健康生命周期基线。",
    },
    envTitle: {
      local: "本地开发",
      public: "公网演示",
    },
    emptyState: {
      noMatches: "没有匹配项",
      chooseDisease: "请先选择疾病",
      chooseGenus: "请先选择菌属",
    },
  },
} as const;

interface SearchComboboxProps {
  ariaLabel: string;
  emptyLabel: string;
  onChange: (nextValue: string) => void;
  options: OptionItem[];
  placeholder: string;
  value: string;
}

function SearchCombobox({
  ariaLabel,
  emptyLabel,
  onChange,
  options,
  placeholder,
  value,
}: SearchComboboxProps) {
  const rootRef = useRef<HTMLDivElement>(null);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const deferredQuery = useDeferredValue(query);

  const selected = useMemo(
    () => options.find((item) => item.value === value) ?? null,
    [options, value],
  );

  useEffect(() => {
    setQuery(selected?.label ?? "");
  }, [selected]);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  const filtered = useMemo(() => {
    const keyword = deferredQuery.trim().toLowerCase();
    if (!keyword) return options.slice(0, 10);
    return options
      .filter((item) => {
        const label = item.label.toLowerCase();
        const hint = item.hint?.toLowerCase() ?? "";
        return label.includes(keyword) || hint.includes(keyword) || item.value.toLowerCase().includes(keyword);
      })
      .slice(0, 12);
  }, [deferredQuery, options]);

  const selectOption = (nextValue: string) => {
    const matched = options.find((item) => item.value === nextValue) ?? null;
    onChange(nextValue);
    setQuery(matched?.label ?? "");
    setOpen(false);
  };

  return (
    <div ref={rootRef} className={classes.combobox}>
      <div className={classes.comboboxFrame}>
        <input
          aria-label={ariaLabel}
          className={classes.comboboxInput}
          onChange={(event) => {
            setQuery(event.target.value);
            if (event.target.value.trim() === "") onChange("");
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              if (filtered[0]) {
                selectOption(filtered[0].value);
              }
            }
            if (event.key === "Escape") {
              setOpen(false);
              setQuery(selected?.label ?? "");
            }
          }}
          placeholder={placeholder}
          value={query}
        />
        {value ? (
          <button
            type="button"
            className={classes.clearButton}
            onClick={() => {
              onChange("");
              setQuery("");
              setOpen(false);
            }}
          >
            ×
          </button>
        ) : null}
      </div>
      {open ? (
        <div className={classes.comboboxMenu}>
          {filtered.length > 0 ? (
            filtered.map((item) => (
              <button
                key={item.value}
                type="button"
                className={classes.comboboxOption}
                onMouseDown={(event) => event.preventDefault()}
                onClick={() => selectOption(item.value)}
              >
                <span>{item.label}</span>
                {item.hint ? <small>{item.hint}</small> : null}
              </button>
            ))
          ) : (
            <div className={classes.emptyOption}>{emptyLabel}</div>
          )}
        </div>
      ) : null}
    </div>
  );
}

interface CardShellProps {
  children: ReactNode;
  description: string;
  helper?: string;
  title: string;
}

function CardShell({ children, description, helper, title }: CardShellProps) {
  return (
    <article className={classes.card}>
      <div className={classes.cardHeader}>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
      <div className={classes.cardBody}>{children}</div>
      {helper ? <p className={classes.helperText}>{helper}</p> : null}
    </article>
  );
}

const DownloadPage = () => {
  const { locale } = useI18n();
  const text = copy[locale];

  const [formats, setFormats] = useState<Record<string, DownloadFormat>>({});
  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [genera, setGenera] = useState<string[]>([]);
  const [countries, setCountries] = useState<string[]>([]);

  const [diseaseProfileDisease, setDiseaseProfileDisease] = useState("");
  const [diffDisease, setDiffDisease] = useState("");
  const [biomarkerDisease, setBiomarkerDisease] = useState("");
  const [cooccurrenceDisease, setCooccurrenceDisease] = useState("");
  const [lifecycleDisease, setLifecycleDisease] = useState("");
  const [selectedGenus, setSelectedGenus] = useState("");
  const [lifecycleCountry, setLifecycleCountry] = useState("");

  const [diffTopN, setDiffTopN] = useState("200");
  const [ldaThreshold, setLdaThreshold] = useState("2.0");
  const [minR, setMinR] = useState("0.3");
  const [lifecycleTopGenera, setLifecycleTopGenera] = useState("15");

  useEffect(() => {
    Promise.all([
      cachedFetch<DiseaseListResponse>(`${ACTIVE_API_BASE}/api/disease-list`),
      cachedFetch<GenusNamesResponse>(`${ACTIVE_API_BASE}/api/genus-names`),
      cachedFetch<FilterOptionsResponse>(`${ACTIVE_API_BASE}/api/filter-options`),
    ])
      .then(([diseasePayload, genusPayload, filterPayload]) => {
        setDiseases((diseasePayload.diseases ?? []).filter((item) => item.name !== "NC"));
        setGenera(genusPayload.genera ?? []);
        setCountries(filterPayload.countries ?? []);
      })
      .catch(() => {});
  }, []);

  const diseaseOptions = useMemo(
    () =>
      diseases.map((item) => ({
        value: item.name,
        label: diseaseShortNameI18n(item.name, locale, 48),
        hint: typeof item.sample_count === "number" ? `n=${item.sample_count}` : undefined,
      })),
    [diseases, locale],
  );

  const genusOptions = useMemo(
    () =>
      genera.map((genus) => ({
        value: genus,
        label: genus,
      })),
    [genera],
  );

  const getFormat = (key: string) => formats[key] ?? "csv";

  const setFormat = (key: string, next: DownloadFormat) => {
    setFormats((previous) => ({ ...previous, [key]: next }));
  };

  const triggerDownload = (endpoint: string, params: Record<string, string | number | undefined>) => {
    const url = new URL(endpoint, ACTIVE_API_BASE);
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined) return;
      const normalized = typeof value === "string" ? value.trim() : value;
      if (normalized === "") return;
      url.searchParams.set(key, String(normalized));
    });
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const renderFormatSelect = (key: string) => (
    <label className={classes.field}>
      <span>{text.labels.format}</span>
      <select
        className={classes.select}
        value={getFormat(key)}
        onChange={(event) => setFormat(key, event.target.value as DownloadFormat)}
      >
        {FORMATS.map((formatName) => (
          <option key={formatName} value={formatName}>
            {formatName.toUpperCase()}
          </option>
        ))}
      </select>
    </label>
  );

  const localPythonExample = `import requests

base = "${LOCAL_API_BASE}"
summary = requests.get(f"{base}/api/download/summary-stats", params={"format": "json"}).json()
diff_rows = requests.get(
    f"{base}/api/download/diff-results",
    params={"disease": "IBD", "top_n": 100, "format": "json"},
).json()["results"]

print(summary["total_samples"])
print(diff_rows[0]["genus"], diff_rows[0]["log2fc"])`;

  const publicCurlExample = `BASE="${PUBLIC_API_BASE}"

curl -s "${PUBLIC_API_BASE}/api/download/summary-stats?format=json"
curl -s "${PUBLIC_API_BASE}/api/download/biomarkers?disease=CRC&lda_threshold=2.0&format=csv" -o crc_biomarkers.csv
curl -s "${PUBLIC_API_BASE}/api/openapi.json" -o gut_microbiome_atlas_openapi.json`;

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>
          {text.back}
        </Link>
        <h1>{text.title}</h1>
        <p>{text.subtitle}</p>
      </div>

      <div className={classes.warningBanner}>{text.warning}</div>

      <section className={classes.section}>
        <div className={classes.sectionHeader}>
          <span>{text.sections.referenceEyebrow}</span>
          <h2>{text.sections.referenceTitle}</h2>
          <p>{text.sections.referenceText}</p>
        </div>

        <div className={classes.grid}>
          <CardShell
            title={text.cards.summaryTitle}
            description={text.cards.summaryDesc}
            helper={text.helper.formatNote}
          >
            {renderFormatSelect("summary")}
            <button
              type="button"
              className={classes.downloadButton}
              onClick={() => triggerDownload("/api/download/summary-stats", { format: getFormat("summary") })}
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.genusListTitle}
            description={text.cards.genusListDesc}
          >
            {renderFormatSelect("genus-list")}
            <button
              type="button"
              className={classes.downloadButton}
              onClick={() => triggerDownload("/api/download/genus-list", { format: getFormat("genus-list") })}
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.diseaseProfileTitle}
            description={text.cards.diseaseProfileDesc}
          >
            <label className={classes.field}>
              <span>{text.labels.disease} <em>{text.labels.required}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.disease}
                emptyLabel={text.emptyState.noMatches}
                onChange={setDiseaseProfileDisease}
                options={diseaseOptions}
                placeholder={text.labels.searchDisease}
                value={diseaseProfileDisease}
              />
            </label>
            {renderFormatSelect("disease-profile")}
            <button
              type="button"
              className={classes.downloadButton}
              disabled={!diseaseProfileDisease}
              onClick={() =>
                triggerDownload("/api/download/disease-profile", {
                  disease: diseaseProfileDisease,
                  format: getFormat("disease-profile"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.genusProfileTitle}
            description={text.cards.genusProfileDesc}
          >
            <label className={classes.field}>
              <span>{text.labels.genus} <em>{text.labels.required}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.genus}
                emptyLabel={text.emptyState.noMatches}
                onChange={setSelectedGenus}
                options={genusOptions}
                placeholder={text.labels.searchGenus}
                value={selectedGenus}
              />
            </label>
            {renderFormatSelect("genus-profile")}
            <button
              type="button"
              className={classes.downloadButton}
              disabled={!selectedGenus}
              onClick={() =>
                triggerDownload("/api/download/species-profile", {
                  genus: selectedGenus,
                  format: getFormat("genus-profile"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>
        </div>
      </section>

      <section className={classes.section}>
        <div className={classes.sectionHeader}>
          <span>{text.sections.analysisEyebrow}</span>
          <h2>{text.sections.analysisTitle}</h2>
          <p>{text.sections.analysisText}</p>
        </div>

        <div className={classes.grid}>
          <CardShell
            title={text.cards.diffTitle}
            description={text.cards.diffDesc}
            helper={text.helper.diffNote}
          >
            <label className={classes.field}>
              <span>{text.labels.disease} <em>{text.labels.required}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.disease}
                emptyLabel={text.emptyState.noMatches}
                onChange={setDiffDisease}
                options={diseaseOptions}
                placeholder={text.labels.searchDisease}
                value={diffDisease}
              />
            </label>
            <div className={classes.inlineFields}>
              <label className={classes.field}>
                <span>{text.labels.topN}</span>
                <input
                  className={classes.input}
                  type="number"
                  min="1"
                  value={diffTopN}
                  onChange={(event) => setDiffTopN(event.target.value)}
                />
              </label>
              {renderFormatSelect("diff-results")}
            </div>
            <button
              type="button"
              className={classes.downloadButton}
              disabled={!diffDisease}
              onClick={() =>
                triggerDownload("/api/download/diff-results", {
                  disease: diffDisease,
                  top_n: diffTopN || "200",
                  format: getFormat("diff-results"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.biomarkerTitle}
            description={text.cards.biomarkerDesc}
            helper={text.helper.biomarkerNote}
          >
            <label className={classes.field}>
              <span>{text.labels.disease} <em>{text.labels.required}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.disease}
                emptyLabel={text.emptyState.noMatches}
                onChange={setBiomarkerDisease}
                options={diseaseOptions}
                placeholder={text.labels.searchDisease}
                value={biomarkerDisease}
              />
            </label>
            <div className={classes.inlineFields}>
              <label className={classes.field}>
                <span>{text.labels.lda}</span>
                <input
                  className={classes.input}
                  type="number"
                  step="0.1"
                  min="0"
                  value={ldaThreshold}
                  onChange={(event) => setLdaThreshold(event.target.value)}
                />
              </label>
              {renderFormatSelect("biomarkers")}
            </div>
            <button
              type="button"
              className={classes.downloadButton}
              disabled={!biomarkerDisease}
              onClick={() =>
                triggerDownload("/api/download/biomarkers", {
                  disease: biomarkerDisease,
                  lda_threshold: ldaThreshold || "2.0",
                  format: getFormat("biomarkers"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.cooccurrenceTitle}
            description={text.cards.cooccurrenceDesc}
            helper={text.helper.cooccurrenceNote}
          >
            <label className={classes.field}>
              <span>{text.labels.disease} <em>{text.labels.optional}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.disease}
                emptyLabel={text.emptyState.noMatches}
                onChange={setCooccurrenceDisease}
                options={diseaseOptions}
                placeholder={text.labels.noFilter}
                value={cooccurrenceDisease}
              />
            </label>
            <div className={classes.inlineFields}>
              <label className={classes.field}>
                <span>{text.labels.minR}</span>
                <input
                  className={classes.input}
                  type="number"
                  step="0.05"
                  min="0"
                  max="1"
                  value={minR}
                  onChange={(event) => setMinR(event.target.value)}
                />
              </label>
              {renderFormatSelect("cooccurrence")}
            </div>
            <button
              type="button"
              className={classes.downloadButton}
              onClick={() =>
                triggerDownload("/api/download/cooccurrence", {
                  disease: cooccurrenceDisease,
                  min_r: minR || "0.3",
                  format: getFormat("cooccurrence"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>

          <CardShell
            title={text.cards.lifecycleTitle}
            description={text.cards.lifecycleDesc}
            helper={text.helper.lifecycleNote}
          >
            <label className={classes.field}>
              <span>{text.labels.lifecycleDisease} <em>{text.labels.optional}</em></span>
              <SearchCombobox
                ariaLabel={text.labels.lifecycleDisease}
                emptyLabel={text.emptyState.noMatches}
                onChange={setLifecycleDisease}
                options={diseaseOptions}
                placeholder={text.labels.noFilter}
                value={lifecycleDisease}
              />
            </label>
            <div className={classes.inlineFields}>
              <label className={classes.field}>
                <span>{text.labels.country}</span>
                <select
                  className={classes.select}
                  value={lifecycleCountry}
                  onChange={(event) => setLifecycleCountry(event.target.value)}
                >
                  <option value="">{text.labels.noCountry}</option>
                  {countries.map((countryCode) => (
                    <option key={countryCode} value={countryCode}>
                      {countryName(countryCode, locale)} ({countryCode})
                    </option>
                  ))}
                </select>
              </label>
              <label className={classes.field}>
                <span>{text.labels.topN}</span>
                <input
                  className={classes.input}
                  type="number"
                  min="1"
                  value={lifecycleTopGenera}
                  onChange={(event) => setLifecycleTopGenera(event.target.value)}
                />
              </label>
              {renderFormatSelect("lifecycle")}
            </div>
            <button
              type="button"
              className={classes.downloadButton}
              onClick={() =>
                triggerDownload("/api/download/lifecycle", {
                  disease: lifecycleDisease,
                  country: lifecycleCountry,
                  top_genera: lifecycleTopGenera || "15",
                  format: getFormat("lifecycle"),
                })
              }
            >
              {text.labels.download}
            </button>
          </CardShell>
        </div>
      </section>

      <section className={classes.section}>
        <div className={classes.sectionHeader}>
          <span>{text.sections.codeEyebrow}</span>
          <h2>{text.sections.codeTitle}</h2>
          <p>{text.sections.codeText}</p>
        </div>

        <div className={classes.codeGrid}>
          <article className={classes.codeCard}>
            <div className={classes.codeHeader}>
              <h3>{text.envTitle.local}</h3>
              <span>{text.labels.localApi}</span>
            </div>
            <p className={classes.endpointLine}>{LOCAL_API_BASE}</p>
            <pre className={classes.codeBlock}>{localPythonExample}</pre>
          </article>

          <article className={classes.codeCard}>
            <div className={classes.codeHeader}>
              <h3>{text.envTitle.public}</h3>
              <span>{text.labels.publicApi}</span>
            </div>
            <p className={classes.endpointLine}>{PUBLIC_API_BASE}</p>
            <pre className={classes.codeBlock}>{publicCurlExample}</pre>
          </article>
        </div>

        <p className={classes.publicNote}>{text.helper.publicNote}</p>

        <div className={classes.docsRow}>
          <a className={classes.secondaryButton} href={`${ACTIVE_API_BASE}/api/openapi.json`} target="_blank" rel="noreferrer">
            {text.labels.openDocs}
          </a>
          <a className={classes.secondaryButton} href={`${ACTIVE_API_BASE}/api/docs`} target="_blank" rel="noreferrer">
            {text.labels.openSwagger}
          </a>
        </div>
      </section>
    </div>
  );
};

export default DownloadPage;
