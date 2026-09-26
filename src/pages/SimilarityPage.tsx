/**
 * SimilarityPage.tsx — Sample similarity search
 * Retrieves the most similar samples from the database based on a user-submitted genus-level abundance vector
 */
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { Link } from "react-router-dom";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { countryName, AGE_GROUP_ZH } from "@/util/countries";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";
import { exportTable } from "@/util/export";

import HealthIndexPanel from "./similarity/HealthIndexPanel";
import SimilarityPreviewHeatmap from "./similarity/SimilarityPreviewHeatmap";
import classes from "./SimilarityPage.module.css";

interface SimilarityResult {
  sample_key: string;
  distance: number;
  similarity_pct: number;
  disease: string;
  disease_list: string[];
  country: string;
  age_group: string;
  project_id: string;
}

interface SearchResponse {
  metric: string;
  top_k: number;
  matched_genera: number;
  total_genera: number;
  unmatched_genera: string[];
  preview_taxa: string[];
  preview_matrix: number[][];
  results: SimilarityResult[];
}

interface DiseaseListItem {
  name: string;
  sample_count: number;
}

interface FilterOptions {
  countries: string[];
  age_groups: string[];
}

function parseAbundanceText(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  const lines = text.trim().split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const lower = trimmed.toLowerCase();
    if (lower.startsWith("genus") || lower.startsWith("taxon") || lower.startsWith("name")) continue;
    const parts = trimmed.split(/[,\t]+/);
    if (parts.length < 2) continue;
    const genus = parts[0].trim();
    const value = parseFloat(parts[1].trim());
    if (genus && !Number.isNaN(value)) {
      result[genus] = value;
    }
  }
  return result;
}

const SimilarityPage = () => {
  const { t, locale } = useI18n();
  const [activeTab, setActiveTab] = useState<"similarity" | "health">("similarity");

  const [file, setFile] = useState<File | null>(null);
  const [pasteText, setPasteText] = useState("");
  const [metric, setMetric] = useState<"braycurtis" | "jaccard">("braycurtis");
  const [topK, setTopK] = useState(10);
  const [filterDisease, setFilterDisease] = useState("");
  const [filterCountry, setFilterCountry] = useState("");
  const [filterAgeGroup, setFilterAgeGroup] = useState("");
  const [diseases, setDiseases] = useState<DiseaseListItem[]>([]);
  const [filterOptions, setFilterOptions] = useState<FilterOptions>({ countries: [], age_groups: [] });
  const [loading, setLoading] = useState(false);
  const [optionsLoading, setOptionsLoading] = useState(true);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState("");

  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    Promise.all([
      cachedFetch<{ diseases: DiseaseListItem[] }>(`${API_BASE}/api/disease-list`),
      cachedFetch<FilterOptions>(`${API_BASE}/api/filter-options`),
    ])
      .then(([diseasePayload, optionPayload]) => {
        setDiseases(diseasePayload.diseases ?? []);
        setFilterOptions({
          countries: optionPayload.countries ?? [],
          age_groups: optionPayload.age_groups ?? [],
        });
      })
      .catch(() => {
        setDiseases([]);
        setFilterOptions({ countries: [], age_groups: [] });
      })
      .finally(() => setOptionsLoading(false));
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  const readFile = (candidate: File) =>
    new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = () => reject(new Error("Failed to read file"));
      reader.readAsText(candidate);
    });

  const diseaseLabel = (name: string) => {
    if (!name || name === "Unknown") return locale === "zh" ? "未知" : "Unknown";
    if (name === "NC") return locale === "zh" ? "健康对照 (NC)" : "Healthy (NC)";
    return diseaseDisplayNameI18n(name, locale);
  };

  const ageGroupLabel = (value: string) => {
    if (!value || value === "Unknown") return locale === "zh" ? "未知" : "Unknown";
    return locale === "zh" ? (AGE_GROUP_ZH[value] ?? value) : value.replace(/_/g, " ");
  };

  const formatDiseaseList = (item: SimilarityResult) => {
    const diseasesList = item.disease_list?.length ? item.disease_list : [item.disease || "Unknown"];
    return diseasesList.map(diseaseLabel).join("; ");
  };

  const downloadTemplate = async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/genus-names`);
      const data = await resp.json();
      const genera: string[] = data.genera ?? [];
      const csvLines = ["genus_name,abundance", ...genera.map((genus) => `${genus},0`)];
      const blob = new Blob([csvLines.join("\n")], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "similarity_template.csv";
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      // ignored
    }
  };

  const runSearch = async () => {
    setError("");
    setResult(null);

    let abundances: Record<string, number> = {};
    try {
      if (file) {
        const text = await readFile(file);
        abundances = parseAbundanceText(text);
      } else if (pasteText.trim()) {
        abundances = parseAbundanceText(pasteText);
      }
    } catch {
      setError(locale === "zh" ? "输入文件解析失败" : "Failed to parse input data");
      return;
    }

    if (Object.keys(abundances).length === 0) {
      setError(locale === "zh" ? "未识别到有效的属名-丰度对" : "No valid genus-abundance pairs found");
      return;
    }

    setLoading(true);
    try {
      const resp = await fetch(`${API_BASE}/api/similarity-search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          abundances,
          metric,
          top_k: topK,
          filter_disease: filterDisease,
          filter_country: filterCountry,
          filter_age_group: filterAgeGroup,
        }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${resp.status}`);
      }
      const payload: SearchResponse = await resp.json();
      setResult(payload);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : (locale === "zh" ? "搜索失败" : "Search failed"));
    } finally {
      setLoading(false);
    }
  };

  const handleDrop = (event: DragEvent) => {
    event.preventDefault();
    const dropped = event.dataTransfer.files?.[0] ?? null;
    if (dropped) setFile(dropped);
  };

  const hasInput = !!file || pasteText.trim().length > 0;

  const diseaseSummary = result
    ? Array.from(
      result.results.reduce((acc, item) => {
        const key = item.disease || "Unknown";
        acc.set(key, (acc.get(key) ?? 0) + 1);
        return acc;
      }, new Map<string, number>()),
    )
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
    : [];

  const maxDiseaseCount = diseaseSummary.length ? Math.max(...diseaseSummary.map(([, count]) => count)) : 1;
  const previewRows = result ? [
    locale === "zh" ? "查询向量" : "Query",
    ...result.results.map((item, index) => `#${index + 1} ${item.sample_key}`),
  ] : [];

  return (
    <div className={classes.page}>
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("similarity.back")}</Link>
        <h1>{t("similarity.title")}</h1>
        <p>{t("similarity.subtitle")}</p>
      </div>

      <div className={classes.tabBar}>
        <button
          className={classes.tabBtn}
          data-active={activeTab === "similarity"}
          onClick={() => setActiveTab("similarity")}
        >
          {t("similarity.search")}
        </button>
        <button
          className={classes.tabBtn}
          data-active={activeTab === "health"}
          onClick={() => setActiveTab("health")}
        >
          {t("healthIndex.tab")}
        </button>
      </div>

      {activeTab === "health" && <HealthIndexPanel />}

      {activeTab === "similarity" && (
        <>
          <div className={classes.inputSection}>
            <div className={classes.uploadCard}>
              <h3>{t("similarity.upload")}</h3>
              <div
                className={`${classes.dropZone} ${file ? classes.dropZoneActive : ""}`}
                onClick={() => fileInputRef.current?.click()}
                onDrop={handleDrop}
                onDragOver={(event) => event.preventDefault()}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,.tsv,.txt"
                  onChange={(event) => setFile(event.target.files?.[0] ?? null)}
                />
                <p>CSV / TSV</p>
                <p className={classes.helper}>genus_name, abundance</p>
              </div>
              {file && <div className={classes.fileName}>{file.name}</div>}
              <button type="button" className={classes.inlineLink} onClick={downloadTemplate}>
                {t("similarity.downloadTemplate")}
              </button>
            </div>

            <div className={classes.pasteCard}>
              <h3>{t("similarity.paste")}</h3>
              <textarea
                className={classes.textarea}
                placeholder={"Bacteroides,0.25\nFaecalibacterium,0.18\nPrevotella,0.12\n..."}
                value={pasteText}
                onChange={(event) => setPasteText(event.target.value)}
              />
            </div>
          </div>

          <div className={classes.controls}>
            <div className={classes.field}>
              <label>{t("similarity.metric")}</label>
              <select className={classes.select} value={metric} onChange={(event) => setMetric(event.target.value as "braycurtis" | "jaccard")}>
                <option value="braycurtis">Bray-Curtis</option>
                <option value="jaccard">Jaccard</option>
              </select>
            </div>

            <div className={classes.field}>
              <label>{t("similarity.topK")}</label>
              <select className={classes.select} value={topK} onChange={(event) => setTopK(Number(event.target.value))}>
                <option value={5}>5</option>
                <option value={10}>10</option>
                <option value={20}>20</option>
                <option value={50}>50</option>
              </select>
            </div>

            <div className={classes.field}>
              <label>{t("similarity.filterDisease")}</label>
              <input
                list="similarity-disease-list"
                className={classes.select}
                value={filterDisease}
                onChange={(event) => setFilterDisease(event.target.value)}
                placeholder={optionsLoading ? (locale === "zh" ? "加载中…" : "Loading…") : t("filter.searchDisease")}
              />
              <datalist id="similarity-disease-list">
                {sortedDiseases.map((item) => (
                  <option key={item.name} value={item.name}>{`${diseaseLabel(item.name)} (${item.sample_count})`}</option>
                ))}
              </datalist>
            </div>

            <div className={classes.field}>
              <label>{t("similarity.filterCountry")}</label>
              <select className={classes.select} value={filterCountry} onChange={(event) => setFilterCountry(event.target.value)}>
                <option value="">{t("filter.all")}</option>
                {filterOptions.countries.map((code) => (
                  <option key={code} value={code}>
                    {countryName(code, locale)}
                  </option>
                ))}
              </select>
            </div>

            <div className={classes.field}>
              <label>{t("similarity.filterAgeGroup")}</label>
              <select className={classes.select} value={filterAgeGroup} onChange={(event) => setFilterAgeGroup(event.target.value)}>
                <option value="">{t("filter.all")}</option>
                {filterOptions.age_groups.map((group) => (
                  <option key={group} value={group}>
                    {ageGroupLabel(group)}
                  </option>
                ))}
              </select>
            </div>

            <button className={classes.runBtn} onClick={runSearch} disabled={!hasInput || loading}>
              {loading ? t("similarity.searching") : t("similarity.search")}
            </button>
          </div>

          {loading && <div className={classes.loading}>{t("similarity.searching")}</div>}
          {error && <div className={classes.error}>{error}</div>}

          {result && (
            <>
              <div className={classes.statsRow}>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{result.matched_genera}</span>
                  <span className={classes.statLabel}>{t("similarity.matchedGenera")}</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{result.total_genera.toLocaleString()}</span>
                  <span className={classes.statLabel}>{t("similarity.totalGenera")}</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{result.results.length}</span>
                  <span className={classes.statLabel}>{t("similarity.topResults")}</span>
                </div>
              </div>

              {result.unmatched_genera.length > 0 && (
                <div className={classes.warningCard}>
                  <strong>{t("similarity.unmatched")}:</strong> {result.unmatched_genera.join(", ")}
                </div>
              )}

              <div className={classes.summaryGrid}>
                <div className={classes.summaryCard}>
                  <h3>{t("similarity.diseaseSummary")}</h3>
                  {diseaseSummary.length === 0 ? (
                    <p className={classes.emptyText}>{t("similarity.noResults")}</p>
                  ) : (
                    <div className={classes.summaryList}>
                      {diseaseSummary.map(([name, count]) => (
                        <div key={name} className={classes.summaryItem}>
                          <div className={classes.summaryHeader}>
                            <span>{diseaseLabel(name)}</span>
                            <span>{count}</span>
                          </div>
                          <div className={classes.summaryBarTrack}>
                            <div
                              className={classes.summaryBarFill}
                              style={{ width: `${(count / maxDiseaseCount) * 100}%` }}
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className={classes.summaryCard}>
                  <h3>{t("similarity.filterSummary")}</h3>
                  <div className={classes.filterSummaryList}>
                    <div><span>{t("similarity.filterDisease")}</span><strong>{filterDisease ? diseaseLabel(filterDisease) : t("filter.all")}</strong></div>
                    <div><span>{t("similarity.filterCountry")}</span><strong>{filterCountry ? countryName(filterCountry, locale) : t("filter.all")}</strong></div>
                    <div><span>{t("similarity.filterAgeGroup")}</span><strong>{filterAgeGroup ? ageGroupLabel(filterAgeGroup) : t("filter.all")}</strong></div>
                    <div><span>{t("similarity.metric")}</span><strong>{metric === "braycurtis" ? "Bray-Curtis" : "Jaccard"}</strong></div>
                  </div>
                </div>
              </div>

              <SimilarityPreviewHeatmap
                title={t("similarity.preview")}
                taxa={result.preview_taxa}
                rows={previewRows}
                matrix={result.preview_matrix}
              />

              <div className={classes.tableCard}>
                <div className={classes.tableHeader}>
                  <h3>{t("similarity.topResults")}</h3>
                  <button
                    onClick={() => {
                      exportTable(
                        result.results.map((item, index) => ({
                          Rank: index + 1,
                          Sample_ID: item.sample_key,
                          Similarity_Pct: item.similarity_pct,
                          Distance: item.distance,
                          Disease: formatDiseaseList(item),
                          Country: countryName(item.country, locale),
                          Age_Group: ageGroupLabel(item.age_group),
                          Project_ID: item.project_id,
                        })),
                        `similarity_results_${Date.now()}`,
                      );
                    }}
                    className={classes.exportBtn}
                  >
                    {t("export.csv")}
                  </button>
                </div>

                {result.results.length === 0 ? (
                  <div className={classes.noResults}>{t("similarity.noResults")}</div>
                ) : (
                  <table className={classes.table}>
                    <thead>
                      <tr>
                        <th>{t("similarity.rank")}</th>
                        <th>{t("similarity.sampleId")}</th>
                        <th>{t("similarity.similarityPct")}</th>
                        <th>{t("similarity.distance")}</th>
                        <th>{t("col.disease")}</th>
                        <th>{t("col.country")}</th>
                        <th>{t("similarity.ageGroup")}</th>
                        <th>{t("similarity.projectId")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.results.map((item, index) => (
                        <tr key={item.sample_key}>
                          <td>{index + 1}</td>
                          <td>{item.sample_key}</td>
                          <td>
                            <div className={classes.similarityCell}>
                              <span
                                className={classes.similarityBar}
                                style={{ width: `${Math.max(12, item.similarity_pct)}px` }}
                              />
                              {item.similarity_pct.toFixed(1)}%
                            </div>
                          </td>
                          <td>{item.distance.toFixed(4)}</td>
                          <td title={formatDiseaseList(item)}>{formatDiseaseList(item)}</td>
                          <td>{countryName(item.country, locale)}</td>
                          <td>{ageGroupLabel(item.age_group)}</td>
                          <td>{item.project_id || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
};

export default SimilarityPage;
