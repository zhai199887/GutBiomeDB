/**
 * SimilarityPage.tsx — 样本相似性搜索
 * 用户上传丰度向量（CSV/TSV 或粘贴文本），搜索数据库中最相似的样本
 */
import { useRef, useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import classes from "./SimilarityPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

/** 单个相似样本结果 */
interface SimilarityResult {
  sample_key: string;
  distance: number;
  similarity: number;
  disease: string;
  country: string;
}

/** API 响应 */
interface SearchResponse {
  metric: string;
  top_k: number;
  matched_genera: number;
  total_genera: number;
  results: SimilarityResult[];
}

/**
 * 解析 CSV/TSV 文本为 genus->abundance 字典
 * 支持格式：每行 "genus_name,abundance" 或 "genus_name\tabundance"
 */
function parseAbundanceText(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  const lines = text.trim().split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    // 跳过表头行
    const lower = trimmed.toLowerCase();
    if (lower.startsWith("genus") || lower.startsWith("taxon") || lower.startsWith("name")) continue;
    // 按逗号或 tab 分割
    const parts = trimmed.split(/[,\t]+/);
    if (parts.length >= 2) {
      const genus = parts[0].trim();
      const value = parseFloat(parts[1].trim());
      if (genus && !isNaN(value)) {
        result[genus] = value;
      }
    }
  }
  return result;
}

const SimilarityPage = () => {
  const { t } = useI18n();

  // ── 状态 ──
  const [file, setFile] = useState<File | null>(null);
  const [pasteText, setPasteText] = useState("");
  const [metric, setMetric] = useState("braycurtis");
  const [topK, setTopK] = useState(10);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<SearchResponse | null>(null);
  const [error, setError] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  /** 从文件读取文本内容 */
  const readFile = (f: File): Promise<string> =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = () => reject(new Error("Failed to read file"));
      reader.readAsText(f);
    });

  /** 下载模板 CSV（从 API 获取属名列表） */
  const downloadTemplate = async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/genus-names`);
      const data = await resp.json();
      const genera: string[] = data.genera ?? [];
      // 生成 CSV 内容：genus_name,abundance
      const csvLines = ["genus_name,abundance", ...genera.map(g => `${g},0`)];
      const blob = new Blob([csvLines.join("\n")], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "similarity_template.csv";
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      /* 静默失败 */
    }
  };

  /** 执行相似性搜索 */
  const runSearch = async () => {
    setError("");
    setResult(null);

    // 解析丰度数据：优先使用文件，其次使用粘贴文本
    let abundances: Record<string, number> = {};
    try {
      if (file) {
        const text = await readFile(file);
        abundances = parseAbundanceText(text);
      } else if (pasteText.trim()) {
        abundances = parseAbundanceText(pasteText);
      }
    } catch {
      setError("Failed to parse input data");
      return;
    }

    if (Object.keys(abundances).length === 0) {
      setError("No valid genus-abundance pairs found in input");
      return;
    }

    setLoading(true);
    try {
      const resp = await fetch(`${API_BASE}/api/similarity-search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ abundances, metric, top_k: topK }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${resp.status}`);
      }
      const data: SearchResponse = await resp.json();
      setResult(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setLoading(false);
    }
  };

  /** 文件选择处理 */
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0] ?? null;
    setFile(f);
  };

  /** 拖放处理 */
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const f = e.dataTransfer.files?.[0] ?? null;
    if (f) setFile(f);
  };

  const hasInput = !!file || pasteText.trim().length > 0;

  return (
    <div className={classes.page}>
      {/* ── 顶部导航 ── */}
      <div className={classes.topBar}>
        <Link to="/" className={classes.back}>{t("similarity.back")}</Link>
        <h1>{t("similarity.title")}</h1>
        <p>{t("similarity.subtitle")}</p>
      </div>

      {/* ── 输入区域：上传 + 粘贴 ── */}
      <div className={classes.inputSection}>
        {/* 文件上传 */}
        <div className={classes.uploadCard}>
          <h3>{t("similarity.upload")}</h3>
          <div
            className={`${classes.dropZone} ${file ? classes.dropZoneActive : ""}`}
            onClick={() => fileInputRef.current?.click()}
            onDrop={handleDrop}
            onDragOver={e => e.preventDefault()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,.tsv,.txt"
              onChange={handleFileChange}
            />
            <p>CSV / TSV</p>
            <p style={{ fontSize: "0.8rem" }}>genus_name, abundance</p>
          </div>
          {file && <div className={classes.fileName}>{file.name}</div>}
          <span className={classes.templateLink} onClick={downloadTemplate}>
            Download template CSV
          </span>
        </div>

        {/* 文本粘贴 */}
        <div className={classes.pasteCard}>
          <h3>{t("similarity.paste")}</h3>
          <textarea
            className={classes.textarea}
            placeholder={"Bacteroides,0.25\nFaecalibacterium,0.18\nPrevotella,0.12\n..."}
            value={pasteText}
            onChange={e => setPasteText(e.target.value)}
          />
        </div>
      </div>

      {/* ── 控制面板 ── */}
      <div className={classes.controls}>
        <div className={classes.field}>
          <label>{t("similarity.metric")}</label>
          <select className={classes.select} value={metric} onChange={e => setMetric(e.target.value)}>
            <option value="braycurtis">Bray-Curtis</option>
            <option value="jaccard">Jaccard</option>
          </select>
        </div>
        <div className={classes.field}>
          <label>Top K</label>
          <select className={classes.select} value={topK} onChange={e => setTopK(Number(e.target.value))}>
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={20}>20</option>
            <option value={50}>50</option>
          </select>
        </div>
        <button className={classes.runBtn} onClick={runSearch} disabled={!hasInput || loading}>
          {loading ? t("similarity.searching") : t("similarity.search")}
        </button>
      </div>

      {/* ── 加载状态 ── */}
      {loading && <div className={classes.loading}>{t("similarity.searching")}</div>}

      {/* ── 错误提示 ── */}
      {error && <div className={classes.error}>{error}</div>}

      {/* ── 搜索结果 ── */}
      {result && (
        <>
          {/* 统计卡片 */}
          <div className={classes.statsRow}>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.matched_genera}</span>
              <span className={classes.statLabel}>Matched Genera</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.total_genera.toLocaleString()}</span>
              <span className={classes.statLabel}>Total Genera</span>
            </div>
            <div className={classes.statCard}>
              <span className={classes.statValue}>{result.results.length}</span>
              <span className={classes.statLabel}>{t("similarity.topResults")}</span>
            </div>
          </div>

          {/* 结果表格 */}
          <div className={classes.tableCard}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <h3>{t("similarity.topResults")}</h3>
              <div style={{ display: "flex", gap: "0.5rem" }}>
                <button onClick={() => {
                  if (!result) return;
                  exportTable(
                    result.results.map((item, idx) => ({
                      Rank: idx + 1,
                      Sample_ID: item.sample_key,
                      Distance: item.distance,
                      Similarity: item.similarity,
                      Disease: item.disease,
                      Country: item.country,
                    })),
                    `similarity_results_${Date.now()}`,
                  );
                }} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem", cursor: "pointer", border: "1px solid #dee2e6", borderRadius: "4px", background: "white" }}>{t("export.csv")}</button>
              </div>
            </div>
            {result.results.length === 0 ? (
              <div className={classes.noResults}>No similar samples found</div>
            ) : (
              <table className={classes.table}>
                <thead>
                  <tr>
                    <th>{t("similarity.rank")}</th>
                    <th>Sample ID</th>
                    <th>{t("similarity.distance")}</th>
                    <th>Similarity</th>
                    <th>{t("similarity.predictedDisease")}</th>
                    <th>{t("similarity.predictedCountry")}</th>
                  </tr>
                </thead>
                <tbody>
                  {result.results.map((item, idx) => (
                    <tr key={item.sample_key}>
                      <td>{idx + 1}</td>
                      <td>{item.sample_key}</td>
                      <td>{item.distance.toFixed(4)}</td>
                      <td>
                        <span
                          className={classes.similarityBar}
                          style={{ width: `${Math.round(item.similarity * 100)}px` }}
                        />
                        {(item.similarity * 100).toFixed(1)}%
                      </td>
                      <td>{item.disease}</td>
                      <td>{item.country}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default SimilarityPage;
