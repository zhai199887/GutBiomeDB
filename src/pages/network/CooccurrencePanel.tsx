import { useEffect, useMemo, useRef, useState } from "react";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportPNG, exportSVG } from "@/util/chartExport";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";
import { exportTable } from "@/util/export";

import NetworkEdgeTable from "./NetworkEdgeTable";
import styles from "./NetworkPanel.module.css";
import NetworkTopologyTable from "./NetworkTopologyTable";
import { drawCooccurrenceGraph } from "./graphUtils";
import type { CoData, ColorMode, DiseaseItem, NetworkMethod } from "./types";
import { API_BASE } from "./types";

const CooccurrencePanel = () => {
  const { locale } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);

  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [disease, setDisease] = useState("");
  const [minR, setMinR] = useState(0.3);
  const [method, setMethod] = useState<NetworkMethod>("spearman");
  const [colorMode, setColorMode] = useState<ColorMode>("phylum");
  const [data, setData] = useState<CoData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    cachedFetch<{ diseases: DiseaseItem[] }>(`${API_BASE}/api/disease-list`)
      .then((payload) => setDiseases(payload.diseases ?? []))
      .catch(() => {});
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  useEffect(() => {
    setLoading(true);
    setError("");
    const params = new URLSearchParams({
      min_r: String(minR),
      top_genera: "45",
      method,
      fdr_threshold: "0.05",
    });
    if (disease) params.set("disease", disease);

    cachedFetch<CoData>(`${API_BASE}/api/cooccurrence?${params}`)
      .then((payload) => setData(payload))
      .catch((unknownError) => {
        setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
      })
      .finally(() => setLoading(false));
  }, [disease, method, minR]);

  useEffect(() => {
    if (!svgRef.current || !data) return undefined;
    return drawCooccurrenceGraph(svgRef.current, data, { locale, colorMode });
  }, [colorMode, data, locale]);

  const availableMethods = data?.available_methods ?? { spearman: true, sparcc: true };
  const methodNote = useMemo(() => {
    if (data?.method_note) return data.method_note;
    if (!availableMethods.sparcc) {
      return locale === "zh"
        ? "SparCC 依赖在当前 Windows 运行环境里不可用，当前仅开放 Spearman 真正计算。"
        : "SparCC is unavailable in the current Windows runtime, so only the real Spearman workflow is enabled.";
    }
    return "";
  }, [availableMethods.sparcc, data?.method_note, locale]);

  const exportEdges = () => {
    if (!data) return;
    exportTable(
      data.edges.map((edge) => ({
        Source: typeof edge.source === "string" ? edge.source : edge.source.id,
        Target: typeof edge.target === "string" ? edge.target : edge.target.id,
        R: edge.r,
        P_value: edge.p_value,
        Adjusted_p: edge.adjusted_p,
        Type: edge.type,
        Method: edge.method,
      })),
      `network_edges_${Date.now()}`,
    );
  };

  return (
    <div className={styles.workspace}>
      <div className={styles.toolbar}>
        <div className={styles.field}>
          <label>{locale === "zh" ? "疾病背景" : "Disease context"}</label>
          <input
            list="network-disease-list"
            className={styles.input}
            value={disease}
            onChange={(event) => setDisease(event.target.value)}
            placeholder={locale === "zh" ? "留空表示健康对照" : "Leave blank for healthy controls"}
          />
          <datalist id="network-disease-list">
            {sortedDiseases.map((item) => (
              <option key={item.name} value={item.name} label={diseaseDisplayNameI18n(item.name, locale)} />
            ))}
          </datalist>
        </div>

        <div className={styles.field}>
          <label>{locale === "zh" ? "最小 |r|" : "Min |r|"}</label>
          <select className={styles.select} value={minR} onChange={(event) => setMinR(Number(event.target.value))}>
            {[0.2, 0.3, 0.4, 0.5].map((value) => (
              <option key={value} value={value}>{value}</option>
            ))}
          </select>
        </div>

        <div className={styles.field}>
          <label>{locale === "zh" ? "计算方法" : "Method"}</label>
          <div className={styles.btnGroup}>
            <button
              type="button"
              className={styles.toggleBtn}
              data-active={method === "spearman"}
              onClick={() => setMethod("spearman")}
            >
              Spearman
            </button>
            <button
              type="button"
              className={styles.toggleBtn}
              data-active={method === "sparcc"}
              disabled={!availableMethods.sparcc}
              data-tooltip={methodNote || undefined}
              onClick={() => setMethod("sparcc")}
            >
              SparCC
            </button>
          </div>
        </div>

        <div className={styles.field}>
          <label>{locale === "zh" ? "节点着色" : "Node color"}</label>
          <div className={styles.btnGroup}>
            <button
              type="button"
              className={styles.toggleBtn}
              data-active={colorMode === "phylum"}
              onClick={() => setColorMode("phylum")}
            >
              {locale === "zh" ? "按门" : "Phylum"}
            </button>
            <button
              type="button"
              className={styles.toggleBtn}
              data-active={colorMode === "community"}
              onClick={() => setColorMode("community")}
            >
              {locale === "zh" ? "按群落" : "Community"}
            </button>
          </div>
        </div>

        <div className={styles.field}>
          <label>{locale === "zh" ? "导出" : "Export"}</label>
          <div className={styles.btnGroup}>
            <button type="button" className={styles.actionBtn} onClick={exportEdges}>{locale === "zh" ? "CSV" : "CSV"}</button>
            <button type="button" className={styles.actionBtn} onClick={() => svgRef.current && exportSVG(svgRef.current, `network_${Date.now()}`)}>SVG</button>
            <button type="button" className={styles.actionBtn} onClick={() => svgRef.current && exportPNG(svgRef.current, `network_${Date.now()}`)}>PNG</button>
          </div>
        </div>
      </div>

      {methodNote ? <p className={styles.note}>{methodNote}</p> : null}
      {error ? <div className={styles.error}>{error}</div> : null}
      {loading ? <div className={styles.loading}>{locale === "zh" ? "正在计算共现网络..." : "Computing co-occurrence network..."}</div> : null}

      {data ? (
        <>
          <div className={styles.statsGrid}>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.n_genera}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "节点数" : "Nodes"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.n_edges}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "边数" : "Edges"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.network_density.toFixed(3)}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "网络密度" : "Density"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.n_communities}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "群落数" : "Modules"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.hub_nodes.length}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "枢纽节点" : "Hubs"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{data.positive_edge_count}/{data.negative_edge_count}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "正/负相关边" : "Positive / negative"}</div>
            </div>
          </div>

          <div className={styles.contentGrid}>
            <div className={styles.graphCard}>
              <div className={styles.graphHead}>
                <div>
                  <h3 className={styles.cardTitle}>
                    {locale === "zh" ? "共现网络" : "Co-occurrence network"}
                  </h3>
                  <p className={styles.cardSubtle}>
                    {locale === "zh"
                      ? `${data.disease}，${data.n_samples} 个样本，FDR ≤ ${data.fdr_threshold}`
                      : `${data.disease}, ${data.n_samples} samples, FDR ≤ ${data.fdr_threshold}`}
                  </p>
                </div>
              </div>

              <div className={styles.graphContainer}>
                <svg ref={svgRef} />
              </div>

              <div className={styles.legend}>
                <div className={styles.legendItem}>
                  <span className={styles.legendDot} style={{ background: "#4ecdc4" }} />
                  <span>{locale === "zh" ? "正相关边" : "Positive edge"}</span>
                </div>
                <div className={styles.legendItem}>
                  <span className={styles.legendDot} style={{ background: "#ff6b6b" }} />
                  <span>{locale === "zh" ? "负相关边" : "Negative edge"}</span>
                </div>
                <div className={styles.legendItem}>
                  <span className={styles.hubRing} />
                  <span>{locale === "zh" ? "枢纽节点" : "Hub node"}</span>
                </div>
              </div>
            </div>

            <NetworkTopologyTable nodes={data.nodes} />
          </div>

          <NetworkEdgeTable edges={data.edges} />
        </>
      ) : null}
    </div>
  );
};

export default CooccurrencePanel;
