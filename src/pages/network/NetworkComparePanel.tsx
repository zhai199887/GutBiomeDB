import { useEffect, useMemo, useRef, useState } from "react";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { diseaseDisplayNameI18n, sortDiseaseItemsByName } from "@/util/diseaseNames";

import styles from "./NetworkPanel.module.css";
import { cooccurrenceEdgeKey, drawCooccurrenceGraph } from "./graphUtils";
import type { DiseaseItem, NetworkCompareData, NetworkMethod } from "./types";
import { API_BASE } from "./types";

const NetworkComparePanel = () => {
  const { locale } = useI18n();
  const diseaseSvgRef = useRef<SVGSVGElement>(null);
  const controlSvgRef = useRef<SVGSVGElement>(null);

  const [diseases, setDiseases] = useState<DiseaseItem[]>([]);
  const [disease, setDisease] = useState("");
  const [minR, setMinR] = useState(0.3);
  const [method, setMethod] = useState<NetworkMethod>("spearman");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<NetworkCompareData | null>(null);

  useEffect(() => {
    cachedFetch<{ diseases: DiseaseItem[] }>(`${API_BASE}/api/disease-list`)
      .then((payload) => setDiseases(payload.diseases ?? []))
      .catch(() => {});
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  const runComparison = async () => {
    if (!disease.trim()) {
      setError(locale === "zh" ? "请先选择疾病。" : "Please select a disease first.");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({
        disease,
        min_r: String(minR),
        top_genera: "45",
        method,
        fdr_threshold: "0.05",
      });
      setResult(await cachedFetch<NetworkCompareData>(`${API_BASE}/api/network-compare?${params}`));
    } catch (unknownError) {
      setError(unknownError instanceof Error ? unknownError.message : String(unknownError));
    } finally {
      setLoading(false);
    }
  };

  const gainedKeys = useMemo(() => new Set(
    (result?.rewired_edges ?? [])
      .filter((edge) => edge.present_in === "disease_only")
      .map((edge) => cooccurrenceEdgeKey({ source: edge.source, target: edge.target })),
  ), [result?.rewired_edges]);

  const lostKeys = useMemo(() => new Set(
    (result?.rewired_edges ?? [])
      .filter((edge) => edge.present_in === "control_only")
      .map((edge) => cooccurrenceEdgeKey({ source: edge.source, target: edge.target })),
  ), [result?.rewired_edges]);

  useEffect(() => {
    if (!result || !diseaseSvgRef.current) return undefined;
    return drawCooccurrenceGraph(diseaseSvgRef.current, result.disease_network, {
      locale,
      colorMode: "community",
      highlightEdgeKeys: gainedKeys,
      highlightStroke: "#22c55e",
      height: 540,
    });
  }, [gainedKeys, locale, result]);

  useEffect(() => {
    if (!result || !controlSvgRef.current) return undefined;
    return drawCooccurrenceGraph(controlSvgRef.current, result.control_network, {
      locale,
      colorMode: "community",
      highlightEdgeKeys: lostKeys,
      highlightStroke: "#ef4444",
      highlightDasharray: "7 5",
      height: 540,
    });
  }, [locale, lostKeys, result]);

  const availableMethods = result?.disease_network.available_methods ?? { spearman: true, sparcc: true };
  const rewiredPreview = result?.rewired_edges.slice(0, 12) ?? [];
  const switchedPreview = result?.sign_switched_edges.slice(0, 8) ?? [];

  return (
    <div className={styles.workspace}>
      <div className={styles.toolbar}>
        <div className={styles.field}>
          <label>{locale === "zh" ? "疾病" : "Disease"}</label>
          <input
            list="network-compare-disease-list"
            className={styles.input}
            value={disease}
            onChange={(event) => setDisease(event.target.value)}
            placeholder={locale === "zh" ? "输入疾病名..." : "Enter a disease..."}
          />
          <datalist id="network-compare-disease-list">
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
                onClick={() => setMethod("sparcc")}
              >
                SparCC
              </button>
          </div>
        </div>

        <div className={styles.field}>
          <label>{locale === "zh" ? "执行" : "Run"}</label>
          <button type="button" className={styles.actionBtnPrimary} onClick={runComparison} disabled={loading}>
            {loading ? (locale === "zh" ? "计算中..." : "Running...") : (locale === "zh" ? "比较网络" : "Compare")}
          </button>
        </div>
      </div>

      {error ? <div className={styles.error}>{error}</div> : null}

      {result ? (
        <>
          <div className={styles.compareSummary}>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{result.gained_edges}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "疾病中新出现的边" : "Edges gained in disease"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{result.lost_edges}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "健康中存在但疾病消失的边" : "Edges lost from controls"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>{result.sign_switched_edges.length}</div>
              <div className={styles.statLabel}>{locale === "zh" ? "方向翻转的边" : "Sign-switched edges"}</div>
            </div>
            <div className={styles.statCard}>
              <div className={styles.statValue}>
                {result.disease_network.n_edges}/{result.control_network.n_edges}
              </div>
              <div className={styles.statLabel}>{locale === "zh" ? "疾病/健康边数" : "Disease / control edges"}</div>
            </div>
          </div>

          <div className={styles.compareGrid}>
            <div className={styles.graphCard}>
              <div className={styles.graphHead}>
                <div>
                  <h3 className={styles.cardTitle}>{locale === "zh" ? "疾病网络" : "Disease network"}</h3>
                  <p className={styles.cardSubtle}>
                    {diseaseDisplayNameI18n(result.disease, locale)} · n={result.disease_network.n_samples}
                  </p>
                </div>
              </div>
              <div className={styles.graphContainer}>
                <svg ref={diseaseSvgRef} />
              </div>
            </div>

            <div className={styles.graphCard}>
              <div className={styles.graphHead}>
                <div>
                  <h3 className={styles.cardTitle}>{locale === "zh" ? "健康对照网络" : "Healthy-control network"}</h3>
                  <p className={styles.cardSubtle}>
                    {locale === "zh" ? `NC · n=${result.control_network.n_samples}` : `NC · n=${result.control_network.n_samples}`}
                  </p>
                </div>
              </div>
              <div className={styles.graphContainer}>
                <svg ref={controlSvgRef} />
              </div>
            </div>
          </div>

          <div className={styles.compareGrid}>
            <div className={styles.tableCard}>
              <div className={styles.tableHead}>
                <div>
                  <h3 className={styles.cardTitle}>{locale === "zh" ? "重塑边 Top 12" : "Top rewired edges"}</h3>
                  <p className={styles.cardSubtle}>
                    {locale === "zh" ? "绿色表示疾病特异，红色表示健康特异" : "Green indicates disease-only edges; red indicates control-only edges"}
                  </p>
                </div>
              </div>
              {!rewiredPreview.length ? (
                <div className={styles.empty}>{locale === "zh" ? "没有检测到明显的边重塑。" : "No prominent rewired edges detected."}</div>
              ) : (
                <div className={styles.compareList}>
                  {rewiredPreview.map((edge) => (
                    <div key={`${edge.source}-${edge.target}-${edge.present_in}`} className={styles.compareItem}>
                      <div>
                        <span className={styles.taxonName}>{edge.source}</span>
                        <span className={styles.compareMeta}> ↔ </span>
                        <span className={styles.taxonName}>{edge.target}</span>
                      </div>
                      <div className={styles.compareMeta}>
                        {edge.present_in === "disease_only"
                          ? (locale === "zh" ? "疾病特异" : "Disease only")
                          : (locale === "zh" ? "健康特异" : "Control only")} · r={edge.r.toFixed(3)}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className={styles.tableCard}>
              <div className={styles.tableHead}>
                <div>
                  <h3 className={styles.cardTitle}>{locale === "zh" ? "方向翻转边" : "Sign-switched edges"}</h3>
                  <p className={styles.cardSubtle}>
                    {locale === "zh" ? "同一对菌在疾病和健康中的正负相关方向变化" : "Edges that switch sign between disease and controls"}
                  </p>
                </div>
              </div>
              {!switchedPreview.length ? (
                <div className={styles.empty}>{locale === "zh" ? "没有方向翻转边。" : "No sign-switched edges."}</div>
              ) : (
                <div className={styles.compareList}>
                  {switchedPreview.map((edge) => (
                    <div key={`${edge.source}-${edge.target}`} className={styles.compareItem}>
                      <div>
                        <span className={styles.taxonName}>{edge.source}</span>
                        <span className={styles.compareMeta}> ↔ </span>
                        <span className={styles.taxonName}>{edge.target}</span>
                      </div>
                      <div className={styles.compareMeta}>
                        {locale === "zh"
                          ? `疾病 ${edge.disease_type} / 健康 ${edge.control_type}`
                          : `Disease ${edge.disease_type} / Control ${edge.control_type}`}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </>
      ) : null}
    </div>
  );
};

export default NetworkComparePanel;
