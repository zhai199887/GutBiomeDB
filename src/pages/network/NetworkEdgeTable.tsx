import { useMemo, useState } from "react";

import { useI18n } from "@/i18n";

import styles from "./NetworkPanel.module.css";
import type { CoEdge } from "./types";

const nodeId = (value: CoEdge["source"] | CoEdge["target"]) => (
  typeof value === "string" ? value : value.id
);

const NetworkEdgeTable = ({ edges }: { edges: CoEdge[] }) => {
  const { locale } = useI18n();
  const [query, setQuery] = useState("");
  const [edgeType, setEdgeType] = useState<"all" | "positive" | "negative">("all");

  const rows = useMemo(() => {
    const lowered = query.trim().toLowerCase();
    return edges
      .filter((edge) => edgeType === "all" || edge.type === edgeType)
      .filter((edge) => {
        if (!lowered) return true;
        return nodeId(edge.source).toLowerCase().includes(lowered) || nodeId(edge.target).toLowerCase().includes(lowered);
      })
      .sort((a, b) => Math.abs(b.r) - Math.abs(a.r))
      .slice(0, 50);
  }, [edgeType, edges, query]);

  return (
    <div className={styles.tableCard}>
      <div className={styles.tableHead}>
        <div>
          <h3 className={styles.cardTitle}>{locale === "zh" ? "边列表" : "Edge table"}</h3>
          <p className={styles.cardSubtle}>
            {locale === "zh" ? "按 |r| 排序，支持菌属检索" : "Sorted by |r| with quick taxon filtering"}
          </p>
        </div>
      </div>

      <div className={styles.toolbar}>
        <div className={styles.field}>
          <label>{locale === "zh" ? "搜索菌属" : "Search taxon"}</label>
          <input
            className={styles.input}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={locale === "zh" ? "输入属名..." : "Enter a genus..."}
          />
        </div>
        <div className={styles.field}>
          <label>{locale === "zh" ? "边类型" : "Edge type"}</label>
          <div className={styles.btnGroup}>
            {[
              { value: "all", label: locale === "zh" ? "全部" : "All" },
              { value: "positive", label: locale === "zh" ? "正相关" : "Positive" },
              { value: "negative", label: locale === "zh" ? "负相关" : "Negative" },
            ].map((item) => (
              <button
                key={item.value}
                type="button"
                className={styles.toggleBtn}
                data-active={edgeType === item.value}
                onClick={() => setEdgeType(item.value as typeof edgeType)}
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {!rows.length ? (
        <div className={styles.empty}>{locale === "zh" ? "当前过滤条件下没有边" : "No edges match the current filter"}</div>
      ) : (
        <div className={styles.tableWrapper}>
          <table className={styles.dataTable}>
            <thead>
              <tr>
                <th>{locale === "zh" ? "菌属 A" : "Genus A"}</th>
                <th>{locale === "zh" ? "菌属 B" : "Genus B"}</th>
                <th>r</th>
                <th>{locale === "zh" ? "校正 p 值" : "Adj. p"}</th>
                <th>{locale === "zh" ? "类型" : "Type"}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((edge) => (
                <tr key={`${nodeId(edge.source)}-${nodeId(edge.target)}`}>
                  <td><span className={styles.taxonName}>{nodeId(edge.source)}</span></td>
                  <td><span className={styles.taxonName}>{nodeId(edge.target)}</span></td>
                  <td>{edge.r.toFixed(3)}</td>
                  <td>{edge.adjusted_p.toFixed(4)}</td>
                  <td>
                    <span className={edge.type === "positive" ? styles.badgePositive : styles.badgeNegative}>
                      {edge.type === "positive"
                        ? (locale === "zh" ? "正相关" : "Positive")
                        : (locale === "zh" ? "负相关" : "Negative")}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default NetworkEdgeTable;
