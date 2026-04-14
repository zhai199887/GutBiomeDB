import { Link } from "react-router-dom";

import { useI18n } from "@/i18n";

import styles from "./NetworkPanel.module.css";
import type { CoNode } from "./types";

const NetworkTopologyTable = ({ nodes }: { nodes: CoNode[] }) => {
  const { locale } = useI18n();

  if (!nodes.length) {
    return <div className={styles.empty}>{locale === "zh" ? "暂无拓扑结果" : "No topology result available"}</div>;
  }

  const topNodes = [...nodes]
    .sort((a, b) => {
      if (b.degree !== a.degree) return b.degree - a.degree;
      return b.betweenness - a.betweenness;
    })
    .slice(0, 10);

  return (
    <div className={styles.tableCard}>
      <div className={styles.tableHead}>
        <div>
          <h3 className={styles.cardTitle}>{locale === "zh" ? "枢纽属 Top 10" : "Top 10 hub genera"}</h3>
          <p className={styles.cardSubtle}>
            {locale === "zh" ? "按度和介数中心性排序" : "Ranked by degree then betweenness"}
          </p>
        </div>
      </div>
      <div className={styles.tableWrapper}>
        <table className={styles.dataTable}>
          <thead>
            <tr>
              <th>#</th>
              <th>{locale === "zh" ? "菌属" : "Genus"}</th>
              <th>{locale === "zh" ? "门" : "Phylum"}</th>
              <th>{locale === "zh" ? "度" : "Degree"}</th>
              <th>{locale === "zh" ? "介数" : "Betweenness"}</th>
              <th>{locale === "zh" ? "群落" : "Module"}</th>
              <th>{locale === "zh" ? "枢纽" : "Hub"}</th>
            </tr>
          </thead>
          <tbody>
            {topNodes.map((node, index) => (
              <tr key={node.id}>
                <td>{index + 1}</td>
                <td>
                  <Link className={styles.taxonName} to={`/species/${encodeURIComponent(node.id)}`}>
                    {node.id}
                  </Link>
                </td>
                <td>{node.phylum}</td>
                <td>{node.degree}</td>
                <td>{node.betweenness.toFixed(3)}</td>
                <td>{node.community + 1}</td>
                <td>{node.is_hub ? "★" : "–"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default NetworkTopologyTable;
