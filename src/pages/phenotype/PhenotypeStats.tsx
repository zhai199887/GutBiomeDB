/**
 * PhenotypeStats — 4-card summary row above charts
 * 表型分析结果摘要卡片组（4格）
 */
import { useI18n } from "@/i18n";
import type { PhenotypeAssociationResponse } from "./types";

interface Props {
  result: PhenotypeAssociationResponse;
}

export default function PhenotypeStats({ result }: Props) {
  const { locale } = useI18n();

  const cards = [
    {
      label: locale === "zh" ? "组 A 样本量" : "Group A Samples",
      value: result.n_a.toLocaleString(),
      sub: result.group_a,
    },
    {
      label: locale === "zh" ? "组 B 样本量" : "Group B Samples",
      value: result.n_b.toLocaleString(),
      sub: result.group_b,
    },
    {
      label: locale === "zh" ? "检验分类群数" : "Taxa Tested",
      value: result.total_taxa.toLocaleString(),
      sub: result.tax_level,
    },
    {
      label: locale === "zh" ? "显著差异 (adj.p<0.05)" : "Significant (adj.p<0.05)",
      value: result.significant_count.toLocaleString(),
      sub: locale === "zh" ? "BH-FDR 校正" : "BH-FDR corrected",
    },
  ];

  return (
    <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap", marginBottom: "1.5rem" }}>
      {cards.map((c) => (
        <div key={c.label} style={{
          background: "var(--dark-gray)",
          border: "1px solid var(--gray)",
          borderRadius: "8px",
          padding: "0.8rem 1.2rem",
          minWidth: "130px",
          flex: "1 1 130px",
        }}>
          <div style={{ fontSize: "0.72rem", color: "var(--light-gray)", textTransform: "uppercase", marginBottom: "0.2rem" }}>
            {c.label}
          </div>
          <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--primary)" }}>
            {c.value}
          </div>
          <div style={{ fontSize: "0.75rem", color: "var(--light-gray)", marginTop: "0.1rem", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "160px" }}>
            {c.sub}
          </div>
        </div>
      ))}
    </div>
  );
}
