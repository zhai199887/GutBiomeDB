import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { SpearmanResult } from "./types";

const correlationColor = (value: number) => {
  const clamped = Math.max(-1, Math.min(1, value));
  if (clamped >= 0) return `rgba(34, 197, 94, ${0.15 + clamped * 0.75})`;
  return `rgba(239, 68, 68, ${0.15 + Math.abs(clamped) * 0.75})`;
};

const SpearmanChart = ({ result }: { result: SpearmanResult | null }) => {
  const { locale } = useI18n();

  if (!result || !result.taxa.length) {
    return (
      <div className={classes.emptyPanel}>
        {locale === "zh" ? "暂无可用的 Spearman 相关性结果" : "No Spearman correlation result available"}
      </div>
    );
  }

  const edges = result.edges.slice(0, 8);
  const cellSize = 34;
  const matrixX = 280;
  const matrixY = 150;
  const matrixBottom = matrixY + result.taxa.length * cellSize;
  const edgeBlockY = matrixBottom + 28;
  const width = Math.max(920, matrixX + result.taxa.length * cellSize + 40);
  const height = edgeBlockY + 28 + edges.length * 18 + 20;
  const shortTaxon = (taxon: string, maxLength: number) =>
    taxon.length > maxLength ? `${taxon.slice(0, maxLength - 1)}…` : taxon;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className={`compare-chart ${classes.chart}`}>
      <text x={width / 2} y={22} textAnchor="middle" fill="currentColor" fontSize="14">
        {locale === "zh" ? "Spearman 相关结构" : "Spearman Correlation Structure"}
      </text>
      <text x={width / 2} y={42} textAnchor="middle" fill="var(--light-gray)" fontSize="11">
        {locale === "zh"
          ? `基于 ${result.summary.sample_count} 个匹配样本计算`
          : `Computed from ${result.summary.sample_count} matched samples`}
      </text>

      {result.taxa.map((taxon, index) => (
        <text
          key={`head-${taxon.taxon}`}
          x={matrixX + index * cellSize + cellSize / 2}
          y={104}
          transform={`rotate(-45, ${matrixX + index * cellSize + cellSize / 2}, 104)`}
          fill="var(--light-gray)"
          fontSize="9"
          textAnchor="end"
        >
          {shortTaxon(taxon.taxon, 11)}
        </text>
      ))}

      {result.matrix.map((row, rowIndex) => (
        <g key={result.taxa[rowIndex]?.taxon ?? rowIndex}>
          <text x={matrixX - 12} y={matrixY + rowIndex * cellSize + 21} textAnchor="end" fill="currentColor" fontSize="10">
            {shortTaxon(result.taxa[rowIndex]?.taxon ?? "", 24)}
          </text>
          {row.map((value, colIndex) => (
            <g key={`${rowIndex}-${colIndex}`}>
              <rect
                x={matrixX + colIndex * cellSize}
                y={matrixY + rowIndex * cellSize}
                width={cellSize - 2}
                height={cellSize - 2}
                rx={5}
                fill={correlationColor(value)}
              />
              <text
                x={matrixX + colIndex * cellSize + (cellSize - 2) / 2}
                y={matrixY + rowIndex * cellSize + 20}
                textAnchor="middle"
                fill="currentColor"
                fontSize="9"
              >
                {value.toFixed(1)}
              </text>
            </g>
          ))}
        </g>
      ))}

      <g transform={`translate(30,${edgeBlockY})`}>
        <text x={0} y={0} fill="currentColor" fontSize="12">
          {locale === "zh" ? "最强相关边" : "Strongest edges"}
        </text>
        {edges.map((edge, index) => (
          <g key={`${edge.source}-${edge.target}`}>
            <circle cx={6} cy={24 + index * 18} r={4} fill={edge.type === "positive" ? "#22c55e" : "#ef4444"} />
            <text x={18} y={28 + index * 18} fill="var(--light-gray)" fontSize="10">
              {shortTaxon(edge.source, 16)} {edge.type === "positive" ? "+" : "-"} {shortTaxon(edge.target, 16)} (r={edge.r.toFixed(2)})
            </text>
          </g>
        ))}
      </g>
    </svg>
  );
};

export default SpearmanChart;
