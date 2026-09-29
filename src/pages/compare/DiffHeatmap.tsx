import { useMemo } from "react";

import { useI18n } from "@/i18n";

import classes from "../ComparePage.module.css";
import type { DiffResult } from "./types";

const colorForValue = (value: number) => {
  const clamped = Math.max(-2, Math.min(2, value));
  if (clamped >= 0) return `rgba(34, 197, 94, ${0.18 + clamped / 2 * 0.62})`;
  return `rgba(59, 130, 246, ${0.18 + Math.abs(clamped) / 2 * 0.62})`;
};

const zScore = (values: number[]) => {
  const mean = values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(values.length, 1);
  const std = Math.sqrt(variance) || 1;
  return values.map((value) => (value - mean) / std);
};

const DiffHeatmap = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const top = result.diff_taxa.slice(0, 24);
  const lvl = (result.summary?.taxonomy_level || "genus").toLowerCase();
  const unitEn = lvl === "phylum" ? "phyla" : lvl === "family" ? "families" : "genera";
  const unitZh = lvl === "phylum" ? "差异门" : lvl === "family" ? "差异科" : "差异属";

  const matrix = useMemo(() => {
    const meanA = zScore(top.map((row) => row.mean_a));
    const meanB = zScore(top.map((row) => row.mean_b));
    const effect = zScore(top.map((row) => row.log2fc));
    return top.map((row, index) => ({
      taxon: row.taxon,
      phylum: row.phylum,
      values: [meanA[index], meanB[index], effect[index]],
    }));
  }, [top]);

  const columns = [
    locale === "zh" ? "A 组丰度" : "Mean A",
    locale === "zh" ? "B 组丰度" : "Mean B",
    "log2FC",
  ];

  const width = 1140;
  const leftLabelX = 320;
  const cellStartX = 380;
  const cellWidth = 200;
  const rowHeight = 30;
  const height = 176 + matrix.length * rowHeight;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className={`compare-chart ${classes.chart}`}>
      <defs>
        <linearGradient id="diff-heatmap-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="rgba(59, 130, 246, 0.8)" />
          <stop offset="50%" stopColor="rgba(148, 163, 184, 0.16)" />
          <stop offset="100%" stopColor="rgba(34, 197, 94, 0.8)" />
        </linearGradient>
      </defs>
      <text x={width / 2} y={22} textAnchor="middle" fill="currentColor" fontSize="14">
        {locale === "zh" ? "差异热图" : "Differential Heatmap"}
      </text>
      <text x={width / 2} y={44} textAnchor="middle" fill="var(--light-gray)" fontSize="11">
        {locale === "zh" ? `对 Top 24 ${unitZh}做列内标准化` : `Column-wise standardized values for the top 24 differential ${unitEn}`}
      </text>
      <text x={760} y={28} fill="var(--light-gray)" fontSize="10">
        {locale === "zh" ? "标准化值" : "Standardized value"}
      </text>
      <rect x={760} y={34} width={170} height={10} rx={999} fill="url(#diff-heatmap-gradient)" />
      <text x={760} y={60} fill="var(--light-gray)" fontSize="9">
        {locale === "zh" ? "较低" : "Lower"}
      </text>
      <text x={845} y={60} textAnchor="middle" fill="var(--light-gray)" fontSize="9">
        0
      </text>
      <text x={930} y={60} textAnchor="end" fill="var(--light-gray)" fontSize="9">
        {locale === "zh" ? "较高" : "Higher"}
      </text>

      <text x={leftLabelX} y={88} textAnchor="end" fill="var(--light-gray)" fontSize="11">
        {locale === "zh" ? "属" : "Genus"}
      </text>
      {columns.map((column, index) => (
        <text key={column} x={cellStartX + index * cellWidth + cellWidth / 2} y={88} textAnchor="middle" fill="var(--light-gray)" fontSize="11">
          {column}
        </text>
      ))}

      {matrix.map((row, rowIndex) => (
        <g key={row.taxon}>
          <text x={leftLabelX} y={118 + rowIndex * rowHeight} textAnchor="end" fill="currentColor" fontSize="11">
            {row.taxon.length > 34 ? `${row.taxon.slice(0, 32)}…` : row.taxon}
          </text>
          <text x={leftLabelX} y={132 + rowIndex * rowHeight} textAnchor="end" fill="var(--light-gray)" fontSize="9">
            {row.phylum}
          </text>
          {row.values.map((value, colIndex) => (
            <g key={`${row.taxon}-${colIndex}`}>
              <rect
                x={cellStartX + colIndex * cellWidth}
                y={100 + rowIndex * rowHeight}
                width={cellWidth - 10}
                height={24}
                rx={6}
                fill={colorForValue(value)}
              />
              <text
                x={cellStartX + colIndex * cellWidth + (cellWidth - 10) / 2}
                y={116 + rowIndex * rowHeight}
                textAnchor="middle"
                fill="currentColor"
                fontSize="11"
              >
                {value.toFixed(1)}
              </text>
            </g>
          ))}
        </g>
      ))}
    </svg>
  );
};

export default DiffHeatmap;
