import { useMemo } from "react";

import { useI18n } from "@/i18n";
import { phylumColor } from "@/util/phylumColors";

import classes from "../ComparePage.module.css";
import type { DiffResult } from "./types";

const StackedBarChart = ({ result }: { result: DiffResult }) => {
  const { locale } = useI18n();
  const rows = result.phylum_composition.rows;

  const totals = useMemo(() => ({
    a: rows.reduce((sum, row) => sum + row.group_a, 0),
    b: rows.reduce((sum, row) => sum + row.group_b, 0),
  }), [rows]);

  let offsetA = 0;
  let offsetB = 0;

  return (
    <div className={classes.compositionWrap}>
      <div className={classes.compositionHeader}>
        <h3>{locale === "zh" ? "门水平组成比较" : "Phylum Composition"}</h3>
        <p>{locale === "zh" ? "显示两组样本的平均相对丰度组成" : "Mean relative abundance composition for the two groups"}</p>
      </div>

      <svg viewBox="0 0 860 240" className={`compare-chart ${classes.chart}`}>
        {[result.summary.group_a_name, result.summary.group_b_name].map((label, index) => {
          const y = index === 0 ? 70 : 150;
          const total = index === 0 ? totals.a || 1 : totals.b || 1;

          return (
            <g key={label}>
              <text x={30} y={y + 8} fill="currentColor" fontSize="12">
                {label}
              </text>
              {rows.map((row) => {
                const value = index === 0 ? row.group_a : row.group_b;
                const width = (value / total) * 620;
                const x = 180 + (index === 0 ? offsetA : offsetB);
                if (index === 0) offsetA += width;
                if (index === 1) offsetB += width;
                return (
                  <g key={`${label}-${row.phylum}`}>
                    <rect
                      x={x}
                      y={y - 14}
                      width={Math.max(width, 1)}
                      height={28}
                      fill={phylumColor(row.phylum)}
                      rx={4}
                    />
                    {width > 44 ? (
                      <text
                        x={x + width / 2}
                        y={y + 4}
                        fill="#0b0f14"
                        fontSize="10"
                        textAnchor="middle"
                      >
                        {row.phylum}
                      </text>
                    ) : null}
                  </g>
                );
              })}
            </g>
          );
        })}
      </svg>

      <div className={classes.legendGrid}>
        {rows.map((row) => (
          <div key={row.phylum} className={classes.legendItem}>
            <span className={classes.legendSwatch} style={{ background: phylumColor(row.phylum) }} />
            <span>{row.phylum}</span>
            <span className={classes.legendValue}>
              A {row.group_a.toFixed(1)}% / B {row.group_b.toFixed(1)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default StackedBarChart;
