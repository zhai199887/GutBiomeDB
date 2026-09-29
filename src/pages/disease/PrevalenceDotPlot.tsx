import { useMemo } from "react";
import { phylumColor } from "@/util/phylumColors";
import type { GenusEntry } from "./types";
import classes from "../DiseasePage.module.css";

interface Props {
  data: GenusEntry[];
  locale: string;
}

const formatP = (value: number) => {
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
};

const PrevalenceDotPlot = ({ data, locale }: Props) => {
  const points = useMemo(
    () =>
      data
        .filter((item) => item.disease_mean > 0)
        .sort((a, b) => b.disease_mean - a.disease_mean),
    [data],
  );

  if (points.length === 0) {
    return <div className={classes.emptyPlot}>{locale === "zh" ? "暂无可绘制数据" : "No points available"}</div>;
  }

  const width = 760;
  const height = 420;
  const margin = { top: 16, right: 24, bottom: 60, left: 72 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;

  const minY = Math.min(...points.map((item) => item.disease_mean));
  const maxY = Math.max(...points.map((item) => item.disease_mean));
  const minLog = Math.log10(Math.max(minY, 1e-6));
  const maxLog = Math.log10(Math.max(maxY, 1e-6));

  const x = (value: number) => margin.left + (value / 100) * innerWidth;
  const y = (value: number) => {
    const scaled = Math.log10(Math.max(value, 1e-6));
    const ratio = maxLog === minLog ? 0.5 : (scaled - minLog) / (maxLog - minLog);
    return margin.top + innerHeight - ratio * innerHeight;
  };

  const tickValues = [0, 20, 40, 60, 80, 100];
  const yTicks = [minY, Math.sqrt(minY * maxY), maxY]
    .filter((value, index, arr) => Number.isFinite(value) && arr.findIndex((other) => Math.abs(other - value) < 1e-9) === index)
    .sort((a, b) => a - b);

  return (
    <div className={classes.dotPlotWrap}>
      <svg viewBox={`0 0 ${width} ${height}`} className={classes.chart}>
        <rect x="0" y="0" width={width} height={height} fill="transparent" />

        {tickValues.map((tick) => (
          <g key={tick}>
            <line
              x1={x(tick)}
              x2={x(tick)}
              y1={margin.top}
              y2={margin.top + innerHeight}
              className={classes.gridLine}
            />
            <text x={x(tick)} y={height - 22} textAnchor="middle" className={classes.axisLabel}>
              {tick}%
            </text>
          </g>
        ))}

        {yTicks.map((tick) => (
          <g key={tick}>
            <line
              x1={margin.left}
              x2={margin.left + innerWidth}
              y1={y(tick)}
              y2={y(tick)}
              className={classes.gridLine}
            />
            <text x={margin.left - 10} y={y(tick) + 4} textAnchor="end" className={classes.axisLabel}>
              {tick.toFixed(tick < 0.1 ? 3 : 2)}%
            </text>
          </g>
        ))}

        {points.map((item) => {
          const href = `/species/${encodeURIComponent(item.genus)}`;
          const stroke = item.enriched_in === "control" ? phylumColor(item.phylum) : "transparent";
          const fill = item.enriched_in === "control" ? "transparent" : phylumColor(item.phylum);
          const strokeWidth = item.enriched_in === "control" ? 1.8 : 0;
          const tooltip = [
            `<strong><i>${item.genus}</i></strong>`,
            `${locale === "zh" ? "门" : "Phylum"}: ${item.phylum}`,
            `${locale === "zh" ? "疾病流行率" : "Disease prevalence"}: ${(item.disease_prevalence * 100).toFixed(1)}%`,
            `${locale === "zh" ? "疾病均值" : "Disease mean"}: ${item.disease_mean.toFixed(3)}%`,
            `log2FC: ${item.log2fc.toFixed(3)}`,
            `adj.p: ${formatP(item.adjusted_p)}`,
          ].join("<br/>");
          return (
            <a key={item.genus} href={href}>
              <circle
                cx={x(item.disease_prevalence * 100)}
                cy={y(item.disease_mean)}
                r={item.adjusted_p < 0.01 ? 6 : 4.5}
                fill={fill}
                stroke={stroke}
                strokeWidth={strokeWidth}
                data-tooltip={tooltip}
                className={classes.plotPoint}
              />
            </a>
          );
        })}

        {points
          .filter((item) => item.adjusted_p < 0.01)
          .slice(0, 10)
          .map((item) => (
            <text
              key={`${item.genus}-label`}
              x={x(item.disease_prevalence * 100) + 8}
              y={y(item.disease_mean) - 8}
              className={classes.dotPlotLabel}
            >
              {item.genus}
            </text>
          ))}

        <text x={margin.left + innerWidth / 2} y={height - 4} textAnchor="middle" className={classes.axisTitle}>
          {locale === "zh" ? "疾病组流行率" : "Disease prevalence"}
        </text>
        <text
          x={18}
          y={margin.top + innerHeight / 2}
          transform={`rotate(-90 18 ${margin.top + innerHeight / 2})`}
          textAnchor="middle"
          className={classes.axisTitle}
        >
          {locale === "zh" ? "疾病组平均相对丰度（对数）" : "Disease mean relative abundance (log scale)"}
        </text>
      </svg>

      <div className={classes.dotPlotLegend}>
        <span>{locale === "zh" ? "实心圆：疾病富集" : "Filled: enriched in disease"}</span>
        <span>{locale === "zh" ? "空心圆：对照富集" : "Hollow: enriched in control"}</span>
        <span>{locale === "zh" ? "标注：adj.p < 0.01" : "Labels: adj.p < 0.01"}</span>
      </div>
    </div>
  );
};

export default PrevalenceDotPlot;
