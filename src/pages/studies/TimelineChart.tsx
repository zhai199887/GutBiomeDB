import * as d3 from "d3";

import { useI18n } from "@/i18n";

import type { ProjectTimelinePoint } from "./types";

const WIDTH = 760;
const HEIGHT = 360;
const MARGIN = { top: 18, right: 32, bottom: 40, left: 56 };

const TimelineChart = ({ timeline }: { timeline: ProjectTimelinePoint[] }) => {
  const { locale } = useI18n();

  if (!timeline.length) {
    return (
      <div style={{ minHeight: 360, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--light-gray)" }}>
        {locale === "zh" ? "暂无时间轴数据" : "No timeline data"}
      </div>
    );
  }

  const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
  const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;

  const years = timeline.map((item) => item.year);
  const sampleMax = Math.max(...timeline.map((item) => item.n_samples), 1);
  const projectMax = Math.max(...timeline.map((item) => item.n_projects), 1);
  const x = d3.scalePoint<number>().domain(years).range([0, innerWidth]).padding(0.45);
  const sampleY = d3.scaleLinear().domain([0, sampleMax]).nice().range([innerHeight, 0]);
  const projectY = d3.scaleLinear().domain([0, projectMax]).nice().range([innerHeight, 0]);
  const area = d3
    .area<ProjectTimelinePoint>()
    .x((item) => x(item.year) ?? 0)
    .y0(innerHeight)
    .y1((item) => sampleY(item.n_samples))
    .curve(d3.curveMonotoneX);
  const line = d3
    .line<ProjectTimelinePoint>()
    .x((item) => x(item.year) ?? 0)
    .y((item) => projectY(item.n_projects))
    .curve(d3.curveMonotoneX);

  return (
    <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} style={{ width: "100%", height: "auto" }}>
      <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
        {sampleY.ticks(4).map((tick) => (
          <g key={`sample-${tick}`} transform={`translate(0,${sampleY(tick)})`}>
            <line x1={0} x2={innerWidth} stroke="#1f2937" strokeDasharray="3 4" />
            <text x={-10} y={4} textAnchor="end" fill="#22c55e" fontSize="10">
              {tick.toLocaleString("en-US")}
            </text>
          </g>
        ))}

        {projectY.ticks(4).map((tick) => (
          <g key={`proj-${tick}`} transform={`translate(${innerWidth},${projectY(tick)})`}>
            <text x={10} y={4} textAnchor="start" fill="#60a5fa" fontSize="10">
              {tick.toLocaleString("en-US")}
            </text>
          </g>
        ))}

        <path d={area(timeline) ?? ""} fill="rgba(34, 197, 94, 0.16)" stroke="#22c55e" strokeWidth={1.6} />
        <path d={line(timeline) ?? ""} fill="none" stroke="#60a5fa" strokeWidth={2.2} />

        {timeline.map((item) => {
          const cx = x(item.year) ?? 0;
          return (
            <g key={item.year}>
              <circle cx={cx} cy={projectY(item.n_projects)} r={4.2} fill="#60a5fa">
                <title>{`${item.year}\nProjects: ${item.n_projects.toLocaleString("en-US")}\nSamples: ${item.n_samples.toLocaleString("en-US")}`}</title>
              </circle>
              <text x={cx} y={innerHeight + 22} textAnchor="middle" fill="#94a3b8" fontSize="10">
                {item.year}
              </text>
            </g>
          );
        })}

        <text x={0} y={-2} fill="#22c55e" fontSize="11">
          {locale === "zh" ? "样本数" : "Samples"}
        </text>
        <text x={innerWidth} y={-2} textAnchor="end" fill="#60a5fa" fontSize="11">
          {locale === "zh" ? "项目数" : "Projects"}
        </text>
      </g>
    </svg>
  );
};

export default TimelineChart;
