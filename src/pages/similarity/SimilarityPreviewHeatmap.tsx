import { Fragment } from "react";

interface SimilarityPreviewHeatmapProps {
  title: string;
  taxa: string[];
  rows: string[];
  matrix: number[][];
}

function cellColor(value: number, maxValue: number): string {
  if (maxValue <= 0) return "rgba(255,255,255,0.08)";
  const ratio = Math.max(0, Math.min(1, value / maxValue));
  const alpha = 0.12 + ratio * 0.88;
  return `rgba(245, 124, 0, ${alpha.toFixed(3)})`;
}

const SimilarityPreviewHeatmap = ({ title, taxa, rows, matrix }: SimilarityPreviewHeatmapProps) => {
  if (!taxa.length || matrix.length < 2) return null;

  const flatValues = matrix.flat();
  const maxValue = flatValues.length ? Math.max(...flatValues) : 0;

  return (
    <div style={{
      marginBottom: "1.5rem",
      padding: "1rem",
      background: "rgba(255,255,255,0.03)",
      border: "1px solid rgba(255,255,255,0.08)",
      borderRadius: 12,
      overflowX: "auto",
    }}>
      <h4 style={{ margin: "0 0 0.75rem", fontSize: "0.95rem" }}>{title}</h4>
      <div style={{ display: "grid", gridTemplateColumns: `150px repeat(${taxa.length}, minmax(64px, 1fr))`, gap: 6, alignItems: "center", minWidth: Math.max(520, taxa.length * 76) }}>
        <div />
        {taxa.map((taxon) => (
          <div
            key={taxon}
            style={{
              fontSize: "0.72rem",
              color: "var(--gray)",
              textAlign: "center",
              fontStyle: "italic",
              lineHeight: 1.2,
            }}
            title={taxon}
          >
            {taxon}
          </div>
        ))}
        {rows.map((rowLabel, rowIndex) => (
          <Fragment key={rowLabel}>
            <div style={{
              fontSize: "0.78rem",
              color: rowIndex === 0 ? "var(--white)" : "var(--light-gray)",
              fontWeight: rowIndex === 0 ? 700 : 500,
              paddingRight: 8,
            }}>
              {rowLabel}
            </div>
            {taxa.map((taxon, colIndex) => {
              const value = matrix[rowIndex]?.[colIndex] ?? 0;
              return (
                <div
                  key={`${rowLabel}:${taxon}`}
                  title={`${rowLabel} · ${taxon}: ${value.toFixed(2)}%`}
                  style={{
                    height: 42,
                    borderRadius: 8,
                    border: "1px solid rgba(255,255,255,0.08)",
                    background: cellColor(value, maxValue),
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: "0.72rem",
                    color: "#fff",
                    fontWeight: 600,
                  }}
                >
                  {value >= 0.01 ? value.toFixed(1) : "0"}
                </div>
              );
            })}
          </Fragment>
        ))}
      </div>
    </div>
  );
};

export default SimilarityPreviewHeatmap;
