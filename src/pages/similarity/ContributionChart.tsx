interface ContributionEntry {
  genus: string;
  abundance: number;
  weight: number;
  contribution: number;
}

interface ContributionChartProps {
  title: string;
  positiveLabel: string;
  negativeLabel: string;
  health: ContributionEntry[];
  disease: ContributionEntry[];
}

const formatPct = (value: number) => `${(value * 100).toFixed(1)}%`;

const ContributionChart = ({
  title,
  positiveLabel,
  negativeLabel,
  health,
  disease,
}: ContributionChartProps) => {
  const left = health.slice(0, 8);
  const right = disease.slice(0, 8);
  const maxContribution = Math.max(
    ...left.map((item) => item.contribution),
    ...right.map((item) => item.contribution),
    0.01,
  );

  return (
    <div
      style={{
        marginTop: "1.5rem",
        padding: "1rem 1.1rem",
        borderRadius: 14,
        border: "1px solid rgba(255,255,255,0.08)",
        background: "rgba(255,255,255,0.03)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "baseline", marginBottom: "1rem", flexWrap: "wrap" }}>
        <h3 style={{ margin: 0, fontSize: "1rem" }}>{title}</h3>
        <div style={{ display: "flex", gap: 16, fontSize: "0.78rem", color: "var(--light-gray)", flexWrap: "wrap" }}>
          <span style={{ color: "#44cc88" }}>{positiveLabel}</span>
          <span style={{ color: "#ff7a7a" }}>{negativeLabel}</span>
        </div>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)",
          gap: "1rem",
        }}
      >
        <div>
          {left.map((item) => (
            <div key={`health:${item.genus}`} style={{ marginBottom: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 4 }}>
                <span style={{ fontStyle: "italic", fontSize: "0.8rem", color: "var(--white)" }}>{item.genus}</span>
                <span style={{ fontSize: "0.76rem", color: "var(--light-gray)" }}>{formatPct(item.contribution)}</span>
              </div>
              <div style={{ height: 10, borderRadius: 999, background: "rgba(255,255,255,0.06)", overflow: "hidden" }}>
                <div
                  style={{
                    width: `${(item.contribution / maxContribution) * 100}%`,
                    height: "100%",
                    borderRadius: 999,
                    background: "linear-gradient(90deg, rgba(68,204,136,0.45), rgba(68,204,136,0.95))",
                  }}
                  title={`${item.genus}: abundance=${item.abundance.toFixed(4)}, weight=${item.weight.toFixed(2)}, contribution=${formatPct(item.contribution)}`}
                />
              </div>
            </div>
          ))}
        </div>

        <div>
          {right.map((item) => (
            <div key={`disease:${item.genus}`} style={{ marginBottom: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 4 }}>
                <span style={{ fontStyle: "italic", fontSize: "0.8rem", color: "var(--white)" }}>{item.genus}</span>
                <span style={{ fontSize: "0.76rem", color: "var(--light-gray)" }}>{formatPct(item.contribution)}</span>
              </div>
              <div style={{ height: 10, borderRadius: 999, background: "rgba(255,255,255,0.06)", overflow: "hidden" }}>
                <div
                  style={{
                    width: `${(item.contribution / maxContribution) * 100}%`,
                    height: "100%",
                    borderRadius: 999,
                    background: "linear-gradient(90deg, rgba(255,122,122,0.45), rgba(255,122,122,0.95))",
                  }}
                  title={`${item.genus}: abundance=${item.abundance.toFixed(4)}, weight=${item.weight.toFixed(2)}, contribution=${formatPct(item.contribution)}`}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default ContributionChart;
