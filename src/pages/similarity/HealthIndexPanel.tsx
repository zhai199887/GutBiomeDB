/**
 * HealthIndexPanel.tsx
 * 肠道微生物组健康指数 (GMHI) — 仪表盘+偏离表
 */
import { useRef, useState, useEffect } from "react";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { exportTable } from "@/util/export";
import classes from "./HealthIndexPanel.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface HealthResult {
  score: number;
  raw_score: number;
  category: string;
  health_genera_matched: number;
  disease_genera_matched: number;
  health_genera_sum: number;
  disease_genera_sum: number;
  health_genera_detail: { genus: string; abundance: number }[];
  disease_genera_detail: { genus: string; abundance: number }[];
  per_genus_deviation: {
    genus: string;
    user_abundance: number;
    nc_mean: number;
    nc_median: number;
    status: string;
  }[];
  reference: {
    n_nc_samples: number;
    n_disease_samples: number;
    health_genera_total: number;
    disease_genera_total: number;
  };
}

function parseAbundanceText(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  const lines = text.trim().split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const lower = trimmed.toLowerCase();
    if (lower.startsWith("genus") || lower.startsWith("taxon") || lower.startsWith("name")) continue;
    const parts = trimmed.split(/[,\t]+/);
    if (parts.length >= 2) {
      const genus = parts[0].trim();
      const value = parseFloat(parts[1].trim());
      if (genus && !isNaN(value)) {
        result[genus] = value;
      }
    }
  }
  return result;
}

const HealthIndexPanel = () => {
  const { t } = useI18n();
  const [pasteText, setPasteText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<HealthResult | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const gaugeRef = useRef<SVGSVGElement>(null);

  const readFile = (f: File): Promise<string> =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result as string);
      reader.onerror = () => reject(new Error("Failed to read file"));
      reader.readAsText(f);
    });

  const calculate = async () => {
    setError("");
    setResult(null);

    let abundances: Record<string, number> = {};
    try {
      if (file) {
        const text = await readFile(file);
        abundances = parseAbundanceText(text);
      } else if (pasteText.trim()) {
        abundances = parseAbundanceText(pasteText);
      }
    } catch {
      setError("Failed to parse input");
      return;
    }

    if (Object.keys(abundances).length === 0) {
      setError("No valid genus-abundance pairs found");
      return;
    }

    setLoading(true);
    try {
      const resp = await fetch(`${API_BASE}/api/health-index`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ abundances }),
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.detail || `HTTP ${resp.status}`);
      }
      setResult(await resp.json());
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Calculation failed");
    } finally {
      setLoading(false);
    }
  };

  // Gauge chart
  useEffect(() => {
    if (!result || !gaugeRef.current) return;
    const svg = d3.select(gaugeRef.current);
    svg.selectAll("*").remove();

    const w = 280, h = 180;
    svg.attr("viewBox", `0 0 ${w} ${h}`);
    const cx = w / 2, cy = 150;
    const r = 110;
    const startAngle = -Math.PI * 0.75;
    const endAngle = Math.PI * 0.75;
    const scoreAngle = startAngle + (result.score / 100) * (endAngle - startAngle);

    // Background arc
    const bgArc = d3.arc<unknown>()
      .innerRadius(r - 18).outerRadius(r)
      .startAngle(startAngle).endAngle(endAngle);
    svg.append("path")
      .attr("d", bgArc({} as never) ?? "")
      .attr("transform", `translate(${cx},${cy})`)
      .attr("fill", "#2a2a3a");

    // Gradient arc segments
    const segments = [
      { end: 0.33, color: "#ff4444" },
      { end: 0.66, color: "#ffaa00" },
      { end: 1.0, color: "#44cc88" },
    ];
    let prevEnd = startAngle;
    for (const seg of segments) {
      const segEnd = startAngle + seg.end * (endAngle - startAngle);
      const clampedEnd = Math.min(segEnd, scoreAngle);
      if (clampedEnd <= prevEnd) break;
      const segArc = d3.arc<unknown>()
        .innerRadius(r - 18).outerRadius(r)
        .startAngle(prevEnd).endAngle(clampedEnd);
      svg.append("path")
        .attr("d", segArc({} as never) ?? "")
        .attr("transform", `translate(${cx},${cy})`)
        .attr("fill", seg.color).attr("opacity", 0.9);
      prevEnd = clampedEnd;
    }

    // Needle
    const needleLen = r - 25;
    const nx = cx + needleLen * Math.cos(scoreAngle - Math.PI / 2);
    const ny = cy + needleLen * Math.sin(scoreAngle - Math.PI / 2);
    svg.append("line")
      .attr("x1", cx).attr("y1", cy)
      .attr("x2", nx).attr("y2", ny)
      .attr("stroke", "#fff").attr("stroke-width", 2);
    svg.append("circle")
      .attr("cx", cx).attr("cy", cy).attr("r", 5).attr("fill", "#fff");

    // Score text
    svg.append("text")
      .attr("x", cx).attr("y", cy - 30)
      .attr("text-anchor", "middle")
      .attr("fill", "#fff").attr("font-size", 32).attr("font-weight", 700)
      .text(result.score.toFixed(0));

    // Category
    const catColor = result.category === "good" ? "#44cc88" : result.category === "moderate" ? "#ffaa00" : "#ff4444";
    svg.append("text")
      .attr("x", cx).attr("y", cy - 5)
      .attr("text-anchor", "middle")
      .attr("fill", catColor).attr("font-size", 13)
      .text(t(`healthIndex.category.${result.category}` as const));
  }, [result, t]);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const f = e.dataTransfer.files?.[0] ?? null;
    if (f) setFile(f);
  };

  const exportDeviations = () => {
    if (!result) return;
    exportTable(
      result.per_genus_deviation.map(g => ({
        Genus: g.genus,
        User_Abundance: g.user_abundance,
        NC_Mean: g.nc_mean,
        NC_Median: g.nc_median,
        Status: g.status,
      })),
      `health_index_deviation_${Date.now()}`,
    );
  };

  const hasInput = !!file || pasteText.trim().length > 0;

  return (
    <div className={classes.panel}>
      <h2 className={classes.title}>{t("healthIndex.title")}</h2>
      <p className={classes.subtitle}>{t("healthIndex.subtitle")}</p>

      {/* 输入区 */}
      <div className={classes.inputRow}>
        <div
          className={`${classes.dropZone} ${file ? classes.dropZoneActive : ""}`}
          onClick={() => fileInputRef.current?.click()}
          onDrop={handleDrop}
          onDragOver={e => e.preventDefault()}
        >
          <input ref={fileInputRef} type="file" accept=".csv,.tsv,.txt" onChange={e => setFile(e.target.files?.[0] ?? null)} style={{ display: "none" }} />
          <p>{file ? file.name : "CSV / TSV"}</p>
          <p style={{ fontSize: "0.75rem", color: "#888" }}>genus_name, abundance</p>
        </div>
        <textarea
          className={classes.textarea}
          placeholder={"Bacteroides,0.25\nFaecalibacterium,0.18\nPrevotella,0.12"}
          value={pasteText}
          onChange={e => setPasteText(e.target.value)}
        />
        <button className={classes.calcBtn} onClick={calculate} disabled={!hasInput || loading}>
          {loading ? t("healthIndex.calculating") : t("healthIndex.calculate")}
        </button>
      </div>

      {error && <div className={classes.error}>{error}</div>}

      {/* 结果 */}
      {result && (
        <div className={classes.results}>
          <div className={classes.gaugeRow}>
            <svg ref={gaugeRef} className={classes.gauge} />
            <div className={classes.statsCol}>
              <div className={classes.statItem}>
                <span className={classes.statVal}>{result.health_genera_matched}</span>
                <span className={classes.statLbl}>{t("healthIndex.healthGenera")}</span>
              </div>
              <div className={classes.statItem}>
                <span className={classes.statVal}>{result.disease_genera_matched}</span>
                <span className={classes.statLbl}>{t("healthIndex.diseaseGenera")}</span>
              </div>
              <div className={classes.statItem}>
                <span className={classes.statVal}>{result.reference.n_nc_samples.toLocaleString()}</span>
                <span className={classes.statLbl}>NC samples (ref)</span>
              </div>
            </div>
          </div>

          {/* 偏离表 */}
          <div className={classes.deviationSection}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
              <h3>{t("healthIndex.deviation")}</h3>
              <button className={classes.exportBtn} onClick={exportDeviations}>{t("export.csv")}</button>
            </div>
            <div className={classes.tableWrap}>
              <table className={classes.table}>
                <thead>
                  <tr>
                    <th>{t("healthIndex.col.genus")}</th>
                    <th>{t("healthIndex.col.yours")}</th>
                    <th>{t("healthIndex.col.ncMean")}</th>
                    <th>{t("healthIndex.col.ncMedian")}</th>
                    <th>{t("healthIndex.col.status")}</th>
                  </tr>
                </thead>
                <tbody>
                  {result.per_genus_deviation.map(g => (
                    <tr key={g.genus}>
                      <td>{g.genus}</td>
                      <td>{g.user_abundance.toFixed(4)}</td>
                      <td>{g.nc_mean.toFixed(4)}</td>
                      <td>{g.nc_median.toFixed(4)}</td>
                      <td>
                        <span className={classes.statusBadge} data-status={g.status}>
                          {g.status === "high" ? "↑" : g.status === "low" ? "↓" : "—"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default HealthIndexPanel;
