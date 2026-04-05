/**
 * PhenotypeExport — CSV / SVG / PNG export buttons
 * 导出按钮：CSV / SVG / PNG 三种格式
 */
import { useI18n } from "@/i18n";
import type { PhenotypeAssociationResponse } from "./types";

interface Props {
  result: PhenotypeAssociationResponse;
  svgId: string; // id of the SVG element to export
}

export default function PhenotypeExport({ result, svgId }: Props) {
  const { locale } = useI18n();

  const exportCSV = () => {
    const header = ["taxon", "phylum", "rank", "mean_a", "mean_b", "log2fc", "effect_size", "prevalence_a", "prevalence_b", "p_value", "adjusted_p", "enriched_in"];
    const rows = result.results.map(r => [
      r.taxon, r.phylum, r.rank,
      r.mean_a, r.mean_b, r.log2fc, r.effect_size,
      r.prevalence_a, r.prevalence_b,
      r.p_value, r.adjusted_p, r.enriched_in,
    ]);
    const csv = [header, ...rows].map(r => r.map(v => `"${v}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `phenotype_${result.group_a}_vs_${result.group_b}_${result.tax_level}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportSVG = () => {
    const svgEl = document.getElementById(svgId);
    if (!svgEl) return;
    const serializer = new XMLSerializer();
    const svgStr = serializer.serializeToString(svgEl);
    const blob = new Blob([svgStr], { type: "image/svg+xml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `phenotype_${result.group_a}_vs_${result.group_b}.svg`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportPNG = () => {
    const svgEl = document.getElementById(svgId) as SVGSVGElement | null;
    if (!svgEl) return;
    const serializer = new XMLSerializer();
    const svgStr = serializer.serializeToString(svgEl);
    const canvas = document.createElement("canvas");
    const bbox = svgEl.getBoundingClientRect();
    canvas.width = bbox.width * 2;
    canvas.height = bbox.height * 2;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.scale(2, 2);
    const img = new Image();
    const blob = new Blob([svgStr], { type: "image/svg+xml" });
    const url = URL.createObjectURL(blob);
    img.onload = () => {
      ctx.drawImage(img, 0, 0);
      URL.revokeObjectURL(url);
      const pngUrl = canvas.toDataURL("image/png");
      const a = document.createElement("a");
      a.href = pngUrl;
      a.download = `phenotype_${result.group_a}_vs_${result.group_b}.png`;
      a.click();
    };
    img.src = url;
  };

  const btnStyle: React.CSSProperties = {
    background: "none",
    border: "1px solid var(--gray)",
    color: "var(--light-gray)",
    borderRadius: "4px",
    padding: "0.3rem 0.7rem",
    cursor: "pointer",
    fontSize: "0.8rem",
  };

  return (
    <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
      <span style={{ fontSize: "0.75rem", color: "var(--light-gray)" }}>
        {locale === "zh" ? "导出：" : "Export:"}
      </span>
      <button style={btnStyle} onClick={exportCSV}>CSV</button>
      <button style={btnStyle} onClick={exportSVG}>SVG</button>
      <button style={btnStyle} onClick={exportPNG}>PNG</button>
    </div>
  );
}
