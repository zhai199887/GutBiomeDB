/**
 * PhenotypeResultTable — sortable, filterable results table
 * 可排序可过滤的表型分析结果表格
 * Columns: Taxon | Phylum | Mean A | Mean B | log2FC | Effect Size | p value | adj.p | Sig | Enriched in
 */
import { useState, useMemo } from "react";
import { useI18n } from "@/i18n";
import type { PhenotypeAssociationResult } from "./types";
import { getPhylumColor, sigLabel } from "./types";

interface Props {
  results: PhenotypeAssociationResult[];
  labelA: string;
  labelB: string;
  onTaxonClick?: (taxon: string) => void;
}

type SortKey = keyof PhenotypeAssociationResult;

export default function PhenotypeResultTable({ results, labelA, labelB, onTaxonClick }: Props) {
  const { locale } = useI18n();
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("adjusted_p");
  const [sortAsc, setSortAsc] = useState(true);
  const [page, setPage] = useState(0);
  const [showOnlySig, setShowOnlySig] = useState(false);
  const PAGE_SIZE = 50;

  const filtered = useMemo(() => {
    let d = results;
    if (showOnlySig) d = d.filter(r => r.adjusted_p < 0.05);
    if (search.trim()) d = d.filter(r => r.taxon.toLowerCase().includes(search.trim().toLowerCase()));
    d = [...d].sort((a, b) => {
      const va = a[sortKey] as number | string | null;
      const vb = b[sortKey] as number | string | null;
      if (va === null || va === undefined) return 1;
      if (vb === null || vb === undefined) return -1;
      const cmp = va < vb ? -1 : va > vb ? 1 : 0;
      return sortAsc ? cmp : -cmp;
    });
    return d;
  }, [results, search, sortKey, sortAsc, showOnlySig]);

  const pages = Math.ceil(filtered.length / PAGE_SIZE);
  const pageData = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  const th = (label: string, key: SortKey) => (
    <th
      key={key}
      onClick={() => { if (sortKey === key) setSortAsc(!sortAsc); else { setSortKey(key); setSortAsc(true); } setPage(0); }}
      style={{ cursor: "pointer", padding: "0.4rem 0.5rem", borderBottom: "1px solid var(--gray)", fontSize: "0.75rem", textAlign: "left", whiteSpace: "nowrap", userSelect: "none", background: "var(--dark-gray)" }}
    >
      {label}{sortKey === key ? (sortAsc ? " ▲" : " ▼") : ""}
    </th>
  );

  const enrichLabel = (e: "a" | "b" | "none") => {
    if (e === "a") return <span style={{ color: "#4dabf7" }}>{labelA}</span>;
    if (e === "b") return <span style={{ color: "#51cf66" }}>{labelB}</span>;
    return <span style={{ color: "var(--light-gray)" }}>—</span>;
  };

  return (
    <div>
      {/* Controls */}
      <div style={{ display: "flex", gap: "0.8rem", alignItems: "center", marginBottom: "0.8rem", flexWrap: "wrap" }}>
        <input
          placeholder={locale === "zh" ? "搜索分类群…" : "Search taxon…"}
          value={search}
          onChange={e => { setSearch(e.target.value); setPage(0); }}
          style={{ background: "var(--dark-gray)", border: "1px solid var(--gray)", color: "var(--light-gray)", borderRadius: "4px", padding: "0.3rem 0.6rem", width: "200px" }}
        />
        <label style={{ fontSize: "0.8rem", color: "var(--light-gray)", display: "flex", alignItems: "center", gap: "0.3rem", cursor: "pointer" }}>
          <input type="checkbox" checked={showOnlySig} onChange={e => { setShowOnlySig(e.target.checked); setPage(0); }} />
          {locale === "zh" ? "仅显示显著结果 (adj.p<0.05)" : "Significant only (adj.p<0.05)"}
        </label>
        <span style={{ fontSize: "0.8rem", color: "var(--light-gray)", marginLeft: "auto" }}>
          {filtered.length} {locale === "zh" ? "条" : "results"}
        </span>
      </div>

      {/* Table */}
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
          <thead>
            <tr>
              {th(locale === "zh" ? "分类群" : "Taxon", "taxon")}
              {th(locale === "zh" ? "门" : "Phylum", "phylum")}
              {th(`Mean ${labelA} (%)`, "mean_a")}
              {th(`Mean ${labelB} (%)`, "mean_b")}
              {th("log2FC", "log2fc")}
              {th(locale === "zh" ? "效应量" : "Effect Size", "effect_size")}
              {th("p value", "p_value")}
              {th("adj.p", "adjusted_p")}
              {th(locale === "zh" ? "显著" : "Sig.", "adjusted_p")}
              {th(locale === "zh" ? "富集于" : "Enriched in", "enriched_in")}
            </tr>
          </thead>
          <tbody>
            {pageData.map((r, i) => (
              <tr
                key={r.taxon}
                style={{ background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,0.02)", cursor: onTaxonClick ? "pointer" : "default" }}
                onClick={() => onTaxonClick?.(r.taxon)}
              >
                <td style={{ padding: "0.3rem 0.5rem", fontStyle: "italic", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                  {r.taxon}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                  {r.phylum ? (
                    <span style={{ display: "inline-flex", alignItems: "center", gap: "4px" }}>
                      <span style={{ width: 8, height: 8, borderRadius: "50%", background: getPhylumColor(r.phylum), display: "inline-block" }} />
                      {r.phylum}
                    </span>
                  ) : "—"}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right" }}>{r.mean_a.toFixed(4)}</td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right" }}>{r.mean_b.toFixed(4)}</td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right", color: r.log2fc > 0 ? "#4dabf7" : "#ff6b6b" }}>
                  {r.log2fc > 0 ? "+" : ""}{r.log2fc.toFixed(3)}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right" }}>{r.effect_size.toFixed(3)}</td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right" }}>
                  {r.p_value < 0.001 ? "<0.001" : r.p_value.toFixed(4)}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "right" }}>
                  {r.adjusted_p < 0.001 ? "<0.001" : r.adjusted_p.toFixed(4)}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)", textAlign: "center", color: r.adjusted_p < 0.05 ? "#ff6b6b" : "var(--light-gray)", fontWeight: r.adjusted_p < 0.05 ? 700 : 400 }}>
                  {sigLabel(r.adjusted_p)}
                </td>
                <td style={{ padding: "0.3rem 0.5rem", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                  {enrichLabel(r.enriched_in)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pages > 1 && (
        <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginTop: "0.8rem", fontSize: "0.8rem" }}>
          <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
            style={{ background: "none", border: "1px solid var(--gray)", color: "var(--light-gray)", borderRadius: "4px", padding: "0.2rem 0.6rem", cursor: "pointer", opacity: page === 0 ? 0.4 : 1 }}>
            ‹
          </button>
          <span style={{ color: "var(--light-gray)" }}>
            {page + 1} / {pages}
          </span>
          <button onClick={() => setPage(p => Math.min(pages - 1, p + 1))} disabled={page >= pages - 1}
            style={{ background: "none", border: "1px solid var(--gray)", color: "var(--light-gray)", borderRadius: "4px", padding: "0.2rem 0.6rem", cursor: "pointer", opacity: page >= pages - 1 ? 0.4 : 1 }}>
            ›
          </button>
        </div>
      )}
    </div>
  );
}
