/**
 * Utility functions for exporting data as CSV/TSV files
 */

type Row = Record<string, string | number | boolean | null | undefined>;

/** Download a string as a file */
function downloadBlob(content: string, filename: string, mime: string) {
  const blob = new Blob(["\uFEFF" + content], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

/** Escape a CSV cell value */
function escapeCell(value: unknown): string {
  const s = value == null ? "" : String(value);
  return s.includes(",") || s.includes('"') || s.includes("\n")
    ? `"${s.replace(/"/g, '""')}"`
    : s;
}

/**
 * Export an array of objects as CSV/TSV.
 * @param data - Array of row objects
 * @param filename - Download filename (without extension)
 * @param format - "csv" or "tsv"
 * @param columns - Optional column order; defaults to Object.keys of first row
 */
export function exportTable(
  data: Row[],
  filename: string,
  format: "csv" | "tsv" = "csv",
  columns?: string[],
) {
  if (!data.length) return;
  const sep = format === "tsv" ? "\t" : ",";
  const cols = columns ?? Object.keys(data[0]);
  const header = cols.join(sep);
  const rows = data.map((row) =>
    cols.map((c) => (format === "csv" ? escapeCell(row[c]) : String(row[c] ?? ""))).join(sep),
  );
  const ext = format === "tsv" ? "tsv" : "csv";
  const mime = format === "tsv" ? "text/tab-separated-values" : "text/csv";
  downloadBlob([header, ...rows].join("\n"), `${filename}.${ext}`, mime);
}

/**
 * Export raw text/JSON as a file.
 */
export function exportJSON(data: unknown, filename: string) {
  downloadBlob(JSON.stringify(data, null, 2), `${filename}.json`, "application/json");
}
