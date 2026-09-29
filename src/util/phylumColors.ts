const PHYLUM_COLORS: Record<string, string> = {
  Bacillota: "#f97316",
  Firmicutes: "#f97316",
  Bacteroidota: "#06b6d4",
  Bacteroidetes: "#06b6d4",
  Actinomycetota: "#facc15",
  Actinobacteria: "#facc15",
  Pseudomonadota: "#3b82f6",
  Proteobacteria: "#3b82f6",
  Verrucomicrobiota: "#14b8a6",
  Verrucomicrobia: "#14b8a6",
  Fusobacteriota: "#ef4444",
  Fusobacteria: "#ef4444",
  Cyanobacteriota: "#8b5cf6",
  Cyanobacteria: "#8b5cf6",
  Spirochaetota: "#ec4899",
  Spirochaetes: "#ec4899",
  Desulfobacterota: "#a855f7",
  Acidobacteriota: "#22c55e",
  Deinococcota: "#e11d48",
  Other: "#6b7280",
};

export function phylumColor(phylum: string): string {
  return PHYLUM_COLORS[phylum] ?? "#94a3b8";
}
