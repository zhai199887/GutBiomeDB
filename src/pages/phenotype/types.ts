/**
 * Shared types for Phenotype Association Analysis page
 */

export { API_BASE } from "@/util/apiBase";

export type DimType = "age" | "sex" | "disease";
export type TaxLevel = "genus" | "phylum";
export type ViewMode = "butterfly" | "lollipop" | "prevalence";

export interface PhenotypeAssociationResult {
  taxon: string;
  rank: TaxLevel;
  phylum: string;
  mean_a: number;
  mean_b: number;
  median_a: number;
  median_b: number;
  prevalence_a: number;
  prevalence_b: number;
  n_a: number;
  n_b: number;
  log2fc: number;
  lda_score: number | null;
  effect_size: number;
  p_value: number;
  adjusted_p: number;
  enriched_in: "a" | "b" | "none";
  ci_low: number;
  ci_high: number;
}

export interface PhenotypeAssociationResponse {
  group_a: string;
  group_b: string;
  dim_type: string;
  tax_level: string;
  method: string;
  n_a: number;
  n_b: number;
  total_taxa: number;
  significant_count: number;
  results: PhenotypeAssociationResult[];
}

export interface PhenotypeGroup {
  group: string;
  sample_count: number;
}

export interface PhenotypeGroupsResponse {
  dim_type: string;
  groups: PhenotypeGroup[];
}

export interface BoxGroupData {
  group: string;
  n: number;
  median: number;
  mean: number;
  q1: number;
  q3: number;
  whisker_low: number;
  whisker_high: number;
  outliers: number[];
}

/** Phylum → color mapping (consistent with rest of platform) */
export const PHYLUM_COLORS: Record<string, string> = {
  Firmicutes: "#4dabf7",
  Bacillota: "#4dabf7",
  Bacteroidetes: "#51cf66",
  Bacteroidota: "#51cf66",
  Proteobacteria: "#ff6b6b",
  Pseudomonadota: "#ff6b6b",
  Actinobacteria: "#ffd43b",
  Actinomycetota: "#ffd43b",
  Verrucomicrobia: "#cc5de8",
  Verrucomicrobiota: "#cc5de8",
  Fusobacteriota: "#ff922b",
  Fusobacteria: "#ff922b",
  Spirochaetota: "#20c997",
  Spirochaetes: "#20c997",
  Campylobacterota: "#f06595",
  Euryarchaeota: "#94d82d",
  Other: "#adb5bd",
};

export function getPhylumColor(phylum: string): string {
  return PHYLUM_COLORS[phylum] ?? (PHYLUM_COLORS.Other as string);
}

export function sigLabel(p: number): string {
  if (p < 0.001) return "***";
  if (p < 0.01) return "**";
  if (p < 0.05) return "*";
  return "ns";
}

export function sigString(p: number): string {
  if (p < 0.001) return "p<0.001 ***";
  if (p < 0.01) return `p=${p.toFixed(3)} **`;
  if (p < 0.05) return `p=${p.toFixed(3)} *`;
  return `p=${p.toFixed(3)} ns`;
}
