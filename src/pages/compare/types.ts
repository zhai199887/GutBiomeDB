/**
 * Shared types for differential analysis page
 * 差异分析页面共享类型定义
 */

export interface GroupFilter {
  country: string;
  disease: string;
  age_group: string;
  sex: string;
}

export interface DiffTaxon {
  taxon: string;
  mean_a: number;
  mean_b: number;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  effect_size: number;
}

export interface DiffResult {
  summary: {
    group_a_name: string;
    group_b_name: string;
    group_a_n: number;
    group_b_n: number;
    taxonomy_level: string;
    method: string;
    total_taxa: number;
  };
  diff_taxa: DiffTaxon[];
  alpha_diversity: {
    group_a: { shannon: number[]; simpson: number[] };
    group_b: { shannon: number[]; simpson: number[] };
  };
  beta_diversity: {
    pcoa_coords: { x: number; y: number; group: "A" | "B" }[];
  };
}

export interface FilterOptions {
  countries: string[];
  diseases: string[];
  age_groups: string[];
  sexes: string[];
}

export const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
export const TAXONOMY_LEVELS = ["genus", "phylum"] as const;
export const METHODS = ["wilcoxon", "t-test"] as const;
export type Tab = "bar" | "volcano" | "alpha" | "beta";
