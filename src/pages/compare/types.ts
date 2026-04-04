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

export interface LefseFeature {
  taxon: string;
  lda_score: number;
  p_value: number;
  enriched_group: "A" | "B";
}

export interface PermanovaResult {
  f_statistic: number;
  p_value: number;
  r_squared: number;
  permutations: number;
  n_a: number;
  n_b: number;
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
  lefse_results?: LefseFeature[];
  permanova?: PermanovaResult;
}

export interface FilterOptions {
  countries: string[];
  diseases: string[];
  age_groups: string[];
  sexes: string[];
}

export interface CrossStudyProjectSummary {
  project_id: string;
  n_disease: number;
  n_control: number;
  error: string | null;
}

export interface CrossStudyMarker {
  taxon: string;
  meta_log2fc: number;
  meta_se: number;
  meta_p: number;
  ci_low: number;
  ci_high: number;
  n_studies: number;
  n_significant: number;
  I2: number;
  Q_p: number;
  direction: "disease" | "control" | "mixed";
  per_project: Record<string, { log2fc: number; se: number; p_value: number }>;
}

export interface CrossStudyResult {
  disease: string;
  method: string;
  taxonomy_level: string;
  n_projects: number;
  project_summaries: CrossStudyProjectSummary[];
  consensus_markers: CrossStudyMarker[];
  total_significant: number;
  all_markers: CrossStudyMarker[];
}

export interface ProjectInfo {
  project_id: string;
  sample_count: number;
  diseases: string[];
  has_control: boolean;
}

export const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
export const TAXONOMY_LEVELS = ["genus", "phylum"] as const;
export const METHODS = ["wilcoxon", "t-test", "lefse", "permanova"] as const;
export type Tab = "bar" | "volcano" | "alpha" | "beta" | "lefse" | "permanova" | "crossstudy";
