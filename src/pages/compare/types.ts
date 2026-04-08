export { API_BASE } from "@/util/apiBase";

export interface GroupFilter {
  country: string;
  disease: string;
  age_group: string;
  sex: string;
}

export interface GroupSampleCount {
  metadata_n: number;
  abundance_n: number;
}

export interface SampleCountResult {
  group_a: GroupSampleCount;
  group_b: GroupSampleCount;
}

export interface DiffTaxon {
  taxon: string;
  phylum: string;
  tax_level: string;
  mean_a: number;
  mean_b: number;
  prevalence_a: number;
  prevalence_b: number;
  log2fc: number;
  p_value: number;
  neg_log10_p?: number;
  adjusted_p: number;
  neg_log10_adjusted_p?: number;
  effect_size: number;
  enriched_in: "A" | "B";
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

export interface AlphaGroup {
  shannon: number[];
  simpson: number[];
  chao1: number[];
}

export interface BetaPoint {
  x: number;
  y: number;
  group: "A" | "B";
}

export interface BetaMetricResult {
  metric: string;
  variance_explained: number[];
  pcoa_coords: BetaPoint[];
}

export interface PhylumCompositionRow {
  phylum: string;
  group_a: number;
  group_b: number;
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
    significant_taxa: number;
  };
  diff_taxa: DiffTaxon[];
  alpha_diversity: {
    group_a: AlphaGroup;
    group_b: AlphaGroup;
  };
  alpha_pvalues: {
    shannon: number;
    simpson: number;
    chao1: number;
  };
  beta_diversity: {
    default_metric: string;
    metrics: Record<string, BetaMetricResult>;
  };
  phylum_composition: {
    groups: [string, string] | string[];
    rows: PhylumCompositionRow[];
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

export interface SpearmanTaxon {
  taxon: string;
  phylum: string;
  mean_abundance: number;
  prevalence: number;
}

export interface SpearmanEdge {
  source: string;
  target: string;
  source_phylum: string;
  target_phylum: string;
  r: number;
  p_value: number;
  type: "positive" | "negative";
}

export interface SpearmanResult {
  summary: {
    sample_count: number;
    taxonomy_level: string;
    max_taxa: number;
  };
  taxa: SpearmanTaxon[];
  matrix: number[][];
  p_values: number[][];
  edges: SpearmanEdge[];
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
  adjusted_meta_p: number;
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
  nc_count: number;
  disease_count: number;
  n_diseases: number;
  diseases: string[];
  has_control: boolean;
  country: string;
  country_list: string[];
  year: number | null;
  instrument: string;
  region_16s: string;
}

export const TAXONOMY_LEVELS = ["genus", "family", "phylum"] as const;
export const METHODS = ["wilcoxon", "t-test", "lefse", "permanova"] as const;
export const BETA_METRICS = ["braycurtis", "aitchison"] as const;
export type TaxonomyLevel = (typeof TAXONOMY_LEVELS)[number];
export type BetaMetric = (typeof BETA_METRICS)[number];
export type Tab =
  | "bar"
  | "volcano"
  | "alpha"
  | "beta"
  | "composition"
  | "heatmap"
  | "correlation"
  | "crossstudy"
  | "lefse"
  | "permanova";
