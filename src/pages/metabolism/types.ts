export interface MetabolismCategory {
  id: string;
  name_en: string;
  name_zh: string;
  icon: string;
  description: string;
  description_zh?: string;
  taxa: string[];
  genus_exact_names?: string[];
  key_metabolites: string[];
  key_metabolites_zh?: string[];
  related_pathways: string[];
  related_pathways_zh?: string[];
  kegg_pathway_ids?: string[];
  metacyc_pathway_ids?: string[];
  health_relevance: string;
  health_relevance_zh?: string;
  references: string[];
}

export interface MetabolismMapping {
  version: string;
  last_updated: string;
  categories: MetabolismCategory[];
}

export interface CategoryDiseaseProfile {
  disease: string;
  sample_count: number;
  control_count: number;
  mean_disease: number;
  mean_nc: number;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  effect_size: number;
  direction: "enriched" | "depleted" | "neutral";
}

export interface CategoryProfileResult {
  category_id: string;
  category_name_en: string;
  category_name_zh: string;
  n_matched: number;
  matched_genera: string[];
  unmatched_genera?: string[];
  control_count: number;
  disease_profiles: CategoryDiseaseProfile[];
  warning?: string;
}

export interface MetabolismOverviewDisease {
  name: string;
  sample_count: number;
}

export interface MetabolismOverviewCategory {
  category_id: string;
  name_en: string;
  name_zh: string;
  icon: string;
  n_matched: number;
  matched_genera: string[];
  values: Array<number | null>;
}

export interface MetabolismOverviewResult {
  diseases: MetabolismOverviewDisease[];
  categories: MetabolismOverviewCategory[];
  warning?: string;
  generated_at?: string;
}
