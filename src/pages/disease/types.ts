export interface DiseaseListItem {
  name: string;
  sample_count: number;
  kind?: "disease" | "special_population" | "healthy_control";
  category?: string;
  category_zh?: string;
  standard_name?: string;
  standard_name_zh?: string;
  abbreviation?: string;
  mesh_id?: string;
  icd10?: string;
}

export interface GenusEntry {
  genus: string;
  phylum: string;
  disease_mean: number;
  disease_prevalence: number;
  control_mean: number;
  control_prevalence: number;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  effect_size: number;
  enriched_in: "disease" | "control" | "none";
  ci_low: number;
  ci_high: number;
}

export interface DemoEntry {
  name: string;
  count: number;
}

export interface DiseaseProfile {
  disease: string;
  sample_count: number;
  control_count: number;
  n_studies: number;
  study_ids: string[];
  standard_name?: string;
  standard_name_zh?: string;
  abbreviation?: string;
  mesh_id?: string;
  icd10?: string;
  category?: string;
  category_zh?: string;
  top_genera: GenusEntry[];
  by_country: DemoEntry[];
  by_age_group: DemoEntry[];
  by_sex: DemoEntry[];
}

export interface Marker {
  taxon: string;
  phylum: string;
  mean_disease: number;
  mean_control: number;
  log2fc: number;
  lda_score: number;
  p_value: number;
  adjusted_p: number;
  prevalence_disease: number;
  prevalence_control: number;
  enriched_in: "disease" | "control" | "none";
  ci_low: number;
  ci_high: number;
}

export interface BiomarkerResult {
  disease: string;
  n_disease: number;
  n_control: number;
  n_markers: number;
  markers: Marker[];
}

export interface LollipopItem {
  genus: string;
  phylum: string;
  log2fc: number;
  neg_log10p: number;
  p_value: number;
  adjusted_p: number;
  mean_disease: number;
  mean_control: number;
  prevalence_disease: number;
  prevalence_control: number;
}

export interface DiseaseStudyEntry {
  project_id: string;
  n_disease: number;
  n_control: number;
  country: string;
  pmid?: string | null;
  cscs_score: number;
  top_marker: string;
}

export interface DiseaseStudiesResult {
  disease: string;
  n_projects: number;
  projects: DiseaseStudyEntry[];
}
