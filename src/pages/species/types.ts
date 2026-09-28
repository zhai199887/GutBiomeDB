export interface ProfileEntry {
  name: string;
  mean_abundance: number;
  prevalence: number;
  sample_count: number;
  median_abundance?: number;
  std_abundance?: number;
  p25?: number;
  p75?: number;
  log2fc?: number;
  p_value?: number;
  effect_size?: number;
  adjusted_p?: number;
  significant?: boolean;
}

export interface SpeciesProfile {
  genus: string;
  phylum: string;
  total_samples: number;
  present_samples: number;
  prevalence: number;
  mean_abundance: number;
  median_abundance: number;
  nc_mean: number;
  nc_prevalence: number;
  by_disease: ProfileEntry[];
  by_country: ProfileEntry[];
  by_age_group: ProfileEntry[];
  by_sex: ProfileEntry[];
}

export interface BiomarkerEntry {
  disease: string;
  n_samples: number;
  n_control: number;
  mean_disease: number;
  mean_control: number;
  log2fc: number;
  p_value: number;
  adjusted_p: number;
  direction: "enriched" | "depleted";
  significant: boolean;
  prevalence_disease: number;
  prevalence_control: number;
  effect_size: number;
}

export interface BiomarkerProfileData {
  genus: string;
  n_diseases_tested: number;
  n_enriched: number;
  n_depleted: number;
  n_control: number;
  control_mean: number;
  profiles: BiomarkerEntry[];
}

export interface CooccurrencePartner {
  genus: string;
  phylum?: string;
  r: number;
  p_value: number;
  adjusted_p?: number;
  significant?: boolean;
  type: "positive" | "negative";
}

export interface CooccurrenceResponse {
  genus: string;
  context: string;
  n_samples: number;
  partners: CooccurrencePartner[];
}

export interface DiseaseListItem {
  name: string;
  sample_count: number;
}

export interface SearchResponse {
  results: string[];
}
