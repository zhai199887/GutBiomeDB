export interface StudiesSummary {
  total_projects: number;
  total_samples: number;
  total_nc: number;
  total_disease: number;
  n_countries: number;
  n_diseases: number;
  year_range: number[];
}

export interface StudyProjectInfo {
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

export interface ProjectCountRow {
  count: number;
  country?: string;
  age_group?: string;
  sex?: string;
  disease?: string;
}

export interface ProjectDetailResult extends StudyProjectInfo {
  total_samples: number;
  ncbi_url: string;
  by_disease: ProjectCountRow[];
  by_country: ProjectCountRow[];
  by_age_group: ProjectCountRow[];
  by_sex: ProjectCountRow[];
}

export interface ProjectListResult {
  projects: StudyProjectInfo[];
  total: number;
  summary: StudiesSummary;
}

export interface ProjectTimelinePoint {
  year: number;
  n_samples: number;
  n_projects: number;
}

export interface ProjectTimelineResult {
  timeline: ProjectTimelinePoint[];
}
