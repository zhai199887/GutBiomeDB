export { API_BASE } from "@/util/apiBase";

export type NetworkMethod = "spearman" | "sparcc";
export type ColorMode = "phylum" | "community";

export interface DiseaseItem {
  name: string;
  sample_count: number;
  standard_name?: string;
  standard_name_zh?: string;
  abbreviation?: string;
  category?: string;
  category_zh?: string;
}

export interface CoNode {
  id: string;
  mean_abundance: number;
  prevalence: number;
  phylum: string;
  degree: number;
  betweenness: number;
  community: number;
  is_hub: boolean;
  x?: number;
  y?: number;
  fx?: number | null;
  fy?: number | null;
}

export interface CoEdge {
  source: string | CoNode;
  target: string | CoNode;
  source_phylum: string;
  target_phylum: string;
  r: number;
  p_value: number;
  adjusted_p: number;
  type: "positive" | "negative";
  method: NetworkMethod | string;
}

export interface CoData {
  disease: string;
  n_samples: number;
  n_genera: number;
  n_edges: number;
  min_r: number;
  method: NetworkMethod | string;
  method_note: string;
  available_methods: Record<string, boolean>;
  fdr_threshold: number;
  hub_nodes: string[];
  n_communities: number;
  network_density: number;
  positive_edge_count: number;
  negative_edge_count: number;
  nodes: CoNode[];
  edges: CoEdge[];
}

export interface CompareEdge {
  source: string;
  target: string;
  present_in: "disease_only" | "control_only";
  r: number;
  adjusted_p: number;
  type: "positive" | "negative";
  source_phylum: string;
  target_phylum: string;
}

export interface SignSwitchedEdge {
  source: string;
  target: string;
  disease_type: "positive" | "negative";
  control_type: "positive" | "negative";
  disease_r: number;
  control_r: number;
}

export interface NetworkCompareData {
  disease: string;
  method: NetworkMethod | string;
  disease_network: CoData;
  control_network: CoData;
  gained_edges: number;
  lost_edges: number;
  rewired_edges: CompareEdge[];
  sign_switched_edges: SignSwitchedEdge[];
}
