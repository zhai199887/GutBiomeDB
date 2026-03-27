import type { FeatureCollection, Geometry } from "geojson";
import { create } from "zustand";

// ── Geo types ────────────────────────────────────────────────────────────────

export type ByGeo = FeatureCollection<
  Geometry,
  (typeof import("../../public/by-country.json"))["features"][number]["properties"]
>;

// ── Metadata summary (aggregated stats) ──────────────────────────────────────

export type CountryStat = {
  total: number;
  sex: { female_pct: number | null; male_pct: number | null; known: number };
  top_ages: Record<string, number>;
  top_diseases: Record<string, number>;
};

export type MetadataSummary = {
  total_samples: number;
  age_counts: Record<string, number>;
  sex_counts: Record<string, number>;
  disease_counts: Record<string, number>;
  country_counts: Record<string, number>;
  region_counts: Record<string, number>;
  age_sex_cross: { age_group: string; sex: string; count: number }[];
  age_disease_cross: { age_group: string; disease: string; count: number }[];
  top20_diseases: string[];
  country_stats: Record<string, CountryStat>;
};

// ── Per-sample metadata record ────────────────────────────────────────────────

export type SampleRecord = {
  sample_id: string;
  country: string;
  region: string;
  project: string;
  age_group: string;
  sex: string;
  disease: string;
};

// ── Abundance summary ─────────────────────────────────────────────────────────

export type AbundanceSummary = {
  genera: string[];
  by_age_group: Record<string, Record<string, number>>;
  by_sex: Record<string, Record<string, number>>;
  by_disease: Record<string, Record<string, number>>;
};

// ── Search types ──────────────────────────────────────────────────────────────

export type SampleEntry = {
  name: string;
  type: "Sample" | "Project";
  samples: number;
  /** Sample-only fields */
  age_group?: string;
  sex?: string;
  disease?: string;
  country?: string;
  /** Project-only summary fields */
  top_disease?: string;
  disease_types?: string;
  age_groups?: string;
  fuzzy?: boolean;
};

export type SampleSearch = SampleEntry[];

export type GeoSearch = {
  name: string;
  type: "Region" | "Country";
  samples: number;
  fuzzy?: boolean;
}[];

// ── Filters ───────────────────────────────────────────────────────────────────

export type Filters = {
  sex: string;
  age_groups: string[];
  diseases: string[];
};

export const DEFAULT_FILTERS: Filters = {
  sex: "all",
  age_groups: [],
  diseases: [],
};

// ── Global store ──────────────────────────────────────────────────────────────

export type Data = {
  byCountry?: ByGeo;
  summary?: MetadataSummary;
  abundance?: AbundanceSummary;
  samples?: SampleRecord[];
  sampleSearch?: SampleSearch;
  geoSearch?: GeoSearch;
  selectedFeature?: { region: string; country: string; code: string };
  filters: Filters;
  loading: Record<string, boolean>;
};

export const useData = create<Data>(() => ({
  filters: DEFAULT_FILTERS,
  loading: {},
}));

// ── Loaders ───────────────────────────────────────────────────────────────────

const setLoading = (key: string, val: boolean) =>
  useData.setState((s) => ({ loading: { ...s.loading, [key]: val } }));

const request = async <T>(url: string): Promise<T> => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status}`);
  return res.json() as Promise<T>;
};

export const loadGeoData = async () => {
  setLoading("geo", true);
  try {
    const byCountry = await request<ByGeo>("data/by-country.json");
    useData.setState({ byCountry });

    // Build geo search list from country counts in summary
    const summary = useData.getState().summary;
    if (summary) buildGeoSearch(summary);
  } finally {
    setLoading("geo", false);
  }
};

export const loadSummary = async () => {
  setLoading("summary", true);
  try {
    const summary = await request<MetadataSummary>("data/metadata_summary.json");
    useData.setState({ summary });
    buildGeoSearch(summary);
  } finally {
    setLoading("summary", false);
  }
};

export const loadAbundance = async () => {
  setLoading("abundance", true);
  try {
    const abundance = await request<AbundanceSummary>(
      "data/abundance_summary.json",
    );
    useData.setState({ abundance });
  } finally {
    setLoading("abundance", false);
  }
};

export const loadSamples = async () => {
  setLoading("samples", true);
  try {
    const samples = await request<SampleRecord[]>("data/metadata.json");
    useData.setState({ samples });

    /** Aggregate project-level stats from all constituent samples */
    const projectMap = new Map<
      string,
      {
        count: number;
        diseases: Map<string, number>;
        age_groups: Set<string>;
        sexes: Set<string>;
      }
    >();

    for (const s of samples) {
      if (!projectMap.has(s.project)) {
        projectMap.set(s.project, {
          count: 0,
          diseases: new Map(),
          age_groups: new Set(),
          sexes: new Set(),
        });
      }
      const p = projectMap.get(s.project)!;
      p.count++;
      p.diseases.set(s.disease, (p.diseases.get(s.disease) ?? 0) + 1);
      p.age_groups.add(s.age_group);
      if (s.sex !== "unknown") p.sexes.add(s.sex);
    }

    const list: SampleSearch = [];

    /** Project rows: show aggregated summary, not single-sample values */
    for (const [proj, info] of projectMap) {
      const topDisease = [...info.diseases.entries()]
        .filter(([d]) => d !== "unknown")
        .sort((a, b) => b[1] - a[1])[0]?.[0] ?? "unknown";

      const diseaseCount = info.diseases.size;
      const ageGroupCount = info.age_groups.size;

      list.push({
        type: "Project",
        name: proj,
        samples: info.count,
        top_disease: topDisease,
        disease_types: `${diseaseCount} type${diseaseCount !== 1 ? "s" : ""}`,
        age_groups: `${ageGroupCount} group${ageGroupCount !== 1 ? "s" : ""}`,
      });
    }

    /** Sample rows: individual-level annotations */
    for (const s of samples) {
      list.push({
        type: "Sample",
        name: s.sample_id,
        samples: 1,
        age_group: s.age_group,
        sex: s.sex,
        disease: s.disease,
        country: s.country,
      });
    }

    useData.setState({ sampleSearch: list });
  } finally {
    setLoading("samples", false);
  }
};

const buildGeoSearch = (summary: MetadataSummary) => {
  const list: GeoSearch = [];
  for (const [name, samples] of Object.entries(summary.country_counts)) {
    list.push({ type: "Country", name, samples });
  }
  for (const [name, samples] of Object.entries(summary.region_counts)) {
    list.push({ type: "Region", name, samples });
  }
  useData.setState({ geoSearch: list });
};

// ── Map feature selection ─────────────────────────────────────────────────────

export const setSelectedFeature = (
  feature?: Data["selectedFeature"],
) =>
  useData.setState({
    selectedFeature:
      useData.getState().selectedFeature === feature ? undefined : feature,
  });

// ── Filter actions ────────────────────────────────────────────────────────────

export const setFilters = (patch: Partial<Filters>) =>
  useData.setState((s) => ({ filters: { ...s.filters, ...patch } }));

export const resetFilters = () =>
  useData.setState({ filters: DEFAULT_FILTERS });

// ── Apply filters to sample list ──────────────────────────────────────────────

export const applyFilters = (
  samples: SampleRecord[],
  filters: Filters,
): SampleRecord[] => {
  let result = samples;
  if (filters.sex !== "all") {
    result = result.filter((s) => s.sex === filters.sex);
  }
  if (filters.age_groups.length > 0) {
    result = result.filter((s) => filters.age_groups.includes(s.age_group));
  }
  if (filters.diseases.length > 0) {
    result = result.filter((s) => filters.diseases.includes(s.disease));
  }
  return result;
};
