import { API_BASE } from "./apiBase";

export type AnalysisJobKind =
  | "diff-analysis"
  | "spearman-analysis"
  | "cross-study"
  | "phenotype-association"
  | "biomarker-discovery"
  | "lollipop-data"
  | "network"
  | "cooccurrence"
  | "network-compare"
  | "lifecycle"
  | "lifecycle-compare"
  | "similarity-search"
  | "health-index"
  | "health-score"
  | "disease-profile"
  | "disease-studies"
  | "biomarker-profile"
  | "species-cooccurrence"
  | "chord-data"
  | "phenotype-taxa-profile"
  | "metabolism-overview"
  | "metabolism-category-profile";
export type AnalysisJobStatus = "queued" | "running" | "completed" | "failed";

export type AnalysisJob<T = unknown> = {
  job_id: string;
  kind: AnalysisJobKind;
  status: AnalysisJobStatus;
  created_at: string;
  updated_at: string;
  error?: string;
  result?: T;
};

export type RememberedAnalysisJob = {
  kind: AnalysisJobKind;
  job_id: string;
  key?: string;
};

const STORAGE_KEY = "gutbiomedb.analysisJobs.v1";

function readRememberedJobs(): RememberedAnalysisJob[] {
  if (typeof window === "undefined") return [];
  try {
    const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "[]");
    return Array.isArray(parsed) ? parsed.filter((item) => item?.kind && item?.job_id) : [];
  } catch {
    return [];
  }
}

export function rememberAnalysisJob(kind: AnalysisJobKind, job_id: string, key?: string): void {
  if (typeof window === "undefined") return;
  const next = [
    { kind, job_id, ...(key ? { key } : {}) },
    ...readRememberedJobs().filter((item) => item.job_id !== job_id),
  ].slice(0, 20);
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  window.dispatchEvent(new Event("gutbiomedb:analysis-job"));
}

export function rememberedAnalysisJobs(): RememberedAnalysisJob[] {
  return readRememberedJobs();
}

export function latestRememberedAnalysisJob(kind: AnalysisJobKind, key?: string): string | null {
  return readRememberedJobs().find((item) => item.kind === kind && (!key || item.key === key))?.job_id ?? null;
}

export async function submitAnalysisJob(
  kind: AnalysisJobKind,
  payload: unknown,
  key?: string,
): Promise<AnalysisJob> {
  const response = await fetch(`${API_BASE}/api/analysis-jobs`, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ kind, payload }),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail ?? "Could not submit analysis job");
  rememberAnalysisJob(kind, data.job_id, key);
  return data as AnalysisJob;
}

export async function getAnalysisJob<T = unknown>(job_id: string): Promise<AnalysisJob<T>> {
  const response = await fetch(`${API_BASE}/api/analysis-jobs/${encodeURIComponent(job_id)}`, {
    cache: "no-store",
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail ?? "Could not read analysis job");
  return data as AnalysisJob<T>;
}
