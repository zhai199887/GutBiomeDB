import { API_BASE } from "./apiBase";

export type AnalysisJobKind =
  | "diff-analysis"
  | "spearman-analysis"
  | "cross-study"
  | "phenotype-association";
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

type RememberedJob = {
  kind: AnalysisJobKind;
  job_id: string;
};

const STORAGE_KEY = "gutbiomedb.analysisJobs.v1";

function readRememberedJobs(): RememberedJob[] {
  if (typeof window === "undefined") return [];
  try {
    const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "[]");
    return Array.isArray(parsed) ? parsed.filter((item) => item?.kind && item?.job_id) : [];
  } catch {
    return [];
  }
}

export function rememberAnalysisJob(kind: AnalysisJobKind, job_id: string): void {
  if (typeof window === "undefined") return;
  const next = [
    { kind, job_id },
    ...readRememberedJobs().filter((item) => item.kind !== kind),
  ].slice(0, 6);
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
}

export function latestRememberedAnalysisJob(kind: AnalysisJobKind): string | null {
  return readRememberedJobs().find((item) => item.kind === kind)?.job_id ?? null;
}

export async function submitAnalysisJob(
  kind: AnalysisJobKind,
  payload: unknown,
): Promise<AnalysisJob> {
  const response = await fetch(`${API_BASE}/api/analysis-jobs`, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ kind, payload }),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail ?? "Could not submit analysis job");
  rememberAnalysisJob(kind, data.job_id);
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
