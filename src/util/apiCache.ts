/**
 * apiCache.ts — High-performance API response cache with request deduplication
 */

import {
  getAnalysisJob,
  latestRememberedAnalysisJob,
  submitAnalysisJob,
  type AnalysisJobKind,
} from "./analysisJobs";

interface CacheEntry {
  data: unknown;
  ts: number;
}

const cache = new Map<string, CacheEntry>();
const inflight = new Map<string, Promise<unknown>>();

// Static data (disease lists, filter options) cache 30 min; dynamic data 10 min
const STATIC_PATTERNS = [
  "/api/disease-list", "/api/disease-names-zh", "/api/filter-options",
  "/api/data-stats", "/api/project-list", "/api/health-index/reference",
  "/api/metabolism-overview", "/api/metabolism-category-profile",
];
const STATIC_TTL = 30 * 60 * 1000; // 30 minutes
const DYNAMIC_TTL = 10 * 60 * 1000; // 10 minutes

const TRACKED_GET_KINDS: Array<[string, AnalysisJobKind]> = [
  ["/api/biomarker-profile", "biomarker-profile"],
  ["/api/biomarker-discovery", "biomarker-discovery"],
  ["/api/lollipop-data", "lollipop-data"],
  ["/api/network-compare", "network-compare"],
  ["/api/network", "network"],
  ["/api/cooccurrence", "cooccurrence"],
  ["/api/chord-data", "chord-data"],
  ["/api/species-cooccurrence", "species-cooccurrence"],
  ["/api/phenotype-taxa-profile", "phenotype-taxa-profile"],
  ["/api/disease-profile", "disease-profile"],
  ["/api/disease-studies", "disease-studies"],
  ["/api/lifecycle-compare", "lifecycle-compare"],
  ["/api/lifecycle", "lifecycle"],
  ["/api/metabolism-category-profile", "metabolism-category-profile"],
  ["/api/metabolism-overview", "metabolism-overview"],
];

const NUMERIC_QUERY_KEYS = new Set([
  "top_n", "top_genera", "top_diseases", "top_k", "min_r", "max_samples",
  "fdr_threshold", "lda_threshold", "p_threshold", "min_samples",
]);

function trackedKind(url: string): AnalysisJobKind | null {
  try {
    const pathname = new URL(url, window.location.origin).pathname;
    return TRACKED_GET_KINDS.find(([prefix]) => pathname === prefix)?.[1] ?? null;
  } catch {
    return null;
  }
}

async function waitForTrackedJob<T>(jobId: string): Promise<T> {
  for (;;) {
    const job = await getAnalysisJob<T>(jobId);
    if (job.status === "completed") return job.result as T;
    if (job.status === "failed") throw new Error(job.error ?? "Analysis job failed");
    await new Promise((resolve) => window.setTimeout(resolve, 700));
  }
}

async function trackedGet<T>(url: string, kind: AnalysisJobKind): Promise<T> {
  const previous = latestRememberedAnalysisJob(kind, url);
  if (previous) {
    try {
      return await waitForTrackedJob<T>(previous);
    } catch {
      // Submit a fresh job if the previous snapshot expired or failed.
    }
  }
  const parsed = new URL(url, window.location.origin);
  const payload: Record<string, string | number> = {};
  parsed.searchParams.forEach((value, key) => {
    payload[key] = NUMERIC_QUERY_KEYS.has(key) ? Number(value) : value;
  });
  const job = await submitAnalysisJob(kind, payload, url);
  return waitForTrackedJob<T>(job.job_id);
}

function getTTL(url: string): number {
  return STATIC_PATTERNS.some(p => url.includes(p)) ? STATIC_TTL : DYNAMIC_TTL;
}

/**
 * Fetch with caching + request deduplication.
 * - Returns cached response if available and not expired.
 * - Deduplicates concurrent requests to the same URL.
 */
export async function cachedFetch<T>(url: string): Promise<T> {
  const now = Date.now();
  const entry = cache.get(url);
  const ttl = getTTL(url);

  if (entry && now - entry.ts < ttl) {
    return entry.data as T;
  }

  // Deduplicate concurrent requests to the same URL
  const existing = inflight.get(url);
  if (existing) {
    return existing as Promise<T>;
  }

  const tracked = trackedKind(url);
  const promise = (tracked ? trackedGet<T>(url, tracked) : fetch(url)
    .then(async (res) => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    }))
    .then((data) => {
      cache.set(url, { data, ts: Date.now() });
      inflight.delete(url);
      return data as T;
    })
    .catch((err) => {
      inflight.delete(url);
      throw err;
    });

  inflight.set(url, promise);

  // Evict old entries if cache grows too large
  if (cache.size > 200) {
    const sorted = [...cache.entries()].sort((a, b) => a[1].ts - b[1].ts);
    for (let i = 0; i < 50; i++) cache.delete(sorted[i][0]);
  }

  return promise as Promise<T>;
}

/** Clear all cached entries */
export function clearApiCache() {
  cache.clear();
  inflight.clear();
}
