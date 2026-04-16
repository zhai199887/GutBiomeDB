/**
 * apiCache.ts — High-performance API response cache with request deduplication
 */

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

  const promise = fetch(url)
    .then(async (res) => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      cache.set(url, { data, ts: Date.now() });
      inflight.delete(url);
      return data;
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
