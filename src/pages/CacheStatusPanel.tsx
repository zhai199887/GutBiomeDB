/**
 * CacheStatusPanel.tsx
 * Read-only cache observability view. Calls /api/cache-audit on mount
 * and on user click. No polling, no token.
 */
import { useCallback, useEffect, useState } from "react";
import { API_BASE } from "@/util/apiBase";
import classes from "./CacheStatusPanel.module.css";

interface TrackedEntry {
  path: string;
  method: string;
  fn_name: string;
  cache_key: string;
  version: string;
  current_hash: string;
  baseline_hash: string;
  stale: boolean;
  disk_file_count: number;
  disk_total_kb: number;
  disk_newest_age_hours: number | null;
}

interface UncachedEntry {
  path: string;
  method: string;
  fn_name: string;
  reason: string;
}

interface AuditResponse {
  summary: {
    total_endpoints: number;
    tracked: number;
    uncached_by_design: number;
    unknown: number;
    stale_count: number;
    disk_file_count: number;
    disk_orphan_count: number;
    baseline_seeded_at: string | null;
  };
  tracked: TrackedEntry[];
  uncached_by_design: UncachedEntry[];
  unknown: Array<{ path: string; method: string; status: string }>;
  disk_orphan_files: string[];
}

const REASON_LABEL: Record<string, string> = {
  download_wrapper: "Download stream",
  admin_endpoint: "Admin op",
  trivial_static: "Trivial",
  compatibility_redirect: "v1 redirect",
  intentional_no_cache: "No cache by design",
};

const formatAge = (hours: number | null): string => {
  if (hours == null) return "—";
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 48) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
};

const CacheStatusPanel = () => {
  const [data, setData] = useState<AuditResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState<"all" | "tracked" | "uncached" | "stale">("all");

  const refresh = useCallback(() => {
    setLoading(true);
    setError("");
    fetch(`${API_BASE}/api/cache-audit`, { signal: AbortSignal.timeout(15000) })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  if (loading && !data) {
    return (
      <section className={classes.panel}>
        <h2>Cache Status</h2>
        <p>Loading cache audit…</p>
      </section>
    );
  }

  if (error && !data) {
    return (
      <section className={classes.panel}>
        <h2>Cache Status</h2>
        <p className={classes.error}>Failed to load: {error}</p>
        <button className={classes.refreshBtn} onClick={refresh}>Retry</button>
      </section>
    );
  }

  if (!data) return null;

  const visibleTracked =
    filter === "stale" ? data.tracked.filter((t) => t.stale) :
    filter === "uncached" ? [] :
    data.tracked;
  const visibleUncached = filter === "tracked" || filter === "stale" ? [] : data.uncached_by_design;

  return (
    <section className={classes.panel}>
      <div className={classes.header}>
        <h2>Cache Status</h2>
        <button className={classes.refreshBtn} onClick={refresh} disabled={loading}>
          {loading ? "…" : "Refresh"}
        </button>
      </div>

      <div className={classes.summaryGrid}>
        <div className={classes.summaryCard}>
          <span className={classes.summaryValue}>{data.summary.total_endpoints}</span>
          <span className={classes.summaryLabel}>Endpoints</span>
        </div>
        <div className={classes.summaryCard} data-good="true">
          <span className={classes.summaryValue}>{data.summary.tracked}</span>
          <span className={classes.summaryLabel}>Cached</span>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryValue}>{data.summary.uncached_by_design}</span>
          <span className={classes.summaryLabel}>By design</span>
        </div>
        <div className={classes.summaryCard} data-warn={data.summary.stale_count > 0}>
          <span className={classes.summaryValue}>{data.summary.stale_count}</span>
          <span className={classes.summaryLabel}>Stale</span>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryValue}>{data.summary.disk_file_count}</span>
          <span className={classes.summaryLabel}>Disk files</span>
        </div>
        <div className={classes.summaryCard} data-warn={data.summary.disk_orphan_count > 0}>
          <span className={classes.summaryValue}>{data.summary.disk_orphan_count}</span>
          <span className={classes.summaryLabel}>Orphans</span>
        </div>
      </div>

      <div className={classes.filterBar}>
        {(["all", "tracked", "uncached", "stale"] as const).map((f) => (
          <button
            key={f}
            className={classes.filterBtn}
            data-active={filter === f}
            onClick={() => setFilter(f)}
          >
            {f}
          </button>
        ))}
      </div>

      {visibleTracked.length > 0 && (
        <div className={classes.tableWrap}>
          <h3>Cached endpoints ({visibleTracked.length})</h3>
          <table className={classes.table}>
            <thead>
              <tr>
                <th>Path</th>
                <th>Key</th>
                <th>Ver</th>
                <th>Files</th>
                <th>Size</th>
                <th>Age</th>
                <th>Hash</th>
              </tr>
            </thead>
            <tbody>
              {visibleTracked.map((t) => (
                <tr key={`${t.method}-${t.path}`} data-stale={t.stale}>
                  <td><code>{t.path}</code></td>
                  <td>{t.cache_key}</td>
                  <td>{t.version}</td>
                  <td>{t.disk_file_count}</td>
                  <td>{t.disk_total_kb.toFixed(1)} KB</td>
                  <td>{formatAge(t.disk_newest_age_hours)}</td>
                  <td>
                    <code>{t.current_hash || "—"}</code>
                    {t.stale && <span className={classes.staleTag}> stale</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {visibleUncached.length > 0 && (
        <div className={classes.tableWrap}>
          <h3>Uncached by design ({visibleUncached.length})</h3>
          <table className={classes.table}>
            <thead>
              <tr>
                <th>Path</th>
                <th>Method</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {visibleUncached.map((u) => (
                <tr key={`${u.method}-${u.path}`}>
                  <td><code>{u.path}</code></td>
                  <td>{u.method}</td>
                  <td>{REASON_LABEL[u.reason] ?? u.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data.disk_orphan_files.length > 0 && (
        <div className={classes.orphanBox}>
          <h3>Orphan disk files ({data.disk_orphan_files.length})</h3>
          <p>Files on disk with no matching endpoint — safe to delete via admin.</p>
          <ul>
            {data.disk_orphan_files.map((f) => <li key={f}><code>{f}</code></li>)}
          </ul>
        </div>
      )}

      {data.summary.baseline_seeded_at && (
        <p className={classes.seeded}>
          Baseline seeded at {data.summary.baseline_seeded_at}
        </p>
      )}
    </section>
  );
};

export default CacheStatusPanel;
