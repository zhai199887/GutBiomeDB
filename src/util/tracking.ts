/**
 * tracking.ts — Usage analytics (fire-and-forget).
 * Records page views, analysis runs, exports, and searches in production.
 */
import { API_BASE } from "@/util/apiBase";

export function trackEvent(event: string, page?: string, detail?: string) {
  if (import.meta.env.DEV) return;
  fetch(`${API_BASE}/api/track`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event,
      page: page ?? (typeof location !== "undefined" ? location.pathname : ""),
      detail: detail ?? "",
    }),
  }).catch(() => {});
}
