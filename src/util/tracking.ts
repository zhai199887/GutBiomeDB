/**
 * tracking.ts — 使用统计追踪（火即忘模式）
 * 生产环境记录页面访问、分析运行、导出、搜索等事件
 */
const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

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
