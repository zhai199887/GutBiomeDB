export const LOCAL_API_BASE = "http://localhost:8000";
export const PUBLIC_API_BASE = "https://32c774a.r12.cpolar.top";
const LOCAL_API_PORT = "8000";
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1"]);

const stripTrailingSlash = (value: string) => value.replace(/\/+$/, "");

function rewriteLocalConfiguredBase(value: string): string {
  if (typeof window === "undefined") {
    return stripTrailingSlash(value);
  }

  const currentHost = window.location.hostname;
  if (!LOCAL_HOSTS.has(currentHost)) {
    return stripTrailingSlash(value);
  }

  try {
    const url = new URL(value);
    if (LOCAL_HOSTS.has(url.hostname) && url.hostname !== currentHost) {
      url.hostname = currentHost;
      if (!url.port) {
        url.port = LOCAL_API_PORT;
      }
      return stripTrailingSlash(url.toString());
    }
  } catch {
    return stripTrailingSlash(value);
  }

  return stripTrailingSlash(value);
}

export function resolveApiBase(): string {
  if (typeof window !== "undefined") {
    const host = window.location.hostname;
    if (LOCAL_HOSTS.has(host)) {
      const configured = import.meta.env.VITE_API_URL?.trim();
      if (configured) return rewriteLocalConfiguredBase(configured);
      return `${window.location.protocol}//${host}:${LOCAL_API_PORT}`;
    }

    // Production and preview deployments should talk to the same-origin /api proxy.
    return stripTrailingSlash(window.location.origin);
  }

  const configured = import.meta.env.VITE_API_URL?.trim();
  if (configured) return stripTrailingSlash(configured);

  return PUBLIC_API_BASE;
}

export const API_BASE = resolveApiBase();
