import { useMemo, useState } from "react";

import SubpageHeader from "@/components/SubpageHeader";
import { useI18n } from "@/i18n";
import Footer from "@/sections/Footer";
import { resolveApiBase } from "@/util/apiBase";

import { API_DOC_CATEGORIES, API_DOC_ENDPOINTS } from "./apiDocs/endpoints";
import type { ApiEndpoint } from "./apiDocs/types";
import css from "./ApiDocsPage.module.css";

const ACTIVE_API_BASE = resolveApiBase();
const ACTIVE_API_ROOT = new URL("/api", `${ACTIVE_API_BASE}/`).toString().replace(/\/$/, "");
const ACTIVE_OPENAPI_SPEC = new URL("/api/openapi.json", `${ACTIVE_API_BASE}/`).toString();
const ACTIVE_SWAGGER_UI = new URL("/api/docs", `${ACTIVE_API_BASE}/`).toString();

type LocaleCopy = {
  title: string;
  subtitle: string;
  apiBase: string;
  openapi: string;
  swagger: string;
  categories: Record<(typeof API_DOC_CATEGORIES)[number], string>;
  overviewTitle: string;
  overviewText: string;
  errorTitle: string;
  errorText: string;
  endpointTitle: string;
  params: string;
  requestBody: string;
  responseSchema: string;
  errorHandling: string;
  codeExamples: string;
  requestPreview: string;
  tryIt: string;
  response: string;
  responseMeta: string;
  copy: string;
  copied: string;
  python: string;
  r: string;
  curl: string;
  status: string;
  duration: string;
  size: string;
  bodyPlaceholder: string;
  invalidJson: string;
};

const COPY: Record<"en" | "zh", LocaleCopy> = {
  en: {
    title: "API Documentation",
    subtitle: "Interactive endpoint reference for database queries, analysis workflows, and reproducible programmatic access.",
    apiBase: "API base",
    openapi: "OpenAPI Spec",
    swagger: "Swagger UI",
    categories: {
      all: "All",
      overview: "Overview",
      species: "Species",
      disease: "Disease",
      network: "Network",
      analysis: "Analysis",
      similarity: "Similarity",
      studies: "Studies",
      download: "Download",
    },
    overviewTitle: "API Scope",
    overviewText:
      "The current API covers project browsing, disease and genus statistics, differential analysis, network relationships, similarity search, and aggregated result export for downstream scripting and integration.",
    errorTitle: "Error Handling",
    errorText:
      "Common response codes include 400 for invalid filters or insufficient samples, 404 for missing resources, 422 for malformed request bodies, 429 for rate limiting, and 500 for server-side calculation failures.",
    endpointTitle: "Endpoints",
    params: "Parameters",
    requestBody: "Request Body",
    responseSchema: "Response Schema",
    errorHandling: "Error Handling",
    codeExamples: "Code Examples",
    requestPreview: "Request Preview",
    tryIt: "Try It",
    response: "Response",
    responseMeta: "Response Meta",
    copy: "Copy",
    copied: "Copied",
    python: "Python",
    r: "R",
    curl: "cURL",
    status: "Status",
    duration: "Time",
    size: "Payload",
    bodyPlaceholder: "Editable JSON body",
    invalidJson: "Invalid JSON body. Fix the request body before retrying.",
  },
  zh: {
    title: "API 文档",
    subtitle: "面向图谱检索、分析工作流和程序化复用的交互式接口参考。",
    apiBase: "API 基址",
    openapi: "OpenAPI 规范",
    swagger: "Swagger UI",
    categories: {
      all: "全部",
      overview: "总览",
      species: "菌属检索",
      disease: "疾病",
      network: "网络",
      analysis: "分析",
      similarity: "相似性",
      studies: "研究项目",
      download: "下载",
    },
    overviewTitle: "接口覆盖范围",
    overviewText:
      "当前 API 覆盖研究项目浏览、疾病与菌属统计、差异分析、网络关系、相似性检索以及聚合结果导出，方便将平台工作流接入脚本与外部应用。",
    errorTitle: "错误处理",
    errorText:
      "常见响应码包括 400（筛选条件无效或样本不足）、404（请求资源不存在）、422（请求体格式错误）、429（触发限流）和 500（服务端计算失败）。",
    endpointTitle: "接口列表",
    params: "参数",
    requestBody: "请求体",
    responseSchema: "响应结构",
    errorHandling: "错误处理",
    codeExamples: "代码示例",
    requestPreview: "请求预览",
    tryIt: "立即试跑",
    response: "响应结果",
    responseMeta: "响应信息",
    copy: "复制",
    copied: "已复制",
    python: "Python",
    r: "R",
    curl: "cURL",
    status: "状态",
    duration: "耗时",
    size: "大小",
    bodyPlaceholder: "可编辑 JSON 请求体",
    invalidJson: "请求体 JSON 无法解析，请修正后再重试。",
  },
};

const COMMON_ERROR_TEXT: Record<number, string> = {
  400: "Invalid filters or insufficient samples",
  404: "Requested resource not found",
  422: "Malformed request payload",
  429: "Rate limit exceeded",
  500: "Server-side calculation failure",
};

function buildUrl(base: string, endpoint: ApiEndpoint): string {
  const url = new URL(endpoint.path, `${base}/`);
  if (endpoint.defaultQuery) {
    for (const [key, value] of Object.entries(endpoint.defaultQuery)) {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function buildPythonExample(base: string, endpoint: ApiEndpoint): string {
  const url = buildUrl(base, endpoint);
  if (endpoint.method === "GET") {
    return [
      "import requests",
      "",
      `res = requests.get("${url}")`,
      "res.raise_for_status()",
      "print(res.json())",
    ].join("\n");
  }

  return [
    "import requests",
    "",
    `payload = ${JSON.stringify(endpoint.defaultBody ?? {}, null, 2)}`,
    `res = requests.post("${url}", json=payload)`,
    "res.raise_for_status()",
    "print(res.json())",
  ].join("\n");
}

function toRList(value: unknown): string {
  if (Array.isArray(value)) {
    return `c(${value.map((item) => toRList(item)).join(", ")})`;
  }
  if (value && typeof value === "object") {
    const pairs = Object.entries(value as Record<string, unknown>)
      .map(([key, inner]) => `${key} = ${toRList(inner)}`)
      .join(", ");
    return `list(${pairs})`;
  }
  if (typeof value === "string") return `"${value}"`;
  return String(value);
}

function buildRExample(base: string, endpoint: ApiEndpoint): string {
  const url = buildUrl(base, endpoint);
  if (endpoint.method === "GET") {
    return [
      "library(httr)",
      "library(jsonlite)",
      "",
      `res <- GET("${url}")`,
      "stop_for_status(res)",
      'cat(content(res, "text", encoding = "UTF-8"))',
    ].join("\n");
  }

  return [
    "library(httr)",
    "library(jsonlite)",
    "",
    `payload <- ${toRList(endpoint.defaultBody ?? {})}`,
    `res <- POST("${url}", body = toJSON(payload, auto_unbox = TRUE), content_type_json())`,
    "stop_for_status(res)",
    'cat(content(res, "text", encoding = "UTF-8"))',
  ].join("\n");
}

function buildCurlExample(base: string, endpoint: ApiEndpoint): string {
  const url = buildUrl(base, endpoint);
  if (endpoint.method === "GET") {
    return `curl -s "${url}"`;
  }

  return [
    `curl -s -X POST "${url}" \\`,
    '  -H "Content-Type: application/json" \\',
    `  -d '${JSON.stringify(endpoint.defaultBody ?? {})}'`,
  ].join("\n");
}

type ResponseMeta = {
  status: number;
  durationMs: number;
  bytes: number;
};

const ApiDocsPage = () => {
  const { locale } = useI18n();
  const text = COPY[locale];
  const [activeCategory, setActiveCategory] = useState<(typeof API_DOC_CATEGORIES)[number]>("all");
  const [expandedKey, setExpandedKey] = useState<string | null>(API_DOC_ENDPOINTS[0]?.path ?? null);
  const [activeLang, setActiveLang] = useState<Record<string, "python" | "r" | "curl">>({});
  const [bodyDrafts, setBodyDrafts] = useState<Record<string, string>>({});
  const [responses, setResponses] = useState<Record<string, string>>({});
  const [responseMeta, setResponseMeta] = useState<Record<string, ResponseMeta>>({});
  const [loadingKey, setLoadingKey] = useState<string | null>(null);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const filteredEndpoints = useMemo(() => {
    if (activeCategory === "all") return API_DOC_ENDPOINTS;
    return API_DOC_ENDPOINTS.filter((endpoint) => endpoint.category === activeCategory);
  }, [activeCategory]);

  const handleCopy = async (key: string, code: string) => {
    await navigator.clipboard.writeText(code);
    setCopiedKey(key);
    window.setTimeout(() => setCopiedKey(null), 1500);
  };

  const handleTryIt = async (endpoint: ApiEndpoint) => {
    const key = `${endpoint.method}:${endpoint.path}`;
    const startedAt = performance.now();
    setLoadingKey(key);

    try {
      const requestInit: RequestInit = { method: endpoint.method };
      if (endpoint.method === "POST") {
        const rawBody = bodyDrafts[key] ?? JSON.stringify(endpoint.defaultBody ?? {}, null, 2);
        let parsed: Record<string, unknown>;
        try {
          parsed = JSON.parse(rawBody) as Record<string, unknown>;
        } catch {
          setResponses((prev) => ({ ...prev, [key]: text.invalidJson }));
          return;
        }
        requestInit.headers = { "Content-Type": "application/json" };
        requestInit.body = JSON.stringify(parsed);
      }

      const url = buildUrl(ACTIVE_API_BASE, endpoint);
      const res = await fetch(url, requestInit);
      const payload = await res.text();
      const durationMs = Math.round(performance.now() - startedAt);
      const bytes = new TextEncoder().encode(payload).length;

      let rendered = payload;
      try {
        rendered = JSON.stringify(JSON.parse(payload), null, 2);
      } catch {
        // keep original text
      }

      setResponses((prev) => ({ ...prev, [key]: rendered }));
      setResponseMeta((prev) => ({
        ...prev,
        [key]: {
          status: res.status,
          durationMs,
          bytes,
        },
      }));
    } catch (error) {
      const durationMs = Math.round(performance.now() - startedAt);
      setResponses((prev) => ({
        ...prev,
        [key]: error instanceof Error ? error.message : String(error),
      }));
      setResponseMeta((prev) => ({
        ...prev,
        [key]: {
          status: 0,
          durationMs,
          bytes: 0,
        },
      }));
    } finally {
      setLoadingKey(null);
    }
  };

  return (
    <>
      <SubpageHeader title={text.title} subtitle={text.subtitle} />
      <main className={css.page}>
        <section className={css.hero}>
          <div className={css.badges}>
            <a className={css.badge} href={ACTIVE_OPENAPI_SPEC} target="_blank" rel="noreferrer">
              {text.openapi}
            </a>
            <a className={css.badge} href={ACTIVE_SWAGGER_UI} target="_blank" rel="noreferrer">
              {text.swagger}
            </a>
          </div>
        </section>

        <section className={css.infoCards}>
          <div className={css.infoCard}>
            <h3>{text.apiBase}</h3>
            <code>{ACTIVE_API_ROOT}</code>
          </div>
          <div className={css.infoCard}>
            <h3>{text.openapi}</h3>
            <code>{ACTIVE_OPENAPI_SPEC}</code>
          </div>
          <div className={css.infoCard}>
            <h3>{text.swagger}</h3>
            <code>{ACTIVE_SWAGGER_UI}</code>
          </div>
        </section>

        <section className={css.section}>
          <h2>{text.overviewTitle}</h2>
          <p>{text.overviewText}</p>
        </section>

        <section className={css.section}>
          <h2>{text.errorTitle}</h2>
          <p>{text.errorText}</p>
          <ul className={css.errorList}>
            {Object.entries(COMMON_ERROR_TEXT).map(([code, label]) => (
              <li key={code}>
                <strong>{code}</strong>
                <span>{label}</span>
              </li>
            ))}
          </ul>
        </section>

        <section className={css.section}>
          <h2>{text.endpointTitle}</h2>
          <div className={css.categories}>
            {API_DOC_CATEGORIES.map((category) => (
              <button
                key={category}
                type="button"
                className={activeCategory === category ? css.categoryActive : css.categoryButton}
                onClick={() => setActiveCategory(category)}
              >
                {text.categories[category]}
              </button>
            ))}
          </div>

          <div className={css.endpointList}>
            {filteredEndpoints.map((endpoint) => {
              const key = `${endpoint.method}:${endpoint.path}`;
              const isOpen = expandedKey === key;
              const lang = activeLang[key] ?? "python";
              const code =
                lang === "python"
                  ? buildPythonExample(ACTIVE_API_BASE, endpoint)
                  : lang === "r"
                    ? buildRExample(ACTIVE_API_BASE, endpoint)
                    : buildCurlExample(ACTIVE_API_BASE, endpoint);
              const requestPreview = buildUrl(ACTIVE_API_BASE, endpoint);
              const bodyDraft = bodyDrafts[key] ?? JSON.stringify(endpoint.defaultBody ?? {}, null, 2);

              return (
                <article key={key} className={css.endpointCard}>
                  <button
                    type="button"
                    className={css.endpointHeader}
                    onClick={() => setExpandedKey(isOpen ? null : key)}
                  >
                    <span className={endpoint.method === "GET" ? css.methodGet : css.methodPost}>{endpoint.method}</span>
                    <span className={css.path}>{endpoint.path}</span>
                    <span className={css.summary}>{endpoint.summary}</span>
                  </button>

                  {isOpen ? (
                    <div className={css.endpointBody}>
                      <p className={css.description}>{endpoint.description}</p>

                      {endpoint.params && endpoint.params.length > 0 ? (
                        <div className={css.block}>
                          <h3>{text.params}</h3>
                          <table className={css.table}>
                            <thead>
                              <tr>
                                <th>Name</th>
                                <th>Type</th>
                                <th>Required</th>
                                <th>Description</th>
                              </tr>
                            </thead>
                            <tbody>
                              {endpoint.params.map((param) => (
                                <tr key={param.name}>
                                  <td><code>{param.name}</code></td>
                                  <td>{param.type}</td>
                                  <td>{param.required ? "Yes" : "No"}</td>
                                  <td>{param.description}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : null}

                      <div className={css.grid}>
                        <div className={css.block}>
                          <h3>{text.requestPreview}</h3>
                          <pre className={css.preview}>{requestPreview}</pre>
                        </div>

                        <div className={css.block}>
                          <h3>{text.responseSchema}</h3>
                          <ul className={css.schemaList}>
                            {endpoint.responseSchema.map((line) => (
                              <li key={line}><code>{line}</code></li>
                            ))}
                          </ul>
                        </div>
                      </div>

                      {endpoint.defaultBody ? (
                        <div className={css.block}>
                          <h3>{text.requestBody}</h3>
                          <textarea
                            className={css.textarea}
                            spellCheck={false}
                            aria-label={text.bodyPlaceholder}
                            value={bodyDraft}
                            onChange={(event) => setBodyDrafts((prev) => ({ ...prev, [key]: event.target.value }))}
                          />
                        </div>
                      ) : null}

                      <div className={css.block}>
                        <h3>{text.errorHandling}</h3>
                        <div className={css.errorBadges}>
                          {endpoint.errorCodes.map((codeValue) => (
                            <span key={codeValue} className={css.errorBadge}>
                              {codeValue} {COMMON_ERROR_TEXT[codeValue] ?? "Unhandled"}
                            </span>
                          ))}
                        </div>
                      </div>

                      <div className={css.block}>
                        <h3>{text.codeExamples}</h3>
                        <div className={css.codeTabs}>
                          {(["python", "r", "curl"] as const).map((tab) => (
                            <button
                              key={tab}
                              type="button"
                              className={lang === tab ? css.codeTabActive : css.codeTab}
                              onClick={() => setActiveLang((prev) => ({ ...prev, [key]: tab }))}
                            >
                              {text[tab]}
                            </button>
                          ))}
                        </div>

                        <div className={css.codeBlock}>
                          <pre>{code}</pre>
                          <button type="button" className={css.copyButton} onClick={() => void handleCopy(key, code)}>
                            {copiedKey === key ? text.copied : text.copy}
                          </button>
                        </div>
                      </div>

                      <div className={css.actions}>
                        <button
                          type="button"
                          className={css.tryButton}
                          disabled={loadingKey === key}
                          onClick={() => void handleTryIt(endpoint)}
                        >
                          {loadingKey === key ? "..." : text.tryIt}
                        </button>
                      </div>

                      {responseMeta[key] ? (
                        <div className={css.block}>
                          <h3>{text.responseMeta}</h3>
                          <div className={css.metaRow}>
                            <span>{text.status}: {responseMeta[key].status}</span>
                            <span>{text.duration}: {responseMeta[key].durationMs} ms</span>
                            <span>{text.size}: {formatBytes(responseMeta[key].bytes)}</span>
                          </div>
                        </div>
                      ) : null}

                      {responses[key] ? (
                        <div className={css.block}>
                          <h3>{text.response}</h3>
                          <pre className={css.responseBlock}>{responses[key]}</pre>
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </article>
              );
            })}
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
};

export default ApiDocsPage;