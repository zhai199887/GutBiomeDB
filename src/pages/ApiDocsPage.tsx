import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import Header from "@/sections/Header";
import Footer from "@/sections/Footer";
import { API_DOC_CATEGORIES, API_DOC_ENDPOINTS } from "./apiDocs/endpoints";
import type { ApiEndpoint } from "./apiDocs/types";
import css from "./ApiDocsPage.module.css";

const LOCAL_API_BASE = "http://localhost:8000";
const PUBLIC_API_BASE = "https://32c774a.r12.cpolar.top";
const ACTIVE_API_BASE = import.meta.env.VITE_API_URL ?? LOCAL_API_BASE;

type LocaleCopy = {
  back: string;
  title: string;
  subtitle: string;
  note: string;
  localApi: string;
  publicApi: string;
  activeApi: string;
  openapi: string;
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
  publicWarning: string;
};

const COPY: Record<"en" | "zh", LocaleCopy> = {
  en: {
    back: "Back to Home",
    title: "API Documentation",
    subtitle: "Human-readable API workspace with executable examples for local and public environments.",
    note: "The public API URL is an operations endpoint and may change. Use localhost or /api/openapi.json for stable development references.",
    localApi: "Local API",
    publicApi: "Public API",
    activeApi: "Active Base URL",
    openapi: "OpenAPI Spec",
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
    overviewTitle: "Research-Oriented API Scope",
    overviewText:
      "This documentation emphasizes reproducible analysis workflows: project browsing, disease-centric statistics, co-occurrence networks, weighted GMHI scoring, and export endpoints for aggregated results.",
    errorTitle: "Error Handling",
    errorText:
      "Common response codes: 400 for invalid biological filters or insufficient samples, 404 for missing genus/project resources, 422 for malformed request bodies, 429 for rate limiting, and 500 for server-side calculation failures.",
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
    publicWarning: "Public API is suitable for demos, not for permanent manuscript citations.",
  },
  zh: {
    back: "返回首页",
    title: "API 文档",
    subtitle: "面向科研使用场景的人类可读 API 工作台，支持本地和公网双环境示例。",
    note: "公网 API 地址属于运维入口，可能变化。稳定开发引用请优先使用 localhost 或 /api/openapi.json。",
    localApi: "本地 API",
    publicApi: "公网 API",
    activeApi: "当前基地址",
    openapi: "OpenAPI 规范",
    categories: {
      all: "全部",
      overview: "总览",
      species: "物种检索",
      disease: "疾病",
      network: "网络",
      analysis: "分析",
      similarity: "相似性",
      studies: "研究项目",
      download: "下载",
    },
    overviewTitle: "科研导向接口范围",
    overviewText:
      "这份文档按真实科研工作流组织接口：项目浏览、疾病统计、共现网络、加权 GMHI，以及聚合分析结果下载，而不是只按技术层级堆接口。",
    errorTitle: "错误处理",
    errorText:
      "常见响应码：400 表示筛选条件无效或样本不足，404 表示属名或项目不存在，422 表示请求体格式错误，429 表示触发限流，500 表示服务端计算失败。",
    endpointTitle: "接口列表",
    params: "参数",
    requestBody: "请求体",
    responseSchema: "响应结构",
    errorHandling: "错误处理",
    codeExamples: "代码示例",
    requestPreview: "请求预览",
    tryIt: "立即试跑",
    response: "响应结果",
    responseMeta: "响应元信息",
    copy: "复制",
    copied: "已复制",
    python: "Python",
    r: "R",
    curl: "cURL",
    status: "状态",
    duration: "耗时",
    size: "大小",
    bodyPlaceholder: "可编辑 JSON 请求体",
    invalidJson: "请求体 JSON 无法解析，请先修正后再重试。",
    publicWarning: "公网 API 适合演示，不适合作为长期论文引用地址。",
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
      <Header />
      <main className={css.page}>
        <Link to="/" className={css.back}>
          {text.back}
        </Link>

        <header className={css.hero}>
          <div>
            <h1 className={css.title}>{text.title}</h1>
            <p className={css.subtitle}>{text.subtitle}</p>
          </div>
          <div className={css.badges}>
            <a className={css.badge} href={`${ACTIVE_API_BASE}/api/openapi.json`} target="_blank" rel="noreferrer">
              {text.openapi}
            </a>
            <a className={css.badge} href={`${ACTIVE_API_BASE}/api/docs`} target="_blank" rel="noreferrer">
              Swagger UI
            </a>
          </div>
        </header>

        <section className={css.warning}>
          <p>{text.note}</p>
          <p>{text.publicWarning}</p>
        </section>

        <section className={css.infoCards}>
          <div className={css.infoCard}>
            <h3>{text.localApi}</h3>
            <code>{LOCAL_API_BASE}</code>
          </div>
          <div className={css.infoCard}>
            <h3>{text.publicApi}</h3>
            <code>{PUBLIC_API_BASE}</code>
          </div>
          <div className={css.infoCard}>
            <h3>{text.activeApi}</h3>
            <code>{ACTIVE_API_BASE}</code>
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

                  {isOpen && (
                    <div className={css.endpointBody}>
                      <p className={css.description}>{endpoint.description}</p>

                      {endpoint.params && endpoint.params.length > 0 && (
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
                      )}

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

                      {endpoint.defaultBody && (
                        <div className={css.block}>
                          <h3>{text.requestBody}</h3>
                          <textarea
                            className={css.textarea}
                            spellCheck={false}
                            aria-label={text.bodyPlaceholder}
                            value={bodyDraft}
                            onChange={(event) =>
                              setBodyDrafts((prev) => ({ ...prev, [key]: event.target.value }))
                            }
                          />
                        </div>
                      )}

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

                      {responseMeta[key] && (
                        <div className={css.block}>
                          <h3>{text.responseMeta}</h3>
                          <div className={css.metaRow}>
                            <span>{text.status}: {responseMeta[key].status}</span>
                            <span>{text.duration}: {responseMeta[key].durationMs} ms</span>
                            <span>{text.size}: {formatBytes(responseMeta[key].bytes)}</span>
                          </div>
                        </div>
                      )}

                      {responses[key] && (
                        <div className={css.block}>
                          <h3>{text.response}</h3>
                          <pre className={css.responseBlock}>{responses[key]}</pre>
                        </div>
                      )}
                    </div>
                  )}
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
