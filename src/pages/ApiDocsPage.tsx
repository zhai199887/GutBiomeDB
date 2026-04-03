import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import css from "./ApiDocsPage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

/* ------------------------------------------------------------------ */
/*  Endpoint definitions                                               */
/* ------------------------------------------------------------------ */

type Param = { name: string; type: string; required: boolean; description: string };

interface Endpoint {
  method: "GET" | "POST";
  path: string;
  category: string;
  summary: string;
  description: string;
  params?: Param[];
  body?: Record<string, unknown>;
  python: (base: string) => string;
  r: (base: string) => string;
  curl: (base: string) => string;
}

const ENDPOINTS: Endpoint[] = [
  // 1. GET /api/health
  {
    method: "GET",
    path: "/api/health",
    category: "overview",
    summary: "Health check",
    description: "Returns server health status and uptime information.",
    python: (b) => `import requests\n\nres = requests.get("${b}/api/health")\nprint(res.json())`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/health")\ncat(content(res, "text"), "\\n")`,
    curl: (b) => `curl -s "${b}/api/health" | python -m json.tool`,
  },
  // 2. GET /api/data-stats
  {
    method: "GET",
    path: "/api/data-stats",
    category: "overview",
    summary: "Dataset statistics",
    description: "Returns high-level statistics: total samples, countries, diseases, genera counts.",
    python: (b) => `import requests\n\nres = requests.get("${b}/api/data-stats")\nstats = res.json()\nprint(f"Samples: {stats['total_samples']}")`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/data-stats")\nstats <- fromJSON(content(res, "text"))\ncat("Samples:", stats$total_samples, "\\n")`,
    curl: (b) => `curl -s "${b}/api/data-stats" | python -m json.tool`,
  },
  // 3. GET /api/species-search
  {
    method: "GET",
    path: "/api/species-search",
    category: "species",
    summary: "Search species / genera",
    description: "Full-text search across genus names. Returns matching genera with basic stats.",
    params: [
      { name: "q", type: "string", required: true, description: "Search query (e.g. 'Blautia')" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/species-search", params={"q": "Blautia"})\nfor g in res.json()["results"]:\n    print(g)`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/species-search", query = list(q = "Blautia"))\ndata <- fromJSON(content(res, "text"))\nprint(data$results)`,
    curl: (b) => `curl -s "${b}/api/species-search?q=Blautia" | python -m json.tool`,
  },
  // 4. GET /api/species-profile
  {
    method: "GET",
    path: "/api/species-profile",
    category: "species",
    summary: "Species / genus profile",
    description: "Detailed profile for a single genus: abundance by disease, country, age, and sex.",
    params: [
      { name: "genus", type: "string", required: true, description: "Genus name (e.g. 'Bacteroides')" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/species-profile", params={"genus": "Bacteroides"})\nprofile = res.json()\nprint(f"Prevalence: {profile['prevalence']}")\nprint(f"Mean abundance: {profile['mean_abundance']}")`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/species-profile", query = list(genus = "Bacteroides"))\nprofile <- fromJSON(content(res, "text"))\ncat("Prevalence:", profile$prevalence, "\\n")`,
    curl: (b) => `curl -s "${b}/api/species-profile?genus=Bacteroides" | python -m json.tool`,
  },
  // 5. GET /api/disease-list
  {
    method: "GET",
    path: "/api/disease-list",
    category: "disease",
    summary: "List all diseases",
    description: "Returns the full list of diseases with sample counts.",
    python: (b) => `import requests\n\nres = requests.get("${b}/api/disease-list")\nfor d in res.json()["diseases"]:\n    print(d["name"], d["sample_count"])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/disease-list")\ndata <- fromJSON(content(res, "text"))\nprint(head(data$diseases))`,
    curl: (b) => `curl -s "${b}/api/disease-list" | python -m json.tool`,
  },
  // 6. GET /api/disease-profile
  {
    method: "GET",
    path: "/api/disease-profile",
    category: "disease",
    summary: "Disease microbiome profile",
    description: "Top genera for a disease vs healthy controls, with fold-change and prevalence.",
    params: [
      { name: "disease", type: "string", required: true, description: "Disease name (e.g. 'IBD')" },
      { name: "top_n", type: "integer", required: false, description: "Number of top genera to return (default 20)" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/disease-profile", params={"disease": "IBD", "top_n": 10})\nfor g in res.json()["top_genera"]:\n    print(g["genus"], g["log2fc"])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/disease-profile", query = list(disease = "IBD", top_n = 10))\nprofile <- fromJSON(content(res, "text"))\nprint(profile$top_genera)`,
    curl: (b) => `curl -s "${b}/api/disease-profile?disease=IBD&top_n=10" | python -m json.tool`,
  },
  // 7. GET /api/biomarker-discovery
  {
    method: "GET",
    path: "/api/biomarker-discovery",
    category: "disease",
    summary: "Biomarker discovery",
    description: "Wilcoxon rank-sum test with BH FDR correction for disease biomarker identification.",
    params: [
      { name: "disease", type: "string", required: true, description: "Disease name" },
      { name: "lda_threshold", type: "number", required: false, description: "LDA effect size threshold (default 2.0)" },
      { name: "p_threshold", type: "number", required: false, description: "Adjusted p-value threshold (default 0.05)" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/biomarker-discovery", params={\n    "disease": "CRC",\n    "lda_threshold": 2.0,\n    "p_threshold": 0.05\n})\nmarkers = res.json()["markers"]\nfor m in markers:\n    print(m["taxon"], m["lda_score"], m["p_adj"])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/biomarker-discovery",\n  query = list(disease = "CRC", lda_threshold = 2.0, p_threshold = 0.05))\nmarkers <- fromJSON(content(res, "text"))$markers\nprint(markers)`,
    curl: (b) => `curl -s "${b}/api/biomarker-discovery?disease=CRC&lda_threshold=2.0&p_threshold=0.05" | python -m json.tool`,
  },
  // 8. POST /api/diff-analysis
  {
    method: "POST",
    path: "/api/diff-analysis",
    category: "analysis",
    summary: "Differential analysis between two groups",
    description: "Compare microbiome composition between two user-defined sample groups. Returns differential abundance, volcano plot data, and diversity metrics.",
    body: {
      group_a_filter: { disease: "IBD", country: "USA" },
      group_b_filter: { disease: "healthy" },
      taxonomy_level: "genus",
      method: "wilcoxon",
    },
    python: (b) => `import requests\n\npayload = {\n    "group_a_filter": {"disease": "IBD", "country": "USA"},\n    "group_b_filter": {"disease": "healthy"},\n    "taxonomy_level": "genus",\n    "method": "wilcoxon"\n}\nres = requests.post("${b}/api/diff-analysis", json=payload)\nprint(res.json()["significant_taxa"][:5])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\npayload <- list(\n  group_a_filter = list(disease = "IBD", country = "USA"),\n  group_b_filter = list(disease = "healthy"),\n  taxonomy_level = "genus",\n  method = "wilcoxon"\n)\nres <- POST("${b}/api/diff-analysis",\n  body = toJSON(payload, auto_unbox = TRUE),\n  content_type_json())\nresult <- fromJSON(content(res, "text"))\nprint(head(result$significant_taxa))`,
    curl: (b) => `curl -s -X POST "${b}/api/diff-analysis" \\\n  -H "Content-Type: application/json" \\\n  -d '{"group_a_filter":{"disease":"IBD","country":"USA"},"group_b_filter":{"disease":"healthy"},"taxonomy_level":"genus","method":"wilcoxon"}' \\\n  | python -m json.tool`,
  },
  // 9. GET /api/cooccurrence
  {
    method: "GET",
    path: "/api/cooccurrence",
    category: "network",
    summary: "Co-occurrence network",
    description: "Spearman correlation-based co-occurrence network for a given disease context.",
    params: [
      { name: "disease", type: "string", required: false, description: "Disease context (default 'healthy')" },
      { name: "min_r", type: "number", required: false, description: "Minimum |r| threshold (default 0.3)" },
      { name: "top_genera", type: "integer", required: false, description: "Number of top genera (default 30)" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/cooccurrence", params={\n    "disease": "CRC",\n    "min_r": 0.4,\n    "top_genera": 20\n})\nnetwork = res.json()\nprint(f"Nodes: {len(network['nodes'])}, Edges: {len(network['edges'])}")`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/cooccurrence",\n  query = list(disease = "CRC", min_r = 0.4, top_genera = 20))\nnet <- fromJSON(content(res, "text"))\ncat("Nodes:", length(net$nodes), "Edges:", length(net$edges), "\\n")`,
    curl: (b) => `curl -s "${b}/api/cooccurrence?disease=CRC&min_r=0.4&top_genera=20" | python -m json.tool`,
  },
  // 10. GET /api/lifecycle
  {
    method: "GET",
    path: "/api/lifecycle",
    category: "lifecycle",
    summary: "Lifecycle microbiome data",
    description: "Gut microbiome composition across 8 life stages, optionally filtered by disease or country.",
    params: [
      { name: "disease", type: "string", required: false, description: "Filter by disease (default all healthy)" },
      { name: "country", type: "string", required: false, description: "Filter by country" },
      { name: "top_genera", type: "integer", required: false, description: "Number of top genera (default 15)" },
    ],
    python: (b) => `import requests\n\nres = requests.get("${b}/api/lifecycle", params={"top_genera": 10})\ndata = res.json()\nprint(f"Total samples: {data['total_samples']}")\nfor row in data["data"]:\n    print(row["age_group"], row["sample_count"])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\nres <- GET("${b}/api/lifecycle", query = list(top_genera = 10))\ndata <- fromJSON(content(res, "text"))\nprint(data$data)`,
    curl: (b) => `curl -s "${b}/api/lifecycle?top_genera=10" | python -m json.tool`,
  },
  // 11. POST /api/similarity-search
  {
    method: "POST",
    path: "/api/similarity-search",
    category: "similarity",
    summary: "Sample similarity search",
    description: "Upload abundance profile to find the most similar samples in the database.",
    body: {
      abundances: { Bacteroides: 0.25, Prevotella: 0.15, Faecalibacterium: 0.10 },
      metric: "braycurtis",
      top_k: 10,
    },
    python: (b) => `import requests\n\npayload = {\n    "abundances": {"Bacteroides": 0.25, "Prevotella": 0.15, "Faecalibacterium": 0.10},\n    "metric": "braycurtis",\n    "top_k": 10\n}\nres = requests.post("${b}/api/similarity-search", json=payload)\nfor hit in res.json()["results"]:\n    print(hit["sample_key"], hit["distance"])`,
    r: (b) => `library(httr)\nlibrary(jsonlite)\n\npayload <- list(\n  abundances = list(Bacteroides = 0.25, Prevotella = 0.15, Faecalibacterium = 0.10),\n  metric = "braycurtis",\n  top_k = 10\n)\nres <- POST("${b}/api/similarity-search",\n  body = toJSON(payload, auto_unbox = TRUE),\n  content_type_json())\nresults <- fromJSON(content(res, "text"))$results\nprint(results)`,
    curl: (b) => `curl -s -X POST "${b}/api/similarity-search" \\\n  -H "Content-Type: application/json" \\\n  -d '{"abundances":{"Bacteroides":0.25,"Prevotella":0.15,"Faecalibacterium":0.10},"metric":"braycurtis","top_k":10}' \\\n  | python -m json.tool`,
  },
  // 12. GET /api/download/summary-stats
  {
    method: "GET",
    path: "/api/download/summary-stats",
    category: "download",
    summary: "Download summary statistics",
    description: "Download aggregated summary statistics in CSV, TSV, or JSON format.",
    params: [
      { name: "format", type: "string", required: false, description: "Output format: csv, tsv, or json (default json)" },
    ],
    python: (b) => `import requests\n\n# Download as CSV\nres = requests.get("${b}/api/download/summary-stats", params={"format": "csv"})\nwith open("summary_stats.csv", "w") as f:\n    f.write(res.text)\nprint("Saved summary_stats.csv")`,
    r: (b) => `library(httr)\n\nres <- GET("${b}/api/download/summary-stats", query = list(format = "csv"))\nwriteLines(content(res, "text"), "summary_stats.csv")\ncat("Saved summary_stats.csv\\n")`,
    curl: (b) => `curl -s "${b}/api/download/summary-stats?format=csv" -o summary_stats.csv`,
  },
];

const CATEGORIES = [
  "all",
  "overview",
  "species",
  "disease",
  "analysis",
  "network",
  "lifecycle",
  "similarity",
  "download",
] as const;

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

const ApiDocsPage = () => {
  const { t } = useI18n();
  const [activeCat, setActiveCat] = useState<string>("all");
  const [openIdx, setOpenIdx] = useState<number | null>(null);
  const [codeTabs, setCodeTabs] = useState<Record<number, string>>({});
  const [copied, setCopied] = useState<number | null>(null);
  const [tryLoading, setTryLoading] = useState<number | null>(null);
  const [responses, setResponses] = useState<Record<number, string>>({});

  const filtered =
    activeCat === "all"
      ? ENDPOINTS
      : ENDPOINTS.filter((ep) => ep.category === activeCat);

  const toggle = (i: number) => setOpenIdx(openIdx === i ? null : i);

  const getCodeTab = (i: number) => codeTabs[i] ?? "python";

  const setTab = (i: number, tab: string) =>
    setCodeTabs((prev) => ({ ...prev, [i]: tab }));

  const copyCode = useCallback(
    (i: number, code: string) => {
      navigator.clipboard.writeText(code).then(() => {
        setCopied(i);
        setTimeout(() => setCopied(null), 1500);
      });
    },
    [],
  );

  const tryEndpoint = useCallback(
    async (i: number, ep: Endpoint) => {
      setTryLoading(i);
      try {
        const url = `${API_BASE}${ep.path}`;
        const opts: RequestInit =
          ep.method === "POST"
            ? {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(ep.body),
              }
            : {};
        const res = await fetch(url, opts);
        const text = await res.text();
        try {
          const json = JSON.parse(text);
          setResponses((prev) => ({
            ...prev,
            [i]: JSON.stringify(json, null, 2),
          }));
        } catch {
          setResponses((prev) => ({ ...prev, [i]: text }));
        }
      } catch (err: unknown) {
        setResponses((prev) => ({
          ...prev,
          [i]: `Error: ${err instanceof Error ? err.message : String(err)}`,
        }));
      } finally {
        setTryLoading(null);
      }
    },
    [],
  );

  const getCode = (ep: Endpoint, tab: string): string => {
    if (tab === "r") return ep.r(API_BASE);
    if (tab === "curl") return ep.curl(API_BASE);
    return ep.python(API_BASE);
  };

  return (
    <div className={css.page}>
      {/* Back link */}
      <Link to="/" className={css.back}>
        {t("apiDocs.back")}
      </Link>

      {/* Title */}
      <h1 className={css.title}>{t("apiDocs.title")}</h1>
      <p className={css.subtitle}>{t("apiDocs.subtitle")}</p>

      {/* Info cards */}
      <div className={css.infoCards}>
        <div className={css.infoCard}>
          <h4>{t("apiDocs.baseUrl")}</h4>
          <p>{API_BASE}</p>
        </div>
        <div className={css.infoCard}>
          <h4>{t("apiDocs.version")}</h4>
          <p>v1</p>
        </div>
        <div className={css.infoCard}>
          <h4>{t("apiDocs.rateLimit")}</h4>
          <p>{t("apiDocs.rateLimitDesc")}</p>
        </div>
        <div className={css.infoCard}>
          <h4>Interactive Docs</h4>
          <p>
            <a href={`${API_BASE}/api/docs`} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)", textDecoration: "underline" }}>Swagger UI</a>
            {" · "}
            <a href={`${API_BASE}/api/redoc`} target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)", textDecoration: "underline" }}>ReDoc</a>
          </p>
        </div>
      </div>

      {/* Overview section */}
      <div className={css.section}>
        <h2>{t("apiDocs.overview")}</h2>
        <p>{t("apiDocs.overviewText")}</p>
      </div>

      {/* Authentication section */}
      <div className={css.section}>
        <h2>{t("apiDocs.auth")}</h2>
        <p>{t("apiDocs.authText")}</p>
      </div>

      {/* Endpoints section */}
      <div className={css.section}>
        <h2>{t("apiDocs.endpoints")}</h2>

        {/* Category filter */}
        <div className={css.categories}>
          {CATEGORIES.map((cat) => (
            <button
              key={cat}
              className={activeCat === cat ? css.catBtnActive : css.catBtn}
              onClick={() => setActiveCat(cat)}
            >
              {cat === "all"
                ? "All"
                : t(`apiDocs.category.${cat}` as any)}
            </button>
          ))}
        </div>

        {/* Endpoint list */}
        {filtered.map((ep, fi) => {
          const globalIdx = ENDPOINTS.indexOf(ep);
          const isOpen = openIdx === globalIdx;
          const tab = getCodeTab(globalIdx);
          const code = getCode(ep, tab);

          return (
            <div key={ep.path + ep.method} className={css.endpoint}>
              {/* Header row */}
              <div
                className={css.endpointHeader}
                onClick={() => toggle(globalIdx)}
              >
                <span
                  className={
                    ep.method === "GET" ? css.methodGet : css.methodPost
                  }
                >
                  {ep.method}
                </span>
                <span className={css.path}>{ep.path}</span>
                <span className={css.desc}>{ep.summary}</span>
                <span className={isOpen ? css.arrowOpen : css.arrow}>
                  &#9654;
                </span>
              </div>

              {/* Expanded body */}
              {isOpen && (
                <div className={css.endpointBody}>
                  <p>{ep.description}</p>

                  {/* Parameters table */}
                  {ep.params && ep.params.length > 0 && (
                    <div className={css.params}>
                      <h4>{t("apiDocs.parameters")}</h4>
                      <table className={css.paramTable}>
                        <thead>
                          <tr>
                            <th>Name</th>
                            <th>Type</th>
                            <th>Required</th>
                            <th>Description</th>
                          </tr>
                        </thead>
                        <tbody>
                          {ep.params.map((p) => (
                            <tr key={p.name}>
                              <td>
                                <code>{p.name}</code>
                              </td>
                              <td>{p.type}</td>
                              <td>{p.required ? "Yes" : "No"}</td>
                              <td>{p.description}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Request body example for POST */}
                  {ep.body && (
                    <div className={css.params}>
                      <h4>Request Body</h4>
                      <div className={css.codeBlock}>
                        {JSON.stringify(ep.body, null, 2)}
                      </div>
                    </div>
                  )}

                  {/* Code examples with tabs */}
                  <div className={css.codeTabs}>
                    {(["python", "r", "curl"] as const).map((lang) => (
                      <button
                        key={lang}
                        className={
                          tab === lang ? css.codeTabActive : css.codeTab
                        }
                        onClick={() => setTab(globalIdx, lang)}
                      >
                        {t(
                          `apiDocs.${lang}` as "apiDocs.python" | "apiDocs.r" | "apiDocs.curl",
                        )}
                      </button>
                    ))}
                  </div>

                  <div className={css.codeBlock}>
                    {code}
                    <button
                      className={css.copyBtn}
                      onClick={() => copyCode(globalIdx, code)}
                    >
                      {copied === globalIdx
                        ? t("apiDocs.copied")
                        : t("apiDocs.copy")}
                    </button>
                  </div>

                  {/* Try it button */}
                  <button
                    className={css.tryBtn}
                    disabled={tryLoading === globalIdx}
                    onClick={() => tryEndpoint(globalIdx, ep)}
                  >
                    {tryLoading === globalIdx
                      ? "..."
                      : t("apiDocs.tryIt")}
                  </button>

                  {/* Response */}
                  {responses[globalIdx] !== undefined && (
                    <div>
                      <h4 style={{ marginTop: "0.75rem", fontSize: "0.9rem" }}>
                        {t("apiDocs.response")}
                      </h4>
                      <div className={css.responseBlock}>
                        {responses[globalIdx]}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default ApiDocsPage;
