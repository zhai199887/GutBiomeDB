/**
 * MetabolismPage.tsx
 * Browse literature-curated metabolic roles without overstating pathway activity
 */
import { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { useData } from "@/data";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import "@/components/tooltip";
import CategoryDiseasePanel from "./metabolism/CategoryDiseasePanel";
import MetabolismOverviewHeatmap from "./metabolism/MetabolismOverviewHeatmap";
import type {
  CategoryProfileResult,
  MetabolismCategory,
  MetabolismMapping,
  MetabolismOverviewResult,
} from "./metabolism/types";
import classes from "./MetabolismPage.module.css";

type SortMode = "default" | "taxa_count" | "alpha";
type DetailTab = "profile" | "disease-context";

interface DiseaseListResponse {
  diseases: Array<{
    name: string;
    count?: number;
  }>;
}

interface MatchResult {
  genera: string[];
  fallback: boolean;
}

const OVERVIEW_CACHE_KEY = `${API_BASE}/api/metabolism-overview`;
const HEATMAP_DISEASE_LIMIT = 18;

const formatDiseaseLabel = (disease: string, locale: "en" | "zh", maxLen = 32) => {
  const full = diseaseDisplayNameI18n(disease, locale);
  return full.length > maxLen ? `${full.slice(0, maxLen - 3)}...` : full;
};

const normalize = (value: string) => value.trim().toLowerCase();

const matchesCategorySearch = (category: MetabolismCategory, query: string): boolean => {
  const q = normalize(query);
  if (!q) {
    return true;
  }

  const fields = [
    category.name_en,
    category.name_zh,
    ...category.taxa,
    ...(category.genus_exact_names ?? []),
    ...category.key_metabolites,
    ...category.related_pathways,
    ...(category.kegg_pathway_ids ?? []),
    ...(category.metacyc_pathway_ids ?? []),
  ];

  return fields.some((field) => normalize(field).includes(q));
};

const sortCategories = (
  categories: MetabolismCategory[],
  sortMode: SortMode,
  locale: "en" | "zh",
): MetabolismCategory[] => {
  const list = [...categories];
  if (sortMode === "taxa_count") {
    list.sort(
      (a, b) =>
        (b.genus_exact_names?.length ?? b.taxa.length) -
        (a.genus_exact_names?.length ?? a.taxa.length),
    );
    return list;
  }

  if (sortMode === "alpha") {
    list.sort((a, b) => {
      const aName = locale === "zh" ? a.name_zh : a.name_en;
      const bName = locale === "zh" ? b.name_zh : b.name_en;
      return aName.localeCompare(bName, locale === "zh" ? "zh-CN" : "en");
    });
  }

  return list;
};

const buildPathwayLinks = (category: MetabolismCategory, locale: "en" | "zh") => {
  const labels =
    locale === "zh" && category.related_pathways_zh?.length
      ? category.related_pathways_zh
      : category.related_pathways;
  return labels.map((pathway, index) => ({
    label: pathway,
    keggId: category.kegg_pathway_ids?.[index] ?? "",
    metacycId: category.metacyc_pathway_ids?.[index] ?? "",
  }));
};

const resolveCategoryGenera = (
  category: MetabolismCategory,
  abundance?: ReturnType<typeof useData.getState>["abundance"],
): MatchResult => {
  if (!abundance) {
    return { genera: [], fallback: false };
  }

  const abundanceLookup = new Map(
    abundance.genera.map((genus) => [normalize(genus), genus]),
  );

  const exactMatches = (category.genus_exact_names ?? [])
    .map((name) => abundanceLookup.get(normalize(name)) ?? "")
    .filter(Boolean);

  if (exactMatches.length > 0) {
    return { genera: Array.from(new Set(exactMatches)), fallback: false };
  }

  const fallbackMatches = abundance.genera.filter((genus) =>
    category.taxa.some((taxon) => {
      const genusLower = normalize(genus);
      const taxonLower = normalize(taxon);
      return genusLower.includes(taxonLower) || taxonLower.includes(genusLower);
    }),
  );

  return { genera: Array.from(new Set(fallbackMatches)), fallback: fallbackMatches.length > 0 };
};

const getAverageAbundance = (
  genus: string,
  abundance: NonNullable<ReturnType<typeof useData.getState>["abundance"]>,
) => {
  const values = Object.values(abundance.by_disease).map((row) => row[genus] ?? 0);
  return d3.mean(values) ?? 0;
};

const getHeatmapDiseases = (
  abundance: NonNullable<ReturnType<typeof useData.getState>["abundance"]>,
  diseaseOrder: string[],
) => {
  const available = new Set(Object.keys(abundance.by_disease));
  const ordered = diseaseOrder.filter((disease) => available.has(disease));
  const remainder = Object.keys(abundance.by_disease).filter((disease) => !ordered.includes(disease));
  return [...ordered, ...remainder].slice(0, HEATMAP_DISEASE_LIMIT);
};

const getPhylumColorScale = (abundance?: ReturnType<typeof useData.getState>["abundance"]) => {
  const phyla = Array.from(
    new Set(
      Object.values(abundance?.phylum_map ?? {}).filter(Boolean),
    ),
  );
  return d3.scaleOrdinal<string, string>(d3.schemeTableau10).domain(phyla);
};

const MetabolismPage = () => {
  const { t, locale } = useI18n();
  const abundance = useData((state) => state.abundance);
  const [mapping, setMapping] = useState<MetabolismMapping | null>(null);
  const [fetchError, setFetchError] = useState<string>("");
  const [selectedCategoryId, setSelectedCategoryId] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortMode, setSortMode] = useState<SortMode>("default");
  const [tab, setTab] = useState<DetailTab>("profile");
  const [overview, setOverview] = useState<MetabolismOverviewResult | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewError, setOverviewError] = useState("");
  const [diseaseOrder, setDiseaseOrder] = useState<string[]>([]);
  const [profileResults, setProfileResults] = useState<Record<string, CategoryProfileResult>>({});
  const [profileLoading, setProfileLoading] = useState<Record<string, boolean>>({});
  const [profileErrors, setProfileErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    fetch("/data/metabolism_mapping.json")
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        return response.json();
      })
      .then((data: MetabolismMapping) => {
        setMapping(data);
        setSelectedCategoryId(data.categories[0]?.id ?? null);
      })
      .catch((error: Error) => {
        setFetchError(error.message || "Failed to load metabolism mapping");
        setMapping({ version: "error", last_updated: "", categories: [] });
      });
  }, []);

  useEffect(() => {
    setOverviewLoading(true);
    cachedFetch<MetabolismOverviewResult>(OVERVIEW_CACHE_KEY)
      .then((data) => {
        setOverview(data);
        setOverviewError("");
      })
      .catch((error: Error) => {
        setOverviewError(error.message || "Failed to load metabolism overview");
      })
      .finally(() => {
        setOverviewLoading(false);
      });
  }, []);

  useEffect(() => {
    cachedFetch<DiseaseListResponse>(`${API_BASE}/api/disease-list`)
      .then((data) => {
        setDiseaseOrder(data.diseases.map((item) => item.name));
      })
      .catch(() => {
        setDiseaseOrder([]);
      });
  }, []);

  useEffect(() => {
    if (
      !selectedCategoryId ||
      profileResults[selectedCategoryId] ||
      profileLoading[selectedCategoryId] ||
      profileErrors[selectedCategoryId]
    ) {
      return;
    }

    setProfileLoading((prev) => ({ ...prev, [selectedCategoryId]: true }));
    cachedFetch<CategoryProfileResult>(
      `${API_BASE}/api/metabolism-category-profile?category_id=${encodeURIComponent(selectedCategoryId)}`,
    )
      .then((data) => {
        setProfileResults((prev) => ({ ...prev, [selectedCategoryId]: data }));
        setProfileErrors((prev) => ({ ...prev, [selectedCategoryId]: "" }));
      })
      .catch((error: Error) => {
        setProfileErrors((prev) => ({
          ...prev,
          [selectedCategoryId]: error.message || "Failed to load category profile",
        }));
      })
      .finally(() => {
        setProfileLoading((prev) => ({ ...prev, [selectedCategoryId]: false }));
      });
  }, [profileErrors, profileLoading, profileResults, selectedCategoryId]);

  if (!mapping) {
    return (
      <div className={classes.page}>
        <div className={classes.loading}>{t("metabolism.loading")}</div>
      </div>
    );
  }

  const visibleCategories = sortCategories(
    mapping.categories.filter((category) => matchesCategorySearch(category, search)),
    sortMode,
    locale,
  );
  const selectedCategory = selectedCategoryId
    ? mapping.categories.find((category) => category.id === selectedCategoryId) ?? null
    : null;
  const selectedProfile = selectedCategoryId ? profileResults[selectedCategoryId] : undefined;
  const selectedProfileError = selectedCategoryId ? profileErrors[selectedCategoryId] : "";
  const selectedProfileLoading = selectedCategoryId ? profileLoading[selectedCategoryId] : false;

  return (
    <div className={classes.page}>
      <div className={classes.header}>
        <Link to="/" className={classes.back}>{t("metabolism.back")}</Link>
        <h1>{t("metabolism.title")}</h1>
        <p>{t("metabolism.subtitle")}</p>
        <div className={classes.disclaimer}>
          {t("metabolism.disclaimer")}
        </div>

        <div className={classes.searchBox}>
          <input
            type="text"
            placeholder={t("metabolism.searchPlaceholder")}
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            className={classes.searchInput}
          />
        </div>

        {fetchError && (
          <div className={classes.errorBanner}>
            {t("metabolism.loadError")} <code>{fetchError}</code>
          </div>
        )}
      </div>

      <div className={classes.layout}>
        <aside className={classes.sidebar}>
          <h3 className={classes.sideTitle}>{t("metabolism.categories")}</h3>
          <div className={classes.sidebarControls}>
            <button
              type="button"
              className={classes.globalOverviewButton}
              data-active={selectedCategoryId === null}
              onClick={() => {
                setSelectedCategoryId(null);
                setTab("profile");
              }}
            >
              {t("metabolism.globalOverview")}
            </button>

            <label className={classes.sortLabel}>
              <span>{t("metabolism.sortBy")}</span>
              <select
                value={sortMode}
                onChange={(event) => setSortMode(event.target.value as SortMode)}
                className={classes.sortSelect}
              >
                <option value="default">{t("metabolism.sortDefault")}</option>
                <option value="taxa_count">{t("metabolism.sortTaxaCount")}</option>
                <option value="alpha">{t("metabolism.sortAlpha")}</option>
              </select>
            </label>
          </div>

          {visibleCategories.length === 0 && (
            <div className={classes.emptyState}>
              {t("metabolism.noCategory")} <b>{search}</b>
            </div>
          )}

          {visibleCategories.map((category) => (
            <button
              key={category.id}
              type="button"
              className={classes.categoryCard}
              data-active={selectedCategoryId === category.id}
              onClick={() => {
                setSelectedCategoryId(category.id);
                setTab("profile");
              }}
            >
              <span className={classes.catIcon}>{category.icon}</span>
              <div className={classes.catInfo}>
                <span className={classes.catName}>
                  {locale === "zh" ? category.name_zh : category.name_en}
                </span>
                <span className={classes.catCount}>
                  {(category.genus_exact_names?.length ?? category.taxa.length)} {t("metabolism.genera")}
                </span>
              </div>
            </button>
          ))}
        </aside>

        <main className={classes.detail}>
          {selectedCategory === null ? (
            <>
              {overviewLoading && (
                <div className={classes.loading}>{t("metabolism.loading")}</div>
              )}
              {overviewError && (
                <div className={classes.errorBanner}>
                  {overviewError}
                </div>
              )}
              {overview && (
                <MetabolismOverviewHeatmap
                  data={overview}
                  onSelectCategory={(categoryId) => {
                    setSelectedCategoryId(categoryId);
                    setTab("profile");
                  }}
                />
              )}
            </>
          ) : (
            <div className={classes.detailPanel}>
              <div className={classes.tabBar}>
                <button
                  type="button"
                  className={classes.tabButton}
                  data-active={tab === "profile"}
                  onClick={() => setTab("profile")}
                >
                  {t("metabolism.tabProfile")}
                </button>
                <button
                  type="button"
                  className={classes.tabButton}
                  data-active={tab === "disease-context"}
                  onClick={() => setTab("disease-context")}
                >
                  {t("metabolism.tabDiseaseContext")}
                </button>
              </div>

              {tab === "profile" ? (
                <CategoryDetail
                  category={selectedCategory}
                  abundance={abundance}
                  diseaseOrder={diseaseOrder}
                  profileResult={selectedProfile}
                  profileLoading={selectedProfileLoading}
                />
              ) : (
                <>
                  {selectedProfileLoading && (
                    <div className={classes.loading}>{t("metabolism.loading")}</div>
                  )}
                  {selectedProfileError && (
                    <div className={classes.errorBanner}>{selectedProfileError}</div>
                  )}
                  {selectedProfile && (
                    <CategoryDiseasePanel
                      category={selectedCategory}
                      result={selectedProfile}
                    />
                  )}
                </>
              )}
            </div>
          )}
        </main>
      </div>
    </div>
  );
};

const CategoryDetail = ({
  category,
  abundance,
  diseaseOrder,
  profileResult,
  profileLoading,
}: {
  category: MetabolismCategory;
  abundance?: ReturnType<typeof useData.getState>["abundance"];
  diseaseOrder: string[];
  profileResult?: CategoryProfileResult;
  profileLoading: boolean;
}) => {
  const { t, locale } = useI18n();
  const barRef = useRef<SVGSVGElement>(null);
  const heatRef = useRef<SVGSVGElement>(null);
  const matchResult = resolveCategoryGenera(category, abundance);
  const pathwayLinks = buildPathwayLinks(category, locale);

  useEffect(() => {
    if (!barRef.current || !abundance) {
      return;
    }

    const svg = d3.select(barRef.current);
    svg.selectAll("*").remove();

    if (matchResult.genera.length === 0) {
      svg.attr("viewBox", "0 0 720 72");
      svg.append("text")
        .attr("x", 12)
        .attr("y", 38)
        .attr("fill", "currentColor")
        .attr("font-size", 13)
        .text(t("metabolism.noAbundance"));
      return;
    }

    const phylumColors = getPhylumColorScale(abundance);
    const rows = matchResult.genera
      .map((genus) => ({
        genus,
        mean: getAverageAbundance(genus, abundance),
        phylum: abundance.phylum_map?.[genus] ?? "Unassigned",
      }))
      .sort((a, b) => b.mean - a.mean);

    const width = 860;
    const rowHeight = 30;
    const margin = { top: 18, right: 40, bottom: 42, left: 240 };
    const height = Math.max(180, margin.top + margin.bottom + rows.length * rowHeight);
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;

    const xScale = d3.scaleLinear()
      .domain([0, d3.max(rows, (row) => row.mean) ?? 0.01])
      .nice()
      .range([0, innerWidth]);
    const yScale = d3.scaleBand()
      .domain(rows.map((row) => row.genus))
      .range([0, innerHeight])
      .padding(0.2);

    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const chart = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    chart.selectAll(".bar")
      .data(rows)
      .join("rect")
      .attr("x", 0)
      .attr("y", (row) => yScale(row.genus) ?? 0)
      .attr("width", (row) => xScale(row.mean))
      .attr("height", yScale.bandwidth())
      .attr("rx", 5)
      .attr("fill", (row) => phylumColors(row.phylum))
      .attr("opacity", 0.88)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (row) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "属" : "Genus"}</span>
            <span>{row.genus}</span>
            <span>{locale === "zh" ? "门" : "Phylum"}</span>
            <span>{row.phylum}</span>
            <span>{locale === "zh" ? "平均丰度" : "Mean abundance"}</span>
            <span>{(row.mean * 100).toFixed(3)}%</span>
          </div>,
        )
      );

    chart.append("g")
      .call(d3.axisLeft(yScale))
      .attr("font-size", 12);
    chart.append("g")
      .attr("transform", `translate(0,${innerHeight})`)
      .call(
        d3.axisBottom(xScale)
          .ticks(5)
          .tickFormat((tick) => `${(Number(tick) * 100).toFixed(2)}%`),
      )
      .attr("font-size", 11);
  }, [abundance, locale, matchResult.genera, t]);

  useEffect(() => {
    if (!heatRef.current || !abundance) {
      return;
    }

    const svg = d3.select(heatRef.current);
    svg.selectAll("*").remove();

    const genera = matchResult.genera;
    const diseases = getHeatmapDiseases(abundance, diseaseOrder);
    if (genera.length === 0 || diseases.length === 0) {
      return;
    }

    const rootStyles = getComputedStyle(document.documentElement);
    const colorBlack = rootStyles.getPropertyValue("--black").trim() || "#101829";
    const colorPrimary = rootStyles.getPropertyValue("--primary").trim() || "#e23fff";

    const cells = genera.flatMap((genus) =>
      diseases.map((disease) => ({
        genus,
        disease,
        value: abundance.by_disease[disease]?.[genus] ?? 0,
      })),
    );

    const maxValue = d3.max(cells, (cell) => cell.value) ?? 0.01;
    const colorScale = d3.scaleSequential()
      .domain([0, maxValue])
      .interpolator(d3.interpolate(colorBlack, colorPrimary));

    const cellWidth = 64;
    const cellHeight = 28;
    const margin = { top: 170, right: 24, bottom: 26, left: 320 };
    const width = margin.left + margin.right + diseases.length * cellWidth;
    const height = margin.top + margin.bottom + genera.length * cellHeight;

    const xScale = d3.scaleBand()
      .domain(diseases)
      .range([0, diseases.length * cellWidth])
      .padding(0.08);
    const yScale = d3.scaleBand()
      .domain(genera)
      .range([0, genera.length * cellHeight])
      .padding(0.08);

    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const chart = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    chart.selectAll(".cell")
      .data(cells)
      .join("rect")
      .attr("x", (cell) => xScale(cell.disease) ?? 0)
      .attr("y", (cell) => yScale(cell.genus) ?? 0)
      .attr("width", xScale.bandwidth())
      .attr("height", yScale.bandwidth())
      .attr("rx", 4)
      .attr("fill", (cell) => colorScale(cell.value))
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (cell) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "属" : "Genus"}</span>
            <span>{cell.genus}</span>
            <span>{locale === "zh" ? "疾病" : "Disease"}</span>
            <span>{diseaseDisplayNameI18n(cell.disease, locale)}</span>
            <span>{locale === "zh" ? "丰度" : "Abundance"}</span>
            <span>{(cell.value * 100).toFixed(3)}%</span>
          </div>,
        )
      );

    chart.append("g")
      .call(d3.axisLeft(yScale))
      .attr("font-size", 12);
    chart.append("g")
      .call(
        d3.axisTop(xScale)
          .tickFormat((tick) => formatDiseaseLabel(String(tick), locale, 32)),
      )
      .attr("font-size", 11)
      .selectAll("text")
      .attr("transform", "rotate(-52)")
      .attr("text-anchor", "start");

    const legendWidth = Math.min(diseases.length * cellWidth, 200);
    const legendY = genera.length * cellHeight + 12;
    const defs = svg.append("defs");
    const gradientId = `metabolismHeatmap-${category.id}`;
    const gradient = defs.append("linearGradient")
      .attr("id", gradientId)
      .attr("x1", "0%")
      .attr("x2", "100%");
    gradient.append("stop").attr("offset", "0%").attr("stop-color", colorBlack);
    gradient.append("stop").attr("offset", "100%").attr("stop-color", colorPrimary);

    chart.append("rect")
      .attr("x", 0)
      .attr("y", legendY)
      .attr("width", legendWidth)
      .attr("height", 12)
      .attr("rx", 4)
      .attr("fill", `url(#${gradientId})`);
    chart.append("text")
      .attr("x", 0)
      .attr("y", legendY + 28)
      .attr("font-size", 10)
      .attr("fill", "currentColor")
      .text("0%");
    chart.append("text")
      .attr("x", legendWidth)
      .attr("y", legendY + 28)
      .attr("text-anchor", "end")
      .attr("font-size", 10)
      .attr("fill", "currentColor")
      .text(`${(maxValue * 100).toFixed(2)}%`);
  }, [abundance, category.id, diseaseOrder, locale, matchResult.genera]);

  return (
    <>
      <div className={classes.catHeader}>
        <span className={classes.detailIcon}>{category.icon}</span>
        <div>
          <h2 className={classes.detailName}>
            {locale === "zh" ? category.name_zh : category.name_en}
          </h2>
        </div>
      </div>

      <p className={classes.detailDesc}>
        {locale === "zh" ? category.description_zh ?? category.description : category.description}
      </p>

      <div className={classes.summaryRow}>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.matchedGenera")}</span>
          <strong>{profileResult?.n_matched ?? matchResult.genera.length}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.strictNc")}</span>
          <strong>{profileResult?.control_count ?? "—"}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.testedDiseases")}</span>
          <strong>{profileResult?.disease_profiles.length ?? "—"}</strong>
        </div>
        <div className={classes.summaryCard}>
          <span className={classes.summaryLabel}>{t("metabolism.fallbackMatch")}</span>
          <strong>{matchResult.fallback ? (locale === "zh" ? "是" : "Yes") : (locale === "zh" ? "否" : "No")}</strong>
        </div>
      </div>

      <div className={classes.infoBlock}>
        <h4>{t("metabolism.memberGenera")} ({category.taxa.length})</h4>
        <div className={classes.taxaGrid}>
          {category.taxa.map((taxon) => (
            <Link
              key={taxon}
              to={`/species/${encodeURIComponent(taxon)}`}
              className={classes.taxonTag}
              title={`View ${taxon} species detail`}
            >
              <i>{taxon}</i>
            </Link>
          ))}
        </div>
      </div>

      <div className={classes.infoBlock}>
        <h4>{t("metabolism.keyMetabolites")}</h4>
        <div className={classes.metaboliteList}>
          {(locale === "zh" && category.key_metabolites_zh?.length
            ? category.key_metabolites_zh
            : category.key_metabolites
          ).map((metabolite, index) => (
            <span key={`${metabolite}-${index}`} className={classes.metaboliteTag}>{metabolite}</span>
          ))}
        </div>
      </div>

      <div className={classes.infoBlock}>
        <h4>{t("metabolism.relatedPathways")}</h4>
        <div className={classes.pathwayList}>
          {pathwayLinks.map((pathway) => (
            <span key={`${category.id}-${pathway.label}`} className={classes.pathwayTag}>
              {pathway.label}
              <span className={classes.pathwayLinks}>
                {pathway.keggId && (
                  <a
                    href={`https://www.kegg.jp/pathway/${pathway.keggId}`}
                    target="_blank"
                    rel="noreferrer noopener"
                    className={classes.externalLink}
                    title={`View ${pathway.keggId} in KEGG`}
                  >
                    {t("metabolism.pathway.kegg")}
                  </a>
                )}
                {pathway.metacycId && (
                  <a
                    href={`https://metacyc.org/META/NEW-IMAGE?type=PATHWAY&object=${pathway.metacycId}`}
                    target="_blank"
                    rel="noreferrer noopener"
                    className={classes.externalLink}
                    title={`View ${pathway.metacycId} in MetaCyc`}
                  >
                    {t("metabolism.pathway.metacyc")}
                  </a>
                )}
              </span>
            </span>
          ))}
        </div>
      </div>

      <div className={classes.relevanceBlock}>
        <h4>{t("metabolism.clinicalRelevance")}</h4>
        <p>{locale === "zh" ? category.health_relevance_zh ?? category.health_relevance : category.health_relevance}</p>
      </div>

      <div className={classes.chartBlock}>
        <div className={classes.chartHeaderRow}>
          <div>
            <h4>{t("metabolism.avgAbundance")}</h4>
            <p className={classes.subtleText}>{t("metabolism.profileNote")}</p>
          </div>
        </div>
        {profileLoading && <p className={classes.subtleText}>{t("metabolism.loading")}</p>}
        {matchResult.fallback && (
          <p className={classes.subtleText}>{t("metabolism.fallbackMatch")}</p>
        )}
        <svg ref={barRef} className={classes.metricChart} />
      </div>

      <div className={classes.chartBlock}>
        <div className={classes.chartHeaderRow}>
          <div>
            <h4>{t("metabolism.heatmap")}</h4>
            <p className={classes.subtleText}>{t("metabolism.heatmapNote")}</p>
          </div>
        </div>
        <svg ref={heatRef} className={classes.metricChart} />
      </div>

      {category.references.length > 0 && (
        <div className={classes.refBlock}>
          <h4>{t("metabolism.references")}</h4>
          {category.references.map((reference) => {
            const doiMatch = reference.match(/10\.\d{4,}\/[\w.\-\/]+/);
            const pmidMatch = reference.match(/PMID[:\s]*(\d+)/i);
            if (pmidMatch) {
              return (
                <a
                  key={reference}
                  href={`https://pubmed.ncbi.nlm.nih.gov/${pmidMatch[1]}`}
                  target="_blank"
                  rel="noreferrer"
                  className={classes.ref}
                >
                  {reference}
                </a>
              );
            }
            if (doiMatch) {
              return (
                <a
                  key={reference}
                  href={`https://doi.org/${doiMatch[0]}`}
                  target="_blank"
                  rel="noreferrer"
                  className={classes.ref}
                >
                  {reference}
                </a>
              );
            }
            return <span key={reference} className={classes.ref}>{reference}</span>;
          })}
        </div>
      )}
    </>
  );
};

export default MetabolismPage;
