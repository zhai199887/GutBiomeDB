/**
 * MetabolismPage.tsx
 * Browse microbiota by metabolic function category
 * 按代谢功能分类浏览微生物组成
 */
import { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import { Link } from "react-router-dom";
import * as d3 from "d3";
import { useI18n } from "@/i18n";
import { useData } from "@/data";
import { diseaseShortNameI18n } from "@/util/diseaseNames";
import "@/components/tooltip";
import classes from "./MetabolismPage.module.css";

// ── Types / 类型定义 ──────────────────────────────────────────────────────────

interface MetabolismCategory {
  id: string;
  name_en: string;
  name_zh: string;
  icon: string;
  description: string;
  taxa: string[];
  key_metabolites: string[];
  related_pathways: string[];
  health_relevance: string;
  references: string[];
}

interface MetabolismMapping {
  version: string;
  last_updated: string;
  categories: MetabolismCategory[];
}

// ── Main component / 主组件 ───────────────────────────────────────────────────

const MetabolismPage = () => {
  const { t, locale } = useI18n();
  const [mapping, setMapping] = useState<MetabolismMapping | null>(null);
  const [fetchError, setFetchError] = useState(false);
  const [selected, setSelected] = useState<MetabolismCategory | null>(null);
  const [search, setSearch] = useState("");
  const abundance = useData((s) => s.abundance);

  useEffect(() => {
    fetch("/data/metabolism_mapping.json")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: MetabolismMapping) => {
        setMapping(data);
        setSelected(data.categories[0] ?? null);
      })
      .catch(() => {
        setFetchError(true);
        setMapping({ version: "error", last_updated: "", categories: [] });
      });
  }, []);

  if (!mapping) {
    return (
      <div className={classes.page}>
        <div className={classes.loading}>Loading metabolism data…</div>
      </div>
    );
  }

  if (fetchError) {
    return (
      <div className={classes.page}>
        <div className={classes.header}>
          <Link to="/" className={classes.back}>{t("metabolism.back")}</Link>
          <h1>{t("metabolism.title")}</h1>
        </div>
        <div className={classes.errorBanner}>
          {t("metabolism.loadError")} Please ensure{" "}
          <code>/public/data/metabolism_mapping.json</code> exists and is valid JSON.
        </div>
      </div>
    );
  }

  // Find which category a taxon belongs to / 查找某物种属于哪个代谢类别
  const taxonToCategories = (taxon: string): MetabolismCategory[] =>
    mapping.categories.filter((c) =>
      c.taxa.some((t) => t.toLowerCase().includes(taxon.toLowerCase())),
    );

  // Search highlighting / 搜索高亮
  const searchResults = search.trim()
    ? taxonToCategories(search.trim())
    : [];

  return (
    <div className={classes.page}>
      {/* Header / 页面标题 */}
      <div className={classes.header}>
        <Link to="/" className={classes.back}>{t("metabolism.back")}</Link>
        <h1>{t("metabolism.title")}</h1>
        <p>{t("metabolism.subtitle")}</p>

        {/* Species search / 物种搜索 */}
        <div className={classes.searchBox}>
          <input
            type="text"
            placeholder={t("metabolism.searchPlaceholder")}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className={classes.searchInput}
          />
          {search && searchResults.length > 0 && (
            <div className={classes.searchResults}>
              <p className={classes.searchHint}>
                <b>{search}</b> found in:
              </p>
              {searchResults.map((c) => (
                <button
                  key={c.id}
                  className={classes.searchResultItem}
                  onClick={() => { setSelected(c); setSearch(""); }}
                >
                  {c.icon} {c.name_en}
                </button>
              ))}
            </div>
          )}
          {search && searchResults.length === 0 && (
            <div className={classes.searchResults}>
              <p className={classes.searchHint}>No category found for "<b>{search}</b>"</p>
            </div>
          )}
        </div>
      </div>

      <div className={classes.layout}>
        {/* Left panel: category cards / 左侧：类别卡片 */}
        <aside className={classes.sidebar}>
          <h3 className={classes.sideTitle}>{t("metabolism.categories")}</h3>
          {mapping.categories.map((cat) => {
            const isHighlighted = search
              ? cat.taxa.some((t) => t.toLowerCase().includes(search.toLowerCase()))
              : false;

            return (
              <button
                key={cat.id}
                className={classes.categoryCard}
                data-active={selected?.id === cat.id}
                data-highlighted={isHighlighted}
                onClick={() => setSelected(cat)}
              >
                <span className={classes.catIcon}>{cat.icon}</span>
                <div className={classes.catInfo}>
                  <span className={classes.catName}>{locale === "zh" ? cat.name_zh : cat.name_en}</span>
                  {locale === "en" && <span className={classes.catZh}>{cat.name_zh}</span>}
                  <span className={classes.catCount}>{cat.taxa.length} {t("metabolism.genera")}</span>
                </div>
              </button>
            );
          })}
        </aside>

        {/* Right panel: details / 右侧：详细信息 */}
        <main className={classes.detail}>
          {selected && (
            <CategoryDetail category={selected} abundance={abundance} />
          )}
        </main>
      </div>
    </div>
  );
};

// ── Category detail component / 类别详情组件 ─────────────────────────────────

const CategoryDetail = ({
  category,
  abundance,
}: {
  category: MetabolismCategory;
  abundance: ReturnType<typeof useData.getState>["abundance"];
}) => {
  const { t, locale } = useI18n();
  const barRef = useRef<SVGSVGElement>(null);
  const heatRef = useRef<SVGSVGElement>(null);

  // Draw abundance bar chart for taxa in this category
  // 绘制该类别下各物种的平均丰度柱状图
  useEffect(() => {
    if (!barRef.current || !abundance) return;
    const svg = d3.select(barRef.current);
    svg.selectAll("*").remove();

    // Find genera present in abundance data / 在丰度数据中查找该类别的属
    const availableGenera = abundance.genera.filter((g) =>
      category.taxa.some((t) =>
        g.toLowerCase().includes(t.toLowerCase()) ||
        t.toLowerCase().includes(g.toLowerCase()),
      ),
    );

    if (availableGenera.length === 0) {
      svg.attr("viewBox", "0 0 480 60");
      svg.append("text").attr("x", 10).attr("y", 30)
        .text("No abundance data available for this category")
        .attr("fill", "currentColor").attr("font-size", 12);
      return;
    }

    // Compute average abundance across all diseases
    // 计算跨所有疾病的平均丰度
    const avgAbundance = availableGenera.map((genus) => {
      const allVals = Object.values(abundance.by_disease).map(
        (d) => d[genus] ?? 0,
      );
      return { genus, avg: d3.mean(allVals) ?? 0 };
    }).sort((a, b) => b.avg - a.avg);

    const margin = { top: 10, right: 20, bottom: 40, left: 180 };
    const W = 640, H = Math.max(150, avgAbundance.length * 28 + 50);
    svg.attr("viewBox", `0 0 ${W} ${H}`);
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;

    const xScale = d3.scaleLinear()
      .domain([0, d3.max(avgAbundance, (d) => d.avg) ?? 0.01])
      .range([0, iW]);
    const yScale = d3.scaleBand()
      .domain(avgAbundance.map((d) => d.genus))
      .range([0, iH])
      .padding(0.2);

    g.selectAll(".bar")
      .data(avgAbundance)
      .join("rect")
      .attr("class", "bar")
      .attr("x", 0)
      .attr("y", (d) => yScale(d.genus) ?? 0)
      .attr("width", (d) => xScale(d.avg))
      .attr("height", yScale.bandwidth())
      .attr("fill", "var(--secondary)")
      .attr("opacity", 0.8)
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "属" : "Genus"}</span><span>{d.genus}</span>
            <span>{locale === "zh" ? "平均丰度" : "Avg. Abundance"}</span><span>{(d.avg * 100).toFixed(4)}%</span>
          </div>
        )
      );

    g.append("g").call(d3.axisLeft(yScale))
      .attr("font-size", 11);
    g.append("g").attr("transform", `translate(0,${iH})`)
      .call(d3.axisBottom(xScale).ticks(4).tickFormat((d) => `${(Number(d) * 100).toFixed(2)}%`))
      .attr("font-size", 10);
  }, [category, abundance, locale]);

  // Draw disease heatmap / 绘制疾病分布热图
  useEffect(() => {
    if (!heatRef.current || !abundance) return;
    const svg = d3.select(heatRef.current);
    svg.selectAll("*").remove();

    const availableGenera = abundance.genera.filter((g) =>
      category.taxa.some((t) =>
        g.toLowerCase().includes(t.toLowerCase()) ||
        t.toLowerCase().includes(g.toLowerCase()),
      ),
    );

    // Top 10 diseases by sample count / 前10种疾病
    const diseases = Object.keys(abundance.by_disease).slice(0, 12);
    if (availableGenera.length === 0 || diseases.length === 0) return;

    const margin = { top: 90, right: 20, bottom: 20, left: 140 };
    const cellW = 60, cellH = 24;
    const W = cellW * diseases.length + margin.left + margin.right;
    const H = cellH * availableGenera.length + margin.top + margin.bottom;
    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    // Build data / 构建数据矩阵
    const flatData: { genus: string; disease: string; val: number }[] = [];
    for (const genus of availableGenera) {
      for (const disease of diseases) {
        flatData.push({ genus, disease, val: abundance.by_disease[disease]?.[genus] ?? 0 });
      }
    }

    const maxVal = d3.max(flatData, (d) => d.val) ?? 0.01;

    // d3.interpolate runs at JS time — CSS variables must be resolved via getComputedStyle
    // d3.interpolate在JS执行时运行，CSS变量需通过getComputedStyle提前解析
    const rootStyles = getComputedStyle(document.documentElement);
    const colorBlack = rootStyles.getPropertyValue("--black").trim() || "#101829";
    const colorPrimary = rootStyles.getPropertyValue("--primary").trim() || "#e23fff";

    const colorScale = d3.scaleSequential()
      .domain([0, maxVal])
      .interpolator(d3.interpolate(colorBlack, colorPrimary));

    const xScale = d3.scaleBand().domain(diseases).range([0, cellW * diseases.length]).padding(0.05);
    const yScale = d3.scaleBand().domain(availableGenera).range([0, cellH * availableGenera.length]).padding(0.05);

    g.selectAll(".cell")
      .data(flatData)
      .join("rect")
      .attr("class", "cell")
      .attr("x", (d) => xScale(d.disease) ?? 0)
      .attr("y", (d) => yScale(d.genus) ?? 0)
      .attr("width", xScale.bandwidth())
      .attr("height", yScale.bandwidth())
      .attr("fill", (d) => colorScale(d.val))
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", (d) =>
        renderToString(
          <div className="tooltip-table">
            <span>{locale === "zh" ? "属" : "Genus"}</span><span>{d.genus}</span>
            <span>{locale === "zh" ? "疾病" : "Disease"}</span><span>{diseaseShortNameI18n(d.disease, locale, 30)}</span>
            <span>{locale === "zh" ? "丰度" : "Abundance"}</span><span>{(d.val * 100).toFixed(4)}%</span>
          </div>
        )
      );

    g.append("g").call(d3.axisLeft(yScale)).attr("font-size", 11);
    g.append("g")
      .call(d3.axisTop(xScale).tickFormat((d) => diseaseShortNameI18n(d as string, locale, 14)))
      .attr("font-size", 10)
      .selectAll("text")
      .attr("transform", "rotate(-40)")
      .attr("text-anchor", "start");

    // Color legend bar / 色阶图例
    const legendW = Math.min(cellW * diseases.length, 200);
    const legendH = 10;
    const legendY = cellH * availableGenera.length + 10;
    const defs = svg.append("defs");
    const gradId = `heatGrad-${category.id}`;
    const grad = defs.append("linearGradient").attr("id", gradId)
      .attr("x1", "0%").attr("x2", "100%");
    // Use already-resolved hex values (CSS variables not valid in SVG export context)
    // 使用已解析的十六进制值（CSS变量在SVG导出时无法解析）
    grad.append("stop").attr("offset", "0%").attr("stop-color", colorBlack);
    grad.append("stop").attr("offset", "100%").attr("stop-color", colorPrimary);

    g.append("rect")
      .attr("x", 0).attr("y", legendY)
      .attr("width", legendW).attr("height", legendH)
      .attr("fill", `url(#${gradId})`).attr("rx", 2);
    g.append("text").attr("x", 0).attr("y", legendY + legendH + 11)
      .attr("font-size", 8).attr("fill", "currentColor").text("0%");
    g.append("text").attr("x", legendW).attr("y", legendY + legendH + 11)
      .attr("text-anchor", "end").attr("font-size", 8).attr("fill", "currentColor")
      .text(`${(maxVal * 100).toFixed(2)}%`);
    g.append("text").attr("x", legendW / 2).attr("y", legendY + legendH + 11)
      .attr("text-anchor", "middle").attr("font-size", 8).attr("fill", "var(--light-gray)")
      .text(locale === "zh" ? "平均丰度" : "Mean Abundance");
  }, [category, abundance, locale]);

  return (
    <div className={classes.detailPanel}>
      {/* Category header / 类别标题 */}
      <div className={classes.catHeader}>
        <span className={classes.detailIcon}>{category.icon}</span>
        <div>
          <h2 className={classes.detailName}>{locale === "zh" ? category.name_zh : category.name_en}</h2>
          {locale === "en" && <p className={classes.detailZh}>{category.name_zh}</p>}
        </div>
      </div>

      <p className={classes.detailDesc}>{category.description}</p>

      {/* Taxa list — each genus links to its Species page / 物种列表，每个属名链接到物种详情页 */}
      <div className={classes.infoBlock}>
        <h4>{t("metabolism.memberGenera")} ({category.taxa.length})</h4>
        <div className={classes.taxaGrid}>
          {category.taxa.map((t) => (
            <Link
              key={t}
              to={`/species/${encodeURIComponent(t)}`}
              className={classes.taxonTag}
              title={`View ${t} species detail`}
            >
              <i>{t}</i>
            </Link>
          ))}
        </div>
      </div>

      {/* Metabolites / 代谢物 */}
      <div className={classes.infoBlock}>
        <h4>{t("metabolism.keyMetabolites")}</h4>
        <div className={classes.metaboliteList}>
          {category.key_metabolites.map((m) => (
            <span key={m} className={classes.metaboliteTag}>{m}</span>
          ))}
        </div>
      </div>

      {/* Pathways / 通路 */}
      <div className={classes.infoBlock}>
        <h4>{t("metabolism.relatedPathways")}</h4>
        <div className={classes.pathwayList}>
          {category.related_pathways.map((p) => (
            <span key={p} className={classes.pathwayTag}>{p}</span>
          ))}
        </div>
      </div>

      {/* Health relevance / 临床意义 */}
      <div className={classes.relevanceBlock}>
        <h4>{t("metabolism.clinicalRelevance")}</h4>
        <p>{category.health_relevance}</p>
      </div>

      {/* Abundance charts (only if data loaded) / 丰度图表 */}
      {abundance && (
        <>
          <div className={classes.chartBlock}>
            <h4>{t("metabolism.avgAbundance")}</h4>
            <svg ref={barRef} className={classes.metaChart} />
          </div>

          <div className={classes.chartBlock}>
            <h4>{t("metabolism.heatmap")}</h4>
            <svg ref={heatRef} className={classes.metaChart} />
          </div>
        </>
      )}

      {/* References / 参考文献 */}
      {category.references.length > 0 && (
        <div className={classes.refBlock}>
          <h4>{t("metabolism.references")}</h4>
          {category.references.map((r) => {
            // If reference looks like a DOI or PubMed ID, make it a link
            // 如果是DOI或PMID格式，转为可点击链接
            const doiMatch = r.match(/10\.\d{4,}\/[^\s.,;)]+/);
            const pmidMatch = r.match(/PMID[:\s]*(\d+)/i);
            if (doiMatch) {
              return (
                <a key={r} href={`https://doi.org/${doiMatch[0]}`}
                  target="_blank" rel="noreferrer" className={classes.ref}>
                  {r}
                </a>
              );
            } else if (pmidMatch) {
              return (
                <a key={r} href={`https://pubmed.ncbi.nlm.nih.gov/${pmidMatch[1]}`}
                  target="_blank" rel="noreferrer" className={classes.ref}>
                  {r}
                </a>
              );
            }
            return <span key={r} className={classes.ref}>{r}</span>;
          })}
        </div>
      )}
    </div>
  );
};

export default MetabolismPage;
