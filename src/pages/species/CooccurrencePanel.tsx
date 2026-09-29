import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { exportTable } from "@/util/export";
import { API_BASE } from "@/util/apiBase";
import { phylumColor } from "@/util/phylumColors";
import { sortDiseaseItemsByName } from "@/util/diseaseNames";

import type { CooccurrenceResponse, DiseaseListItem } from "./types";
import { formatPValue, translateDimensionName } from "./utils";

interface CooccurrencePanelProps {
  genus: string;
  phylum: string;
}

export default function CooccurrencePanel({ genus, phylum }: CooccurrencePanelProps) {
  const { locale, t } = useI18n();
  const svgRef = useRef<SVGSVGElement>(null);
  const [diseases, setDiseases] = useState<DiseaseListItem[]>([]);
  const [disease, setDisease] = useState("");
  const [data, setData] = useState<CooccurrenceResponse | null>(null);

  useEffect(() => {
    cachedFetch<{ diseases: DiseaseListItem[] }>(`${API_BASE}/api/disease-list`)
      .then((payload) => setDiseases(payload.diseases ?? []))
      .catch(() => setDiseases([]));
  }, []);

  const sortedDiseases = useMemo(() => sortDiseaseItemsByName(diseases), [diseases]);

  useEffect(() => {
    const search = new URLSearchParams({
      genus,
      top_k: "18",
    });
    if (disease) search.set("disease", disease);
    cachedFetch<CooccurrenceResponse>(`${API_BASE}/api/species-cooccurrence?${search.toString()}`)
      .then((payload) => setData(payload))
      .catch(() => setData(null));
  }, [disease, genus]);

  const tableRows = useMemo(
    () => (data?.partners ?? []).map((item) => ({
      genus: item.genus,
      phylum: item.phylum,
      r: item.r,
      adjusted_p: item.adjusted_p,
      type: item.type,
    })),
    [data],
  );

  useEffect(() => {
    if (!svgRef.current || !data || data.partners.length === 0) return;
    drawMiniNetwork(svgRef.current, genus, phylum, data, locale);
  }, [data, genus, locale, phylum]);

  if (!data || data.partners.length === 0) return null;

  return (
    <section className="species-block">
      <div className="species-blockHeader">
        <div>
          <h2>{t("species.tab.cooccurrence")}</h2>
          <p>{t("species.cooccurrence.subtitle")}</p>
        </div>
        <div className="species-inlineControls">
          <label htmlFor="species-cooccurrence-disease">{t("species.cooccurrence.context")}</label>
          <select id="species-cooccurrence-disease" value={disease} onChange={(event) => setDisease(event.target.value)}>
            <option value="">{t("species.cooccurrence.nc")}</option>
            {sortedDiseases.map((item) => (
              <option key={item.name} value={item.name}>
                {translateDimensionName(item.name, locale, "disease")}
              </option>
            ))}
          </select>
          <button type="button" onClick={() => exportTable(tableRows, `${genus}_cooccurrence_${disease || "nc"}`)}>CSV</button>
        </div>
      </div>

      <div className="species-summaryPills">
        <span>{`${locale === "zh" ? "上下文" : "Context"}: ${disease ? translateDimensionName(disease, locale, "disease") : t("species.cooccurrence.nc")}`}</span>
        <span>{`${locale === "zh" ? "样本数" : "Samples"}: ${data.n_samples.toLocaleString("en")}`}</span>
        <span>{`${locale === "zh" ? "显著伙伴" : "Significant partners"}: ${data.partners.length}`}</span>
      </div>

      <div className="species-grid species-gridNetwork">
        <article className="species-chartCard">
          <div className="species-chartHeader">
            <div>
              <h3>{t("species.cooccurrence.miniNetwork")}</h3>
              <p>{t("species.cooccurrence.networkHint")}</p>
            </div>
          </div>
          <svg ref={svgRef} className="species-chart" />
        </article>

        <article className="species-chartCard">
          <div className="species-chartHeader">
            <div>
              <h3>{t("species.cooccurrence.edgeTable")}</h3>
              <p>{t("species.cooccurrence.tableHint")}</p>
            </div>
          </div>
          <div className="species-tableWrap">
            <table className="species-table">
              <thead>
                <tr>
                  <th>{t("species.cooccurrence.genus")}</th>
                  <th>{t("species.phylum")}</th>
                  <th>r</th>
                  <th>{t("species.cooccurrence.adjP")}</th>
                  <th>{t("species.cooccurrence.type")}</th>
                </tr>
              </thead>
              <tbody>
                {data.partners.map((item) => (
                  <tr key={item.genus}>
                    <td>
                      <Link to={`/species/${encodeURIComponent(item.genus)}`}>
                        <i>{item.genus}</i>
                      </Link>
                    </td>
                    <td>{item.phylum ?? "Other"}</td>
                    <td style={{ color: item.type === "positive" ? "#16a34a" : "#dc2626" }}>
                      {item.r > 0 ? "+" : ""}
                      {item.r.toFixed(3)}
                    </td>
                    <td>{formatPValue(item.adjusted_p)}</td>
                    <td>{item.type === "positive" ? t("species.cooccurrence.positive") : t("species.cooccurrence.negative")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </div>
    </section>
  );
}

function drawMiniNetwork(
  svgEl: SVGSVGElement,
  genus: string,
  phylum: string,
  payload: CooccurrenceResponse,
  locale: string,
) {
  interface MiniNode extends d3.SimulationNodeDatum {
    id: string;
    phylum: string;
    r: number;
    type: "root" | "positive" | "negative";
  }

  interface MiniLink extends d3.SimulationLinkDatum<MiniNode> {
    source: string | MiniNode;
    target: string | MiniNode;
    r: number;
    type: "positive" | "negative";
  }

  const svg = d3.select(svgEl);
  svg.selectAll("*").remove();

  const width = 1240;
  const height = 700;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const rootNode: MiniNode = { id: genus, phylum, r: 0, type: "root" };
  const nodes: MiniNode[] = [rootNode, ...payload.partners.map((item) => ({ id: item.genus, phylum: item.phylum ?? "Other", r: item.r, type: item.type }))];
  const links: MiniLink[] = payload.partners.map((item) => ({ source: genus, target: item.genus, r: item.r, type: item.type }));

  const simulation = d3.forceSimulation<MiniNode>(nodes)
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("charge", d3.forceManyBody().strength(-420))
    .force("link", d3.forceLink<MiniNode, MiniLink>(links).id((node) => node.id).distance((link) => 220 - Math.min(Math.abs(link.r) * 96, 76)))
    .force("collision", d3.forceCollide(54));

  const link = svg.append("g")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke", (item) => (item.type === "positive" ? "rgba(34,197,94,0.55)" : "rgba(239,68,68,0.5)"))
    .attr("stroke-width", (item) => 1.4 + Math.abs(item.r) * 3);

  const node = svg.append("g")
    .selectAll("circle")
    .data(nodes)
    .join("circle")
    .attr("r", (item) => (item.id === genus ? 32 : 19 + Math.abs(item.r) * 9))
    .attr("fill", (item) => phylumColor(item.phylum))
    .attr("stroke", "rgba(255,255,255,0.7)")
    .attr("stroke-width", (item) => (item.id === genus ? 2.2 : 1))
    .attr("data-tooltip", (item) => renderToString(
      <div className="tooltip-table">
        <span>{locale === "zh" ? "菌属" : "Genus"}</span><span>{item.id}</span>
        <span>{locale === "zh" ? "门" : "Phylum"}</span><span>{item.phylum}</span>
        <span>r</span><span>{item.r.toFixed(3)}</span>
      </div>,
    ));

  const labels = svg.append("g")
    .selectAll("text")
    .data(nodes)
    .join("text")
    .attr("font-size", 16)
    .attr("font-weight", (item) => (item.id === genus ? 700 : 500))
    .attr("text-anchor", "middle")
    .attr("fill", "currentColor")
    .text((item) => (item.id.length > 30 ? `${item.id.slice(0, 28)}…` : item.id));

  simulation.on("tick", () => {
    link
      .attr("x1", (item) => (typeof item.source === "object" ? item.source.x ?? 0 : 0))
      .attr("y1", (item) => (typeof item.source === "object" ? item.source.y ?? 0 : 0))
      .attr("x2", (item) => (typeof item.target === "object" ? item.target.x ?? 0 : 0))
      .attr("y2", (item) => (typeof item.target === "object" ? item.target.y ?? 0 : 0));

    node
      .attr("cx", (item) => item.x ?? 0)
      .attr("cy", (item) => item.y ?? 0);

    labels
      .attr("x", (item) => item.x ?? 0)
      .attr("y", (item) => (item.y ?? 0) + 48);
  });
}
