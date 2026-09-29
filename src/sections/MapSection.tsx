import { useEffect, useRef, useState } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import type { Feature } from "geojson";
import { clamp } from "lodash";
import Placeholder from "@/components/Placeholder";
import type { ApiStats, Data, Filters, MetadataSummary, CountryStat } from "@/data";
import { DEFAULT_FILTERS, setSelectedFeature, useData } from "@/data";
import { useI18n } from "@/i18n";
import { API_BASE } from "@/util/apiBase";
import { downloadSvg, getCssVariable } from "@/util/dom";
import { formatNumber } from "@/util/string";
import classes from "./Map.module.css";

const width = 770;
const height = 400;
/** Estimate filtered sample count for a country based on its stats + global fallback */
const estimateFilteredCount = (
  stat: CountryStat | undefined,
  total: number,
  filters: Filters,
  summary?: MetadataSummary,
): number => {
  if (!stat) return 0;
  const isDefault =
    filters.sex === DEFAULT_FILTERS.sex &&
    filters.age_groups.length === 0 &&
    filters.diseases.length === 0;
  if (isDefault) return total;

  let ratio = 1.0;

  // Sex filter
  if (filters.sex !== "all") {
    if (filters.sex === "unknown") {
      // unknown = samples without sex annotation; estimate as 1 - known/total
      const knownRatio = stat.sex.known > 0 ? stat.sex.known / total : 0;
      ratio *= Math.max(0, 1 - knownRatio);
    } else if (stat.sex.known > 0) {
      const pct = filters.sex === "female" ? stat.sex.female_pct : stat.sex.male_pct;
      if (pct !== null) ratio *= pct / 100;
      else ratio *= 0;
    }
  }

  // Age filter: use per-country stats if available, else global proportion
  if (filters.age_groups.length > 0 && summary) {
    const globalTotal = summary.total_samples || 1;
    const globalSelected = filters.age_groups.reduce((s, g) => s + (summary.age_counts[g] ?? 0), 0);
    // Check if country has data for any selected age group
    const countrySelected = filters.age_groups.reduce((s, g) => s + (stat.top_ages[g] ?? 0), 0);
    if (countrySelected > 0) {
      // Country has data for these age groups — use country-level ratio
      ratio *= countrySelected / total;
    } else {
      // Fallback to global proportion
      ratio *= globalSelected / globalTotal;
    }
  }

  // Disease filter: same approach
  if (filters.diseases.length > 0 && summary) {
    const globalTotal = summary.total_samples || 1;
    const globalSelected = filters.diseases.reduce((s, d) => s + (summary.disease_counts[d] ?? 0), 0);
    const countrySelected = filters.diseases.reduce((s, d) => s + (stat.top_diseases[d] ?? 0), 0);
    if (countrySelected > 0) {
      ratio *= countrySelected / total;
    } else {
      ratio *= globalSelected / globalTotal;
    }
  }

  return Math.max(0, Math.round(total * ratio));
};

const MapSection = () => {
  const { t } = useI18n();
  const byCountry = useData((s) => s.byCountry);
  const summary = useData((s) => s.summary);
  const selectedFeature = useData((s) => s.selectedFeature);
  const filters = useData((s) => s.filters);
  const [mapMode, setMapMode] = useState<"samples" | "projects">("samples");
  const [apiStats, setApiStats] = useState<ApiStats | null>(null);
  const mapInstanceRef = useRef<ReturnType<typeof makeMap> | null>(null);
  const mapModeLabels = {
    samples: t("home.mapMode.samples"),
    projects: t("home.mapMode.projects"),
  } as const;

  useEffect(() => {
    mapInstanceRef.current = makeMap();
  }, []);

  useEffect(() => {
    fetch(`${API_BASE}/api/data-stats`)
      .then((response) => response.json())
      .then(setApiStats)
      .catch(() => { /* backend not running, ignore */ });
  }, []);

  useEffect(() => {
    mapInstanceRef.current?.(
      byCountry,
      summary,
      selectedFeature,
      filters,
      mapMode,
      apiStats?.country_project_counts ?? {},
    );
  }, [apiStats?.country_project_counts, byCountry, filters, mapMode, selectedFeature, summary]);

  if (!byCountry || !summary)
    return <Placeholder height={400}>Loading map...</Placeholder>;

  const gray = getCssVariable("--gray");
  const legendStart = mapMode === "samples" ? t("home.fewerSamples") : `${t("home.mapMode.projects")} ↓`;
  const legendEnd = mapMode === "samples" ? t("home.moreSamples") : `${t("home.mapMode.projects")} ↑`;

  return (
    <section>
      <h2>{t("home.geographicDistribution")}</h2>
      <div className="sub-section">
        <div className={classes.mapControls}>
          <span className={classes.mapModeLabel}>{t("home.mapColorBy")}</span>
          {(["samples", "projects"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              className={classes.mapModeBtn}
              data-active={mapMode === mode}
              onClick={() => setMapMode(mode)}
            >
              {mapModeLabels[mode]}
            </button>
          ))}
        </div>
        <svg
          viewBox={[0, 0, width, height].join(" ")}
          id="map"
          className={classes.svg}
          onClick={(e) => {
            if (e.shiftKey) downloadSvg(e.currentTarget, "map");
          }}
        >
          <g className="map-container">
            <path
              className="outline"
              fill="none"
              stroke={gray}
              strokeWidth={0.5}
            />
            <g className="graticules" />
            <g className="features" />
          </g>
        </svg>

        <div className={classes.legend}>
          <span>{legendStart}</span>
          <span
            className={classes.gradient}
            data-inactive={!!selectedFeature}
          />
          <span>{legendEnd}</span>
        </div>
      </div>
    </section>
  );
};

export default MapSection;

const makeMap = () => {
  const projection = d3.geoNaturalEarth1();
  fitProjection(projection);
  const baseScale = projection.scale();

  const resetProjection = () => {
    projection.center([0, 0]);
    projection.rotate([0, 0]);
    projection.scale(baseScale);
  };
  resetProjection();

  const path = d3.geoPath().projection(projection);
  const graticules = d3.geoGraticule().step([20, 20])();

  type SVG = d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown>;
  type ZoomEvent = d3.D3ZoomEvent<SVGSVGElement, unknown>;
  let oldTransform: d3.ZoomTransform | undefined;

  const updateMap = (svg: SVG, fullEvent?: ZoomEvent) => {
    const { sourceEvent: event, transform } = fullEvent || {};
    let [x, y] = projection.center();
    let scale = projection.scale();
    let [lambda, phi] = projection.rotate();

    if (event && transform && oldTransform) {
      const dx = transform.x - oldTransform.x;
      const dy = transform.y - oldTransform.y;
      const dk = transform.k - oldTransform.k;
      if (dk) {
        scale = transform.k;
        const oldPointer = getPointer(svg.node(), projection, event);
        projection.scale(scale);
        for (let i = 0; i < 3; i++) {
          const newPointer = getPointer(svg.node(), projection, event);
          lambda += newPointer.x - oldPointer.x;
          y += newPointer.y - oldPointer.y;
          projection.rotate([lambda, phi]);
          projection.center([x, y]);
        }
      } else {
        lambda += (baseScale / scale / 2) * dx;
        y += (baseScale / scale / 2) * dy;
      }
    }

    if (transform) oldTransform = transform;
    const yLimit = 0.89 * (90 - 90 * (baseScale / scale));
    y = clamp(y, -yLimit, yLimit);
    if (lambda < -180) lambda += 360;
    if (lambda > 180) lambda -= 360;
    projection.rotate([lambda, phi]);
    projection.center([x, y]);

    svg.select(".outline").attr("d", () => path({ type: "Sphere" }));
    svg.selectAll<Element, Feature>(".graticule").attr("d", path);
    svg.selectAll<Element, Feature>(".feature").attr("d", path);
  };

  return (
    byCountry: Data["byCountry"],
    summary: MetadataSummary | undefined,
    selectedFeature: Data["selectedFeature"],
    filters?: Filters,
    mapMode: "samples" | "projects" = "samples",
    countryProjectCounts: Record<string, number> = {},
  ) => {
    if (!byCountry) return;

    const primary = getCssVariable("--primary");
    const secondary = getCssVariable("--secondary");
    const gray = getCssVariable("--gray");
    const darkGray = getCssVariable("--dark-gray");
    const black = getCssVariable("--black");

    type Datum = (typeof byCountry)["features"][number];
    type EnrichedDatum = Datum & {
      properties: Datum["properties"] & {
        samples: number;
        projects: number;
      };
    };
    const svg = d3.select<SVGSVGElement, unknown>("#map");
    if (!svg.node()) return;

    svg.select(".outline").attr("d", () => path({ type: "Sphere" }));

    svg
      .select(".graticules")
      .selectAll(".graticule")
      .data([graticules])
      .join("path")
      .attr("class", "graticule")
      .attr("d", path)
      .attr("fill", "none")
      .attr("stroke", darkGray)
      .attr("stroke-width", 0.5);

    const countryStats = summary?.country_stats ?? {};
    const activeFilters = filters ?? DEFAULT_FILTERS;

    const enriched = {
      ...byCountry,
      features: byCountry.features.map((f) => {
        const code = f.properties.code ?? "";
        const stat = countryStats[code];
        const filteredSamples = estimateFilteredCount(stat, f.properties.samples, activeFilters, summary);
        const projectCount = countryProjectCounts[code] ?? 0;
        return {
          ...f,
          properties: {
            ...f.properties,
            samples: filteredSamples,
            projects: projectCount,
          },
        };
      }),
    };

    const getValue = (feature: EnrichedDatum) =>
      mapMode === "samples" ? feature.properties.samples : (feature.properties.projects ?? 0);
    const [, maxValue = 1000] = d3.extent(
      enriched.features,
      (d) => getValue(d),
    );
    const max = Math.max(1, maxValue);

    const isSelected = (d: EnrichedDatum) =>
      selectedFeature?.country === ""
        ? selectedFeature?.region === d.properties.region
        : selectedFeature?.country === d.properties.country;

    const scale = d3
      .scaleLog<string>()
      .domain([1, max])
      .range(selectedFeature ? [darkGray, gray] : [gray, primary])
      .interpolate(d3.interpolateLab);

    svg
      .select(".features")
      .selectAll<Element, EnrichedDatum>(".feature")
      .data(enriched.features as EnrichedDatum[], (d) => {
        const { region, country } = d.properties;
        return region + "|" + country;
      })
      .join("path")
      .attr("class", "feature")
      .attr("d", path)
      .attr("fill", (d) =>
        isSelected(d) ? secondary : scale(Math.max(1, getValue(d))),
      )
      .attr("stroke", black)
      .attr("stroke-width", 0.5)
      .style("cursor", "pointer")
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", ({ properties }) => {
        const { country, region, code, samples, projects } = properties;
        const stat = countryStats[code ?? ""];

        let sexInfo: string | undefined;
        let topDiseases: string[] = [];
        let topAges: string[] = [];

        if (stat) {
          if (stat.sex.female_pct !== null && stat.sex.male_pct !== null) {
            sexInfo = `${stat.sex.female_pct}% Female / ${stat.sex.male_pct}% Male`;
          }

          topDiseases = Object.entries(stat.top_diseases)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([name, count]) => `${name} (${formatNumber(count, false)})`);

          topAges = Object.entries(stat.top_ages)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(
              ([group, count]) =>
                `${group.replace(/_/g, " ")} (${formatNumber(count, false)})`,
            );
        }

        return renderToString(
          <div className="tooltip-table">
            {country && (
              <>
                <span>Country</span>
                <span>
                  {country} ({code})
                </span>
              </>
            )}
            <span>Region</span>
            <span>{region}</span>
            <span>Samples</span>
            <span>{formatNumber(samples, false)}</span>
            <span>Projects</span>
            <span>{formatNumber(projects, false)}</span>
            {sexInfo && (
              <>
                <span>Sex ratio</span>
                <span>{sexInfo}</span>
              </>
            )}
            {topAges.length > 0 && (
              <>
                <span>Top age groups</span>
                <span>{topAges.join(" · ")}</span>
              </>
            )}
            {topDiseases.length > 0 && (
              <>
                <span>Top diseases</span>
                <span>{topDiseases.join(" · ")}</span>
              </>
            )}
          </div>,
        );
      })
      .on("keydown", selectFeature)
      .on("click", selectFeature);

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([baseScale, baseScale * 10])
      .on("zoom", (event) => updateMap(svg, event));

    zoom.transform(svg, d3.zoomTransform(svg.node()!));
    updateMap(svg);
    zoom(svg);

    svg
      .on("wheel", (event) => event.preventDefault())
      .on("dblclick.zoom", () => {
        zoom.transform(svg, d3.zoomIdentity.scale(baseScale));
        resetProjection();
        updateMap(svg);
      });
  };
};

const fitProjection = (proj: d3.GeoProjection) =>
  proj.fitSize([width, height], {
    type: "Feature",
    properties: {},
    geometry: {
      type: "Polygon",
      coordinates: [
        [
          [-180, -90],
          [180, -90],
          [180, 90],
          [-180, 90],
        ],
      ],
    },
  });

const getPointer = (
  svg: SVGSVGElement | null,
  projection: d3.GeoProjection,
  event: PointerEvent | WheelEvent | TouchEvent,
) => {
  const pt = new DOMPoint(0, 0);
  if ("clientX" in event) {
    pt.x = event.clientX;
    pt.y = event.clientY;
  }
  if ("touches" in event) {
    if (event.touches.length === 1) {
      pt.x = event.touches[0]!.clientX;
      pt.y = event.touches[0]!.clientY;
    }
    if (event.touches.length === 2) {
      pt.x = (event.touches[0]!.clientX + event.touches[1]!.clientX) / 2;
      pt.y = (event.touches[0]!.clientY + event.touches[1]!.clientY) / 2;
    }
  }
  const svgPt = pt.matrixTransform(svg?.getScreenCTM()?.inverse());
  const mapPt = projection.invert?.([svgPt.x, svgPt.y]) || [];
  return { x: mapPt[0] || 0, y: -(mapPt[1] || 0) };
};

const selectFeature = (
  event: PointerEvent | KeyboardEvent,
  d: NonNullable<Data["byCountry"]>["features"][number],
) => {
  const feature = d.properties;
  if ("key" in event) {
    if (event.key === "Enter") setSelectedFeature(feature);
    if (event.key === "Escape") setSelectedFeature();
  } else {
    setSelectedFeature(feature);
    event.stopPropagation();
  }
};

d3.select(window).on(
  "click",
  () => document.activeElement === document.body && setSelectedFeature(),
);
