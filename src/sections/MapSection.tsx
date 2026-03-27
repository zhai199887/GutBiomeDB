import { useEffect, useRef } from "react";
import { renderToString } from "react-dom/server";
import * as d3 from "d3";
import type { Feature } from "geojson";
import { clamp } from "lodash";
import Placeholder from "@/components/Placeholder";
import type { Data, MetadataSummary } from "@/data";
import { setSelectedFeature, useData } from "@/data";
import { downloadSvg, getCssVariable } from "@/util/dom";
import { formatNumber } from "@/util/string";
import classes from "./Map.module.css";

const width = 770;
const height = 400;

const MapSection = () => {
  const byCountry = useData((s) => s.byCountry);
  const summary = useData((s) => s.summary);
  const selectedFeature = useData((s) => s.selectedFeature);
  const mapInstanceRef = useRef<ReturnType<typeof makeMap> | null>(null);

  useEffect(() => {
    mapInstanceRef.current = makeMap();
  }, []);

  useEffect(() => {
    mapInstanceRef.current?.(byCountry, summary, selectedFeature);
  }, [byCountry, summary, selectedFeature]);

  if (!byCountry || !summary)
    return <Placeholder height={400}>Loading map...</Placeholder>;

  const gray = getCssVariable("--gray");

  return (
    <section>
      <h2>Geographic Distribution</h2>
      <div className="sub-section">
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
          <span>Fewer Samples</span>
          <span
            className={classes.gradient}
            data-inactive={!!selectedFeature}
          />
          <span>More Samples</span>
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
  ) => {
    if (!byCountry) return;

    const primary = getCssVariable("--primary");
    const secondary = getCssVariable("--secondary");
    const gray = getCssVariable("--gray");
    const darkGray = getCssVariable("--dark-gray");
    const black = getCssVariable("--black");

    type Datum = (typeof byCountry)["features"][number];
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

    /** GeoJSON uses "code" field (e.g. "CN") — match against country_stats */
    const countryStats = summary?.country_stats ?? {};

    /** use samples already stored in GeoJSON (correct counts), enrich only for color */
    const enriched = {
      ...byCountry,
      features: byCountry.features.map((f) => ({
        ...f,
        properties: {
          ...f.properties,
          /** GeoJSON already has samples; keep as-is */
        },
      })),
    };

    const [, max = 1000] = d3.extent(
      enriched.features,
      (d) => d.properties.samples,
    );

    const isSelected = (d: Datum) =>
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
      .selectAll(".feature")
      .data(enriched.features, (d) => {
        const { region, country } = (d as Datum).properties;
        return region + "|" + country;
      })
      .join("path")
      .attr("class", "feature")
      .attr("d", path)
      .attr("fill", (d) =>
        isSelected(d) ? secondary : scale(d.properties.samples || 1),
      )
      .attr("stroke", black)
      .attr("stroke-width", 0.5)
      .style("cursor", "pointer")
      .attr("role", "graphics-symbol")
      .attr("data-tooltip", ({ properties }) => {
        const { country, region, code, samples } = properties;

        /** look up per-country stats by ISO code */
        const stat = countryStats[code ?? ""];

        let sexInfo: string | undefined;
        let top3diseases: string[] | undefined;
        let top3ages: string[] | undefined;

        if (stat) {
          if (stat.sex.female_pct !== null && stat.sex.male_pct !== null) {
            sexInfo = `${stat.sex.female_pct}% F / ${stat.sex.male_pct}% M`;
          }
          top3diseases = Object.entries(stat.top_diseases)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([d, n]) => `${d} (${formatNumber(n, false)})`);
          top3ages = Object.entries(stat.top_ages)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([g, n]) => `${g.replace(/_/g, " ")} (${formatNumber(n, false)})`);
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
            {sexInfo && (
              <>
                <span>Sex ratio</span>
                <span>{sexInfo}</span>
              </>
            )}
            {top3ages && top3ages.length > 0 && (
              <>
                <span>Top age groups</span>
                <span>{top3ages.join(" · ")}</span>
              </>
            )}
            {top3diseases && top3diseases.length > 0 && (
              <>
                <span>Top diseases</span>
                <span>{top3diseases.join(" · ")}</span>
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
