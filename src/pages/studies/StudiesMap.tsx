import { useEffect, useMemo, useState } from "react";
import * as d3 from "d3";
import type { FeatureCollection, Geometry } from "geojson";

import { useI18n } from "@/i18n";
import { countryName } from "@/util/countries";

type CountryFeatureCollection = FeatureCollection<
  Geometry,
  { region?: string; country?: string; code?: string; samples?: number }
>;

const WIDTH = 760;
const HEIGHT = 360;

type StudiesMapProps = {
  counts: Record<string, number>;
  selectedCountry: string;
  onSelectCountry: (country: string) => void;
};

const StudiesMap = ({ counts, selectedCountry, onSelectCountry }: StudiesMapProps) => {
  const { locale } = useI18n();
  const [geoData, setGeoData] = useState<CountryFeatureCollection | null>(null);

  useEffect(() => {
    fetch("/data/by-country.json")
      .then((response) => response.json())
      .then((payload: CountryFeatureCollection) => setGeoData(payload))
      .catch(() => setGeoData(null));
  }, []);

  const projection = useMemo(() => {
    const base = d3.geoNaturalEarth1();
    if (geoData) {
      base.fitSize([WIDTH, HEIGHT], geoData);
    } else {
      base.fitSize([WIDTH, HEIGHT], { type: "Sphere" } as never);
    }
    return base;
  }, [geoData]);

  const path = useMemo(() => d3.geoPath(projection), [projection]);
  const maxCount = Math.max(1, ...Object.values(counts));
  const colorScale = useMemo(
    () => d3.scaleLinear<string>().domain([0, maxCount]).range(["#15202b", "#1fb773"]),
    [maxCount],
  );

  if (!geoData) {
    return (
      <div style={{ minHeight: 360, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--light-gray)" }}>
        {locale === "zh" ? "正在加载地图…" : "Loading map..."}
      </div>
    );
  }

  return (
    <div>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} style={{ width: "100%", height: "auto" }}>
        <path d={path({ type: "Sphere" }) ?? ""} fill="#0c1117" stroke="#334155" strokeWidth={0.6} />
        <path d={path(d3.geoGraticule10()) ?? ""} fill="none" stroke="#1e293b" strokeWidth={0.4} />
        {geoData.features.map((feature) => {
          const code = String(feature.properties.code ?? "");
          const label = code ? countryName(code, locale) : (feature.properties.country ?? "Unknown");
          const projectCount = counts[code] ?? 0;
          const isSelected = selectedCountry === code;
          return (
            <path
              key={`${code}_${feature.properties.country ?? ""}`}
              d={path(feature) ?? ""}
              fill={isSelected ? "#f59e0b" : colorScale(projectCount)}
              stroke="#020617"
              strokeWidth={0.5}
              style={{ cursor: code ? "pointer" : "default" }}
              onClick={() => {
                if (!code) return;
                onSelectCountry(selectedCountry === code ? "" : code);
              }}
            >
              <title>{`${label} (${code || "NA"})\nProjects: ${projectCount.toLocaleString("en-US")}`}</title>
            </path>
          );
        })}
      </svg>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 10, color: "var(--light-gray)", fontSize: "0.8rem" }}>
        <span>{locale === "zh" ? "较少项目" : "Fewer projects"}</span>
        <span style={{ flex: 1, height: 10, borderRadius: 999, background: "linear-gradient(90deg, #15202b 0%, #1fb773 100%)" }} />
        <span>{locale === "zh" ? "较多项目" : "More projects"}</span>
      </div>
    </div>
  );
};

export default StudiesMap;
