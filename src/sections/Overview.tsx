import BarsIcon from "@/assets/bars.svg?react";
import EarthIcon from "@/assets/earth.svg?react";
import MicroscopeIcon from "@/assets/microscope.svg?react";
import Placeholder from "@/components/Placeholder";
import { useData } from "@/data";
import { formatNumber } from "@/util/string";
import classes from "./Overview.module.css";

const Overview = () => {
  const summary = useData((state) => state.summary);

  const countries = summary
    ? Object.keys(summary.country_counts).filter((k) => k !== "unknown").length
    : undefined;

  const regions = summary
    ? Object.keys(summary.region_counts).filter((k) => k !== "unknown").length
    : undefined;

  const diseases = summary
    ? Object.keys(summary.disease_counts).filter((k) => k !== "unknown").length
    : undefined;

  const tiles = [
    {
      icon: MicroscopeIcon,
      text: (
        <>
          {formatNumber(summary?.total_samples, false)} samples
        </>
      ),
    },
    {
      icon: EarthIcon,
      text: (
        <>
          {formatNumber(countries)} countries
          <br />
          {formatNumber(regions)} regions
        </>
      ),
    },
    {
      icon: BarsIcon,
      text: (
        <>
          {formatNumber(diseases)} disease types
          <br />
          from <i>inform-all</i>
        </>
      ),
    },
  ];

  return (
    <section>
      <h2>Overview</h2>

      <p>
        This platform lets you explore{" "}
        {summary
          ? "over " + formatNumber(
              Math.floor((summary.total_samples || 0) / 10000) * 10000,
            )
          : "thousands of"}{" "}
        publicly available human gut microbiome samples, annotated with age,
        sex, and disease metadata.
      </p>

      {summary ? (
        <div className={classes.tiles}>
          {tiles.map(({ icon, text }, index) => {
            const percent = (index / (tiles.length - 1)) * 100;
            const color = `color-mix(in hsl, var(--primary-light), ${percent}% var(--secondary-light))`;
            return (
              <div key={index} className={classes.tile}>
                {icon({ style: { color } })}
                <span>{text}</span>
              </div>
            );
          })}
        </div>
      ) : (
        <Placeholder height={150}>Loading overview...</Placeholder>
      )}

      <hr />
    </section>
  );
};

export default Overview;
