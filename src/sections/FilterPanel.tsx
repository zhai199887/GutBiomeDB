import { useMemo, useState } from "react";
import { useI18n } from "@/i18n";
import { useData, setFilters, resetFilters, DEFAULT_FILTERS } from "@/data";
import { formatNumber } from "@/util/string";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { AGE_GROUP_ZH } from "@/util/countries";
import classes from "./FilterPanel.module.css";

const AGE_GROUPS = [
  "Infant",
  "Child",
  "Adolescent",
  "Adult",
  "Older_Adult",
  "Oldest_Old",
  "Centenarian",
  "Unknown",
];

const FilterPanel = () => {
  const { t, locale } = useI18n();
  const summary = useData((s) => s.summary);
  const filters = useData((s) => s.filters);
  const [diseaseSearch, setDiseaseSearch] = useState("");
  const diseaseCountMap = useMemo(() => {
    if (!summary) return {} as Record<string, number>;
    const next: Record<string, number> = { ...summary.disease_counts };
    const ncCount = summary.healthy_control_counts?.NC;
    if (typeof ncCount === "number" && ncCount > 0) {
      next.NC = ncCount;
    }
    return next;
  }, [summary]);
  const allDiseases = useMemo(
    () =>
      summary
        ? Object.keys(diseaseCountMap)
            .filter((d) => d !== "unknown")
            .sort((a, b) => (diseaseCountMap[b] ?? 0) - (diseaseCountMap[a] ?? 0))
        : [],
    [diseaseCountMap, summary],
  );

  const displayDiseases = useMemo(() => {
    if (diseaseSearch.trim()) {
      const query = diseaseSearch.trim().toLowerCase();
      return allDiseases
        .filter((d) => {
          const displayName = diseaseDisplayNameI18n(d, locale).toLowerCase();
          return d.toLowerCase().includes(query) || displayName.includes(query);
        });
    }
    return allDiseases;
  }, [allDiseases, diseaseSearch, locale]);

  const countFiltered = () => {
    if (!summary) return 0;
    let total = summary.total_samples;
    if (filters.sex !== "all") {
      total = summary.sex_counts[filters.sex] ?? 0;
    }
    if (filters.age_groups.length > 0) {
      const base = filters.sex !== "all" ? total : summary.total_samples;
      const ageTotal = filters.age_groups.reduce(
        (s, g) => s + (summary.age_counts[g] ?? 0),
        0,
      );
      total = Math.round((ageTotal / summary.total_samples) * base);
    }
    return total;
  };

  const toggleAge = (group: string) => {
    const cur = filters.age_groups;
    setFilters({
      age_groups: cur.includes(group)
        ? cur.filter((g) => g !== group)
        : [...cur, group],
    });
  };

  const toggleDisease = (disease: string) => {
    const cur = filters.diseases;
    setFilters({
      diseases: cur.includes(disease)
        ? cur.filter((d) => d !== disease)
        : [...cur, disease],
    });
  };

  const isDefault =
    filters.sex === DEFAULT_FILTERS.sex &&
    filters.age_groups.length === 0 &&
    filters.diseases.length === 0;

  const filteredCount = countFiltered();

  const SEX_LABELS: Record<string, string> = {
    all: t("filter.all"),
    female: t("filter.female"),
    male: t("filter.male"),
    unknown: t("filter.unknown"),
  };

  if (!summary) return null;

  return (
    <section className={classes.panel}>
      <div className={classes.header}>
        <h2>{t("filter.title")}</h2>
        <span className={classes.count}>
          {t("filter.showing")}{" "}
          <b>{formatNumber(isDefault ? summary.total_samples : filteredCount, false)}</b>
          {" "}{t("filter.of")}{" "}
          <b>{formatNumber(summary.total_samples, false)}</b> {t("filter.samples")}
        </span>
        {!isDefault && (
          <button className={classes.reset} onClick={resetFilters}>
            {t("filter.reset")}
          </button>
        )}
      </div>

      <div className={classes.row}>
        {/* Sex filter */}
        <div className={classes.group}>
          <label className={classes.label}>{t("filter.sex")}</label>
          <div className={classes.buttons}>
            {(["all", "female", "male", "unknown"] as const).map((s) => (
              <button
                key={s}
                className={classes.btn}
                data-active={filters.sex === s}
                onClick={() => setFilters({ sex: s })}
              >
                {SEX_LABELS[s] ?? s}
              </button>
            ))}
          </div>
        </div>

        {/* Age group filter */}
        <div className={classes.group}>
          <label className={classes.label}>{t("filter.ageGroup")}</label>
          <div className={classes.buttons}>
            {AGE_GROUPS.map((g) => (
              <button
                key={g}
                className={classes.btn}
                data-active={filters.age_groups.includes(g)}
                onClick={() => toggleAge(g)}
              >
                {locale === "zh" ? (AGE_GROUP_ZH[g] ?? g.replace("_", " ")) : g.replace("_", " ")}
              </button>
            ))}
          </div>
        </div>

        {/* Disease filter */}
        <div className={classes.group}>
          <label className={classes.label}>{t("filter.disease")}</label>
          <input
            className={classes.diseaseSearch}
            type="text"
            placeholder={t("filter.searchDisease")}
            value={diseaseSearch}
            onChange={(event) => setDiseaseSearch(event.target.value)}
          />
          <div className={classes.buttons} data-variant="disease">
            {displayDiseases.map((d) => (
              <button
                key={d}
                className={classes.btn}
                data-active={filters.diseases.includes(d)}
                onClick={() => toggleDisease(d)}
                title={diseaseDisplayNameI18n(d, locale)}
              >
                {diseaseDisplayNameI18n(d, locale)}
              </button>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
};

export default FilterPanel;
