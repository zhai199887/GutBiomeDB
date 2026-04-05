import { useMemo, useState } from "react";
import { useI18n } from "@/i18n";
import { useData, setFilters, resetFilters, DEFAULT_FILTERS } from "@/data";
import { formatNumber } from "@/util/string";
import { diseaseShortNameI18n } from "@/util/diseaseNames";
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
  const [showAllDiseases, setShowAllDiseases] = useState(false);

  if (!summary) return null;

  const allDiseases = useMemo(
    () =>
      Object.keys(summary.disease_counts)
        .filter((d) => d !== "unknown" && d !== "NC")
        .sort((a, b) => (summary.disease_counts[b] ?? 0) - (summary.disease_counts[a] ?? 0)),
    [summary],
  );

  const displayDiseases = useMemo(() => {
    if (diseaseSearch.trim()) {
      const query = diseaseSearch.trim().toLowerCase();
      return allDiseases
        .filter((d) => d.toLowerCase().includes(query))
        .slice(0, 30);
    }
    return showAllDiseases ? allDiseases : summary.top20_diseases;
  }, [allDiseases, diseaseSearch, showAllDiseases, summary.top20_diseases]);

  const countFiltered = () => {
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
          <div className={classes.buttons}>
            {displayDiseases.map((d) => (
              <button
                key={d}
                className={classes.btn}
                data-active={filters.diseases.includes(d)}
                onClick={() => toggleDisease(d)}
                title={d}
              >
                {diseaseShortNameI18n(d, locale, 20)}
              </button>
            ))}
          </div>
          {!diseaseSearch.trim() && (
            <button
              type="button"
              className={classes.toggleAll}
              onClick={() => setShowAllDiseases((current) => !current)}
            >
              {showAllDiseases ? t("filter.showLess") : t("filter.showAll", { n: String(allDiseases.length) })}
            </button>
          )}
        </div>
      </div>
    </section>
  );
};

export default FilterPanel;
