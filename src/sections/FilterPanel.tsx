import { useData, setFilters, resetFilters, DEFAULT_FILTERS } from "@/data";
import { formatNumber } from "@/util/string";
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
  const summary = useData((s) => s.summary);
  const filters = useData((s) => s.filters);

  if (!summary) return null;

  /** count samples matching current filters (approximation from summary) */
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

  return (
    <section className={classes.panel}>
      <div className={classes.header}>
        <h2>Filter</h2>
        <span className={classes.count}>
          Showing{" "}
          <b>{formatNumber(isDefault ? summary.total_samples : filteredCount, false)}</b>
          {" "}of{" "}
          <b>{formatNumber(summary.total_samples, false)}</b> samples
        </span>
        {!isDefault && (
          <button className={classes.reset} onClick={resetFilters}>
            Reset filters
          </button>
        )}
      </div>

      <div className={classes.row}>
        {/* Sex filter */}
        <div className={classes.group}>
          <label className={classes.label}>Sex</label>
          <div className={classes.buttons}>
            {(["all", "female", "male", "unknown"] as const).map((s) => (
              <button
                key={s}
                className={classes.btn}
                data-active={filters.sex === s}
                onClick={() => setFilters({ sex: s })}
              >
                {s === "all" ? "All" : s.charAt(0).toUpperCase() + s.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Age group filter */}
        <div className={classes.group}>
          <label className={classes.label}>Age Group</label>
          <div className={classes.buttons}>
            {AGE_GROUPS.map((g) => (
              <button
                key={g}
                className={classes.btn}
                data-active={filters.age_groups.includes(g)}
                onClick={() => toggleAge(g)}
              >
                {g.replace("_", " ")}
              </button>
            ))}
          </div>
        </div>

        {/* Disease filter */}
        <div className={classes.group}>
          <label className={classes.label}>Disease (Top 20)</label>
          <div className={classes.buttons}>
            {summary.top20_diseases.map((d) => (
              <button
                key={d}
                className={classes.btn}
                data-active={filters.diseases.includes(d)}
                onClick={() => toggleDisease(d)}
                title={d}
              >
                {d.length > 20 ? d.slice(0, 18) + "…" : d}
              </button>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
};

export default FilterPanel;
