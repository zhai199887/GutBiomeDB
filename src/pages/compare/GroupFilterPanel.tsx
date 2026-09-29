import type { ChangeEvent } from "react";

import { useI18n } from "@/i18n";
import { AGE_GROUP_ZH, SEX_ZH, countryName } from "@/util/countries";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";

import classes from "../ComparePage.module.css";
import type { FacetOption, FilterOptions, GroupFilter, GroupFilterOptions, GroupSampleCount } from "./types";

interface Props {
  label: string;
  color: string;
  value: GroupFilter;
  onChange: (filter: GroupFilter) => void;
  options: FilterOptions | null;
  dynamicOptions?: GroupFilterOptions | null;
  sampleCount: GroupSampleCount | null;
  optionsLoading?: boolean;
}

const GroupFilterPanel = ({
  label,
  color,
  value,
  onChange,
  options,
  dynamicOptions,
  sampleCount,
  optionsLoading = false,
}: Props) => {
  const { t, locale } = useI18n();

  const setSelect = (key: keyof GroupFilter) => (event: ChangeEvent<HTMLSelectElement | HTMLInputElement>) => {
    onChange({ ...value, [key]: event.target.value });
  };

  const ageName = (name: string) => (
    locale === "zh" ? (AGE_GROUP_ZH[name] ?? name.replace(/_/g, " ")) : name.replace(/_/g, " ")
  );
  const sexName = (name: string) => (locale === "zh" ? (SEX_ZH[name] ?? name) : name);

  const fallback = (values: string[]): FacetOption[] => values.map((option) => ({
    value: option,
    metadata_n: 0,
    abundance_n: 0,
  }));
  const facet = (field: keyof GroupFilterOptions, values: string[]) =>
    dynamicOptions?.[field] ?? fallback(values);
  const countryOptions = facet("country", options?.countries ?? []);
  const diseaseOptions = facet("disease", options?.diseases ?? []);
  const ageOptions = facet("age_group", options?.age_groups ?? []);
  const sexOptions = facet("sex", options?.sexes ?? []);
  const hasDynamicOptions = Boolean(dynamicOptions && Object.keys(dynamicOptions).length > 0);
  const optionLabel = (option: FacetOption, label: string) =>
    hasDynamicOptions ? `${label} (${option.abundance_n.toLocaleString()})` : label;
  const optionDisabled = (option: FacetOption, current: string) =>
    Boolean(hasDynamicOptions && option.abundance_n === 0 && option.value !== current);
  const keepCurrent = (items: FacetOption[], current: string) =>
    current && !items.some((item) => item.value === current)
      ? [...items, { value: current, metadata_n: 0, abundance_n: 0 }]
      : items;
  const visibleDiseaseOptions = hasDynamicOptions
    ? keepCurrent(diseaseOptions, value.disease).filter(
      (option) => option.abundance_n > 0 || option.value === value.disease,
    )
    : diseaseOptions;
  const noMatchingSamples = Boolean(sampleCount && sampleCount.abundance_n === 0);

  return (
    <div className={classes.groupPanel}>
      <div className={classes.groupHeader}>
        <h3 className={classes.groupLabel} style={{ borderColor: color, color }}>
          {label}
        </h3>
        <div className={classes.countBadge}>
          {sampleCount ? `n=${sampleCount.abundance_n}` : "n=..."}
        </div>
      </div>

      <div className={classes.fieldRow}>
        <label>{t("compare.country")}</label>
        <select value={value.country} onChange={setSelect("country")} className={classes.select} disabled={optionsLoading}>
          <option value="">{t("compare.any")}</option>
          {keepCurrent(countryOptions, value.country).map((option) => (
            <option key={option.value} value={option.value} disabled={optionDisabled(option, value.country)}>
              {optionLabel(option, countryName(option.value, locale))}
            </option>
          ))}
        </select>
      </div>

      <div className={classes.fieldRow}>
        <label>{t("compare.disease")}</label>
        <select value={value.disease} onChange={setSelect("disease")} className={classes.select} disabled={optionsLoading}>
          <option value="">{t("compare.any")}</option>
          {visibleDiseaseOptions.map((option) => (
            <option
              key={option.value}
              value={option.value}
              disabled={optionDisabled(option, value.disease)}
            >
              {optionLabel(option, diseaseDisplayNameI18n(option.value, locale))}
            </option>
          ))}
        </select>
      </div>

      <div className={classes.fieldRow}>
        <label>{t("compare.ageGroup")}</label>
        <select value={value.age_group} onChange={setSelect("age_group")} className={classes.select} disabled={optionsLoading}>
          <option value="">{t("compare.any")}</option>
          {keepCurrent(ageOptions, value.age_group).map((option) => (
            <option key={option.value} value={option.value} disabled={optionDisabled(option, value.age_group)}>
              {optionLabel(option, ageName(option.value))}
            </option>
          ))}
        </select>
      </div>

      <div className={classes.fieldRow}>
        <label>{t("compare.sex")}</label>
        <select value={value.sex} onChange={setSelect("sex")} className={classes.select} disabled={optionsLoading}>
          <option value="">{t("compare.any")}</option>
          {keepCurrent(sexOptions, value.sex).map((option) => (
            <option key={option.value} value={option.value} disabled={optionDisabled(option, value.sex)}>
              {optionLabel(option, sexName(option.value))}
            </option>
          ))}
        </select>
      </div>

      <div className={classes.groupMeta}>
        {sampleCount ? `${sampleCount.metadata_n.toLocaleString()} metadata / ${sampleCount.abundance_n.toLocaleString()} abundance` : t("compare.previewing")}
      </div>
      {noMatchingSamples ? (
        <div className={classes.error} role="status">
          {locale === "zh" ? "当前筛选组合没有可用样本，请清除一个条件。" : "No samples match this filter combination. Clear one condition."}
        </div>
      ) : null}
    </div>
  );
};

export default GroupFilterPanel;
