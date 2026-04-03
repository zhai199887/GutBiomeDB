/**
 * GroupFilterPanel – filter controls for one sample group
 * 组筛选面板 – 一个样本组的筛选控件
 */
import { useEffect, useState } from "react";
import type { GroupFilter, FilterOptions } from "./types";
import { useI18n } from "@/i18n";
import { countryName, AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
import classes from "../ComparePage.module.css";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

const GroupFilterPanel = ({
  label,
  color,
  value,
  onChange,
  options,
}: {
  label: string;
  color: string;
  value: GroupFilter;
  onChange: (f: GroupFilter) => void;
  options: FilterOptions | null;
}) => {
  const { t, locale } = useI18n();
  const [diseaseZh, setDiseaseZh] = useState<Record<string, string>>({});

  // Load disease Chinese names / 加载疾病中文名
  useEffect(() => {
    if (locale === "zh") {
      fetch(`${API_BASE}/api/disease-names-zh`)
        .then((r) => r.json())
        .then(setDiseaseZh)
        .catch(() => {});
    }
  }, [locale]);

  const set = (key: keyof GroupFilter) => (e: React.ChangeEvent<HTMLSelectElement>) =>
    onChange({ ...value, [key]: e.target.value });

  const dName = (name: string) => (locale === "zh" && diseaseZh[name]) ? diseaseZh[name] : name;
  const ageName = (name: string) => locale === "zh" ? (AGE_GROUP_ZH[name] ?? name.replace(/_/g, " ")) : name.replace(/_/g, " ");
  const sexName = (name: string) => locale === "zh" ? (SEX_ZH[name] ?? name) : name;

  return (
    <div className={classes.groupPanel}>
      <h3 className={classes.groupLabel} style={{ borderColor: color, color }}>
        {label}
      </h3>
      <div className={classes.fieldRow}>
        <label>{t("compare.country")}</label>
        <select value={value.country} onChange={set("country")} className={classes.select}>
          <option value="">{t("compare.any")}</option>
          {options?.countries.map((c) => (
            <option key={c} value={c}>{countryName(c, locale)} ({c})</option>
          ))}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>{t("compare.disease")}</label>
        <select value={value.disease} onChange={set("disease")} className={classes.select}>
          <option value="">{t("compare.any")}</option>
          {options?.diseases.slice(0, 200).map((d) => {
            const display = dName(d);
            return <option key={d} value={d}>{display.length > 40 ? display.slice(0, 38) + "…" : display}</option>;
          })}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>{t("compare.ageGroup")}</label>
        <select value={value.age_group} onChange={set("age_group")} className={classes.select}>
          <option value="">{t("compare.any")}</option>
          {options?.age_groups.map((a) => (
            <option key={a} value={a}>{ageName(a)}</option>
          ))}
        </select>
      </div>
      <div className={classes.fieldRow}>
        <label>{t("compare.sex")}</label>
        <select value={value.sex} onChange={set("sex")} className={classes.select}>
          <option value="">{t("compare.any")}</option>
          {options?.sexes.map((s) => (
            <option key={s} value={s}>{sexName(s)}</option>
          ))}
        </select>
      </div>
    </div>
  );
};

export default GroupFilterPanel;
