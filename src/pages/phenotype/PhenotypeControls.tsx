/**
 * PhenotypeControls — dimension / group / taxonomy-level selectors
 */
import { useEffect, useState, useCallback } from "react";
import { useI18n } from "@/i18n";
import { diseaseDisplayNameI18n } from "@/util/diseaseNames";
import { AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
import {
  API_BASE,
  type DimType,
  type TaxLevel,
  type PhenotypeGroup,
} from "./types";

interface Props {
  dimType: DimType;
  groupA: string;
  groupB: string;
  taxLevel: TaxLevel;
  minPrevalence: number;
  loading: boolean;
  onDimChange: (d: DimType) => void;
  onGroupAChange: (g: string) => void;
  onGroupBChange: (g: string) => void;
  onTaxLevelChange: (l: TaxLevel) => void;
  onMinPrevalenceChange: (v: number) => void;
  onAnalyze: () => void;
}

const STATIC_AGE_GROUPS = [
  "Infant", "Child", "Adolescent", "Adult", "Older_Adult", "Oldest_Old", "Centenarian",
];
const STATIC_SEX_GROUPS = ["female", "male"];

export default function PhenotypeControls({
  dimType, groupA, groupB, taxLevel, minPrevalence, loading,
  onDimChange, onGroupAChange, onGroupBChange, onTaxLevelChange,
  onMinPrevalenceChange, onAnalyze,
}: Props) {
  const { t, locale } = useI18n();
  const [groups, setGroups] = useState<PhenotypeGroup[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);

  const labelOf = useCallback((g: string) => {
    if (dimType === "disease") return diseaseDisplayNameI18n(g, locale);
    if (locale === "zh") {
      if (dimType === "age") return AGE_GROUP_ZH[g] ?? g.replace(/_/g, " ");
      if (dimType === "sex") return SEX_ZH[g] ?? g;
    }
    return g.replace(/_/g, " ");
  }, [dimType, locale]);

  // Load dynamic groups for disease; use static lists for age/sex
  useEffect(() => {
    let cancelled = false;
    if (dimType === "disease") {
      setGroups([]);
      setGroupsLoading(true);
      fetch(`${API_BASE}/api/phenotype-groups?dim_type=disease`)
        .then(r => r.json())
        .then(data => {
          if (cancelled) return;
          setGroups(data.groups ?? []);
          setGroupsLoading(false);
        })
        .catch(() => {
          if (cancelled) return;
          setGroups([]);
          setGroupsLoading(false);
        });
    } else if (dimType === "age") {
      setGroupsLoading(false);
      setGroups(STATIC_AGE_GROUPS.map(g => ({ group: g, sample_count: 0 })));
    } else {
      setGroupsLoading(false);
      setGroups(STATIC_SEX_GROUPS.map(g => ({ group: g, sample_count: 0 })));
    }
    return () => {
      cancelled = true;
    };
  }, [dimType]);

  const groupOptions = dimType === "disease"
    ? groups
    : dimType === "age"
      ? STATIC_AGE_GROUPS.map(g => ({ group: g, sample_count: 0 }))
      : STATIC_SEX_GROUPS.map(g => ({ group: g, sample_count: 0 }));

  const selectStyle: React.CSSProperties = {
    background: "var(--dark-gray)",
    border: "1px solid var(--gray)",
    color: "var(--light-gray)",
    borderRadius: "4px",
    padding: "0.35rem 0.6rem",
    minWidth: "160px",
    maxWidth: "260px",
  };

  const labelStyle: React.CSSProperties = {
    fontSize: "0.75rem",
    textTransform: "uppercase",
    color: "var(--light-gray)",
    marginBottom: "0.3rem",
    letterSpacing: "0.04em",
  };

  return (
    <div style={{ display: "flex", gap: "1.2rem", flexWrap: "wrap", alignItems: "flex-end", marginBottom: "1.5rem" }}>

      {/* Dimension */}
      <div>
        <div style={labelStyle}>{t("phenotype.compareBy")}</div>
        <div style={{ display: "flex", gap: "0.35rem" }}>
          {(["age", "sex", "disease"] as DimType[]).map(d => (
            <button key={d} onClick={() => onDimChange(d)} style={{
              background: dimType === d ? "var(--primary)" : "none",
              border: "1px solid var(--gray)",
              color: dimType === d ? "var(--black)" : "var(--light-gray)",
              borderRadius: "4px",
              padding: "0.3rem 0.75rem",
              cursor: "pointer",
              fontWeight: dimType === d ? 600 : 400,
            }}>
              {t(`phenotype.${d}`)}
            </button>
          ))}
        </div>
      </div>

      {/* Group A */}
      <div>
        <div style={labelStyle}>{locale === "zh" ? "组 A" : "GROUP A"}</div>
        <select value={groupA} onChange={e => onGroupAChange(e.target.value)} style={selectStyle} disabled={groupsLoading}>
          {groupsLoading && dimType === "disease" && (
            <option value={groupA}>{locale === "zh" ? "加载疾病分组中…" : "Loading disease groups..."}</option>
          )}
          {groupOptions.map(({ group, sample_count }) => (
            <option key={group} value={group}>
              {labelOf(group)}{sample_count > 0 ? ` (n=${sample_count.toLocaleString()})` : ""}
            </option>
          ))}
        </select>
      </div>

      <span style={{ color: "var(--light-gray)", paddingBottom: "0.3rem", fontSize: "0.9rem" }}>VS</span>

      {/* Group B */}
      <div>
        <div style={labelStyle}>{locale === "zh" ? "组 B" : "GROUP B"}</div>
        <select value={groupB} onChange={e => onGroupBChange(e.target.value)} style={selectStyle} disabled={groupsLoading}>
          {groupsLoading && dimType === "disease" && (
            <option value={groupB}>{locale === "zh" ? "加载疾病分组中…" : "Loading disease groups..."}</option>
          )}
          {groupOptions.map(({ group, sample_count }) => (
            <option key={group} value={group}>
              {labelOf(group)}{sample_count > 0 ? ` (n=${sample_count.toLocaleString()})` : ""}
            </option>
          ))}
        </select>
      </div>

      {/* Tax level */}
      <div>
        <div style={labelStyle}>{t("phenotype.controls.taxLevel")}</div>
        <div style={{ display: "flex", gap: "0.35rem" }}>
          {(["genus", "phylum"] as TaxLevel[]).map(lv => (
            <button key={lv} onClick={() => onTaxLevelChange(lv)} style={{
              background: taxLevel === lv ? "var(--secondary)" : "none",
              border: "1px solid var(--gray)",
              color: taxLevel === lv ? "var(--black)" : "var(--light-gray)",
              borderRadius: "4px",
              padding: "0.3rem 0.6rem",
              cursor: "pointer",
              textTransform: "capitalize",
            }}>
              {lv}
            </button>
          ))}
        </div>
      </div>

      {/* Min prevalence */}
      <div>
        <div style={labelStyle}>
          {t("phenotype.controls.minPrevalence")} ({Math.round(minPrevalence * 100)}%)
        </div>
        <input
          type="range" min={0} max={0.5} step={0.05}
          value={minPrevalence}
          onChange={e => onMinPrevalenceChange(Number(e.target.value))}
          style={{ width: "100px", accentColor: "var(--primary)" }}
        />
      </div>

      {/* Run button */}
      <div>
        <button
          onClick={onAnalyze}
          disabled={loading || groupA === groupB}
          style={{
            background: "var(--primary)",
            border: "none",
            color: "var(--black)",
            borderRadius: "4px",
            padding: "0.45rem 1.2rem",
            cursor: loading || groupA === groupB ? "not-allowed" : "pointer",
            fontWeight: 600,
            opacity: loading || groupA === groupB ? 0.6 : 1,
          }}
        >
          {loading ? t("phenotype.loading.analysis") : t("phenotype.controls.analyze")}
        </button>
      </div>
    </div>
  );
}
