import { countryName, AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";
import { diseaseShortNameI18n } from "@/util/diseaseNames";

export type DimensionType = "disease" | "country" | "age" | "sex";

export function translateDimensionName(name: string, locale: string, type: DimensionType): string {
  if (type === "disease") return diseaseShortNameI18n(name, locale, 36);
  if (type === "country") return countryName(name, locale);
  if (type === "age") return locale === "zh" ? (AGE_GROUP_ZH[name] ?? name.replace(/_/g, " ")) : name.replace(/_/g, " ");
  if (type === "sex") return locale === "zh" ? (SEX_ZH[name] ?? name) : name;
  return name;
}

export function formatPValue(value: number | undefined): string {
  if (value == null || Number.isNaN(value)) return "NA";
  if (value === 0) return "<1e-300";
  if (value < 0.001) return value.toExponential(2);
  return value.toFixed(4);
}

export function starsForPValue(value: number | undefined): string {
  if (value == null || Number.isNaN(value)) return "";
  if (value < 0.001) return "***";
  if (value < 0.01) return "**";
  if (value < 0.05) return "*";
  return "";
}

export function formatPercent(value: number, digits = 2): string {
  return `${value.toFixed(digits)}%`;
}

export function formatPrevalence(value: number, digits = 1): string {
  return `${(value * 100).toFixed(digits)}%`;
}

export function buildKeggLink(pathwayId: string): string {
  return `https://www.kegg.jp/pathway/${pathwayId}`;
}
