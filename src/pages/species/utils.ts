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

function trimTrailingZeros(value: string): string {
  return value.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
}

function abundanceDigits(value: number): number {
  const absValue = Math.abs(value);
  if (absValue >= 10) return 1;
  if (absValue >= 1) return 2;
  if (absValue >= 0.1) return 3;
  if (absValue >= 0.01) return 4;
  if (absValue >= 0.001) return 5;
  return 6;
}

export function formatAbundancePercent(value: number): string {
  if (!Number.isFinite(value) || value === 0) return "0%";
  const digits = abundanceDigits(value);
  const fixed = value.toFixed(digits);
  if (Number(fixed) === 0) {
    return value > 0 ? "<0.000001%" : ">-0.000001%";
  }
  return `${trimTrailingZeros(fixed)}%`;
}

export function formatAbundanceTick(value: number): string {
  if (!Number.isFinite(value) || value === 0) return "0%";
  return `${trimTrailingZeros(value.toFixed(abundanceDigits(value)))}%`;
}

export function buildKeggLink(pathwayId: string): string {
  return `https://www.kegg.jp/pathway/${pathwayId}`;
}
