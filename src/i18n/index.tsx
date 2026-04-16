/**
 * Lightweight i18n system using React Context
 */
import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import { translations, type Locale, type TranslationKey } from "./locales";

interface I18nContextValue {
  locale: Locale;
  setLocale: (l: Locale) => void;
  t: (key: TranslationKey, vars?: Record<string, string>) => string;
}

const I18nContext = createContext<I18nContextValue>({
  locale: "en",
  setLocale: () => {},
  t: (key) => key,
});

const STORAGE_KEY = "gut-atlas-locale";
const TRANSLATION_OVERRIDES: Partial<Record<Locale, Partial<Record<TranslationKey, string>>>> = {
  en: {
    "overview.diseaseTypes": "condition categories",
    "apiDocs.overviewText": "The GutBiomeDB API provides programmatic access to 168,464 human gut microbiome samples across 4,680 taxa, 72 countries, and 225 condition categories, including one NC control category. All endpoints return JSON responses.",
    "cite.dataSourcesText": "GutBiomeDB integrates 168,464 human gut 16S rRNA gene sequencing samples from public repositories including NCBI SRA, ENA, and DDBJ. Samples span 72 countries, 224 non-NC condition labels plus one NC category (225 total), and 8 life stages from Infant to Centenarian.",
    "cite.statDiseases": "Condition categories",
  },
  zh: {
    "overview.diseaseTypes": "条件类别",
    "apiDocs.overviewText": "GutBiomeDB API 提供对 168,464 份人类肠道微生物样本的编程访问，涵盖 4,680 个分类单元、72 个国家和 225 个条件类别，其中包含 1 个 NC 对照类别。所有端点返回 JSON 格式数据。",
    "cite.dataSourcesText": "GutBiomeDB 整合了来自 NCBI SRA、ENA 和 DDBJ 等公共数据库的 168,464 份人类肠道 16S rRNA 基因测序样本，覆盖 72 个国家、224 个非 NC 条件标签加 1 个 NC 类别（共 225 个），以及从婴儿到百岁老人的 8 个生命阶段。",
    "cite.statDiseases": "条件类别",
  },
};

function getInitialLocale(): Locale {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === "en" || saved === "zh") return saved;
  } catch { /* SSR or blocked storage */ }
  // Auto-detect browser language
  const browserLang = navigator.language ?? "";
  return browserLang.startsWith("zh") ? "zh" : "en";
}

export const I18nProvider = ({ children }: { children: ReactNode }) => {
  const [locale, setLocaleState] = useState<Locale>(() => {
    const l = getInitialLocale();
    document.documentElement.lang = l === "zh" ? "zh-CN" : "en";
    return l;
  });

  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l);
    try { localStorage.setItem(STORAGE_KEY, l); } catch { /* noop */ }
    document.documentElement.lang = l === "zh" ? "zh-CN" : "en";
  }, []);

  const t = useCallback(
    (key: TranslationKey, vars?: Record<string, string>) => {
      let s = TRANSLATION_OVERRIDES[locale]?.[key] ?? translations[locale][key] ?? key;
      if (vars) for (const [k, v] of Object.entries(vars)) s = s.replace(`{${k}}`, v);
      return s;
    },
    [locale],
  );

  return (
    <I18nContext.Provider value={{ locale, setLocale, t }}>
      {children}
    </I18nContext.Provider>
  );
};

export const useI18n = () => useContext(I18nContext);
export type { Locale, TranslationKey };