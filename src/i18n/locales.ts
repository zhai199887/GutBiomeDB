/**
 * i18n translation dictionaries — English & Chinese
 * 国际化翻译字典 — 英文 & 中文
 */

export type Locale = "en" | "zh";

const en = {
  // Header & Nav
  "nav.home": "Home",
  "nav.explorer": "Explorer",
  "nav.compare": "Compare",
  "nav.metabolism": "Metabolism",
  "nav.admin": "Admin",

  // Header
  "header.title": "Gut Microbiome Atlas",
  "header.subtitle": "Interactive exploration of 168,000+ human gut microbiome samples across age, sex, and disease dimensions.",

  // Overview
  "overview.title": "Overview",
  "overview.intro": "This platform lets you explore over 168,000 publicly available human gut microbiome samples, annotated with age, sex, and disease metadata.",
  "overview.methods": "Data & Methods:",
  "overview.methods.detail": "Taxonomic profiles generated with MetaPhlAn. Differential abundance uses Wilcoxon rank-sum test with Benjamini–Hochberg FDR correction (adj. p < 0.05). Beta diversity computed as Bray–Curtis dissimilarity, visualised by PCoA.",
  "overview.samples": "samples",
  "overview.countries": "countries",
  "overview.regions": "regions",
  "overview.diseaseTypes": "disease types",
  "overview.from": "from",
  "overview.dataVersion": "Data version:",
  "overview.lastUpdated": "Last updated:",

  // Feature cards
  "feature.phenotype.title": "Phenotype Explorer",
  "feature.phenotype.desc": "Compare microbiome composition across age groups, sexes, and diseases",
  "feature.compare.title": "Differential Analysis",
  "feature.compare.desc": "Statistical comparison between two user-defined sample groups (differential abundance, volcano plot, PCoA)",
  "feature.metabolism.title": "Metabolic Functions",
  "feature.metabolism.desc": "Browse microbiota by metabolic role: SCFAs, bile acids, tryptophan, TMAO, and more",

  // Filter panel
  "filter.title": "Filter",
  "filter.showing": "Showing",
  "filter.of": "of",
  "filter.samples": "samples",
  "filter.reset": "Reset filters",
  "filter.sex": "Sex",
  "filter.all": "All",
  "filter.female": "Female",
  "filter.male": "Male",
  "filter.unknown": "Unknown",
  "filter.ageGroup": "Age Group",
  "filter.disease": "Disease (Top 20)",

  // Search
  "search.title": "Search",
  "search.geography": "Geography",
  "search.sampleProject": "Sample / Project",
  "search.geoHint": "Search for a country or region.",
  "search.sampleHint": "Search by SRR accession or BioProject ID.",
  "search.sampleSub": "Projects show aggregated disease/age summaries across all their samples. Samples show individual annotations.",
  "search.placeholder": "Search",
  "search.type": "Type:",
  "search.items": "items",
  "search.noResults": "No results",
  "search.loading.geo": "Loading geography data...",
  "search.loading.sample": "Loading sample data...",

  // Table columns
  "col.name": "Name",
  "col.type": "Type",
  "col.samples": "Samples",
  "col.mainDisease": "Main Disease",
  "col.diseaseTypes": "Disease Types",
  "col.ageGroups": "Age Groups",
  "col.ageGroup": "Age Group",
  "col.sex": "Sex",
  "col.disease": "Disease",
  "col.country": "Country",

  // Compare page
  "compare.title": "Differential Analysis",
  "compare.subtitle": "Compare gut microbiome composition between two groups",
  "compare.back": "← Back to Atlas",
  "compare.groupA": "Group A",
  "compare.groupB": "Group B",
  "compare.country": "Country",
  "compare.disease": "Disease",
  "compare.ageGroup": "Age group",
  "compare.sex": "Sex",
  "compare.any": "-- Any --",
  "compare.taxLevel": "Taxonomy level",
  "compare.statTest": "Statistical test",
  "compare.run": "Run Analysis",
  "compare.analyzing": "Analyzing…",
  "compare.loading": "Loading filter options…",
  "compare.backendError": "Differential analysis backend is under development. Stay tuned!",
  "compare.vs": "vs",
  "compare.tab.bar": "Differential Abundance",
  "compare.tab.volcano": "Volcano Plot",
  "compare.tab.alpha": "Alpha Diversity",
  "compare.tab.beta": "Beta Diversity (PCoA)",
  "compare.tab.lefse": "LEfSe",
  "compare.tab.permanova": "PERMANOVA",
  "compare.export.csv": "Export CSV",
  "compare.export.svg": "Export SVG",

  // Phenotype page
  "phenotype.title": "Phenotype Association Analysis",
  "phenotype.subtitle": "Compare microbiome composition (Top 20 genera) between two groups.",
  "phenotype.back": "← Back to main",
  "phenotype.compareBy": "Compare by",
  "phenotype.age": "Age",
  "phenotype.sex": "Sex",
  "phenotype.disease": "Disease",
  "phenotype.loading": "Loading abundance data…",
  "phenotype.noData": "No data for selected groups",

  // Metabolism page
  "metabolism.title": "Metabolic Function Browser",
  "metabolism.subtitle": "Explore gut microbiota organized by metabolic role and clinical relevance",
  "metabolism.back": "← Back to Atlas",
  "metabolism.loading": "Loading metabolism data…",
  "metabolism.loadError": "Unable to load metabolism data.",
  "metabolism.categories": "Functional Categories",
  "metabolism.genera": "genera",
  "metabolism.memberGenera": "Member Genera",
  "metabolism.keyMetabolites": "Key Metabolites",
  "metabolism.relatedPathways": "Related Pathways",
  "metabolism.clinicalRelevance": "Clinical Relevance",
  "metabolism.avgAbundance": "Average Abundance in Dataset",
  "metabolism.heatmap": "Abundance by Disease (Heatmap)",
  "metabolism.references": "Key References",
  "metabolism.searchPlaceholder": "Search for a genus (e.g. Bifidobacterium)…",
  "metabolism.foundIn": "found in:",
  "metabolism.noCategory": "No category found for",
  "metabolism.noAbundance": "No abundance data available for this category",

  // Footer
  "footer.project": "A project of the",
  "footer.lab": "Gastroenterology Laboratory",
  "footer.university": "China Medical University",

  // Sankey
  "sankey.title": "Taxonomy Flow: Phylum → Genus",

  // Language switch
  "lang.switch": "中文",
} as const;

const zh: Record<keyof typeof en, string> = {
  // Header & Nav
  "nav.home": "首页",
  "nav.explorer": "探索",
  "nav.compare": "差异分析",
  "nav.metabolism": "代谢功能",
  "nav.admin": "管理",

  // Header
  "header.title": "肠道微生物图谱",
  "header.subtitle": "交互式探索超过168,000份人类肠道微生物样本，涵盖年龄、性别和疾病维度。",

  // Overview
  "overview.title": "概览",
  "overview.intro": "本平台提供超过168,000份公开的人类肠道微生物样本，标注了年龄、性别和疾病元数据。",
  "overview.methods": "数据与方法：",
  "overview.methods.detail": "分类学谱由MetaPhlAn生成。差异丰度使用Wilcoxon秩和检验结合Benjamini–Hochberg FDR校正（adj. p < 0.05）。Beta多样性使用Bray–Curtis距离，通过PCoA可视化。",
  "overview.samples": "个样本",
  "overview.countries": "个国家",
  "overview.regions": "个区域",
  "overview.diseaseTypes": "种疾病",
  "overview.from": "来自",
  "overview.dataVersion": "数据版本：",
  "overview.lastUpdated": "最后更新：",

  // Feature cards
  "feature.phenotype.title": "表型探索",
  "feature.phenotype.desc": "按年龄组、性别和疾病比较微生物组组成",
  "feature.compare.title": "差异分析",
  "feature.compare.desc": "两组样本间的统计比较（差异丰度、火山图、PCoA）",
  "feature.metabolism.title": "代谢功能",
  "feature.metabolism.desc": "按代谢角色浏览微生物：短链脂肪酸、胆汁酸、色氨酸、TMAO等",

  // Filter panel
  "filter.title": "筛选",
  "filter.showing": "显示",
  "filter.of": "/",
  "filter.samples": "个样本",
  "filter.reset": "重置筛选",
  "filter.sex": "性别",
  "filter.all": "全部",
  "filter.female": "女",
  "filter.male": "男",
  "filter.unknown": "未知",
  "filter.ageGroup": "年龄组",
  "filter.disease": "疾病（前20）",

  // Search
  "search.title": "搜索",
  "search.geography": "地理位置",
  "search.sampleProject": "样本 / 项目",
  "search.geoHint": "搜索国家或区域。",
  "search.sampleHint": "按SRR编号或BioProject ID搜索。",
  "search.sampleSub": "项目显示其所有样本的疾病/年龄汇总。样本显示单独标注。",
  "search.placeholder": "搜索",
  "search.type": "类型：",
  "search.items": "条",
  "search.noResults": "无结果",
  "search.loading.geo": "加载地理数据...",
  "search.loading.sample": "加载样本数据...",

  // Table columns
  "col.name": "名称",
  "col.type": "类型",
  "col.samples": "样本数",
  "col.mainDisease": "主要疾病",
  "col.diseaseTypes": "疾病种类",
  "col.ageGroups": "年龄组",
  "col.ageGroup": "年龄组",
  "col.sex": "性别",
  "col.disease": "疾病",
  "col.country": "国家",

  // Compare page
  "compare.title": "差异分析",
  "compare.subtitle": "比较两组间的肠道微生物组组成",
  "compare.back": "← 返回首页",
  "compare.groupA": "A 组",
  "compare.groupB": "B 组",
  "compare.country": "国家",
  "compare.disease": "疾病",
  "compare.ageGroup": "年龄组",
  "compare.sex": "性别",
  "compare.any": "-- 不限 --",
  "compare.taxLevel": "分类层级",
  "compare.statTest": "统计检验",
  "compare.run": "开始分析",
  "compare.analyzing": "分析中…",
  "compare.loading": "加载筛选选项…",
  "compare.backendError": "差异分析后端正在开发中，敬请期待！",
  "compare.vs": "vs",
  "compare.tab.bar": "差异丰度",
  "compare.tab.volcano": "火山图",
  "compare.tab.alpha": "Alpha多样性",
  "compare.tab.beta": "Beta多样性 (PCoA)",
  "compare.tab.lefse": "LEfSe",
  "compare.tab.permanova": "PERMANOVA",
  "compare.export.csv": "导出CSV",
  "compare.export.svg": "导出SVG",

  // Phenotype page
  "phenotype.title": "表型关联分析",
  "phenotype.subtitle": "比较两组间的微生物组组成（前20属）。",
  "phenotype.back": "← 返回首页",
  "phenotype.compareBy": "比较维度",
  "phenotype.age": "年龄",
  "phenotype.sex": "性别",
  "phenotype.disease": "疾病",
  "phenotype.loading": "加载丰度数据…",
  "phenotype.noData": "所选分组无数据",

  // Metabolism page
  "metabolism.title": "代谢功能浏览器",
  "metabolism.subtitle": "按代谢角色和临床相关性探索肠道微生物",
  "metabolism.back": "← 返回首页",
  "metabolism.loading": "加载代谢数据…",
  "metabolism.loadError": "无法加载代谢数据。",
  "metabolism.categories": "功能分类",
  "metabolism.genera": "个属",
  "metabolism.memberGenera": "成员属",
  "metabolism.keyMetabolites": "关键代谢物",
  "metabolism.relatedPathways": "相关通路",
  "metabolism.clinicalRelevance": "临床意义",
  "metabolism.avgAbundance": "数据集中的平均丰度",
  "metabolism.heatmap": "疾病分布热图",
  "metabolism.references": "关键参考文献",
  "metabolism.searchPlaceholder": "搜索属名（如 Bifidobacterium）…",
  "metabolism.foundIn": "存在于：",
  "metabolism.noCategory": "未找到相关类别",
  "metabolism.noAbundance": "该类别无丰度数据",

  // Footer
  "footer.project": "项目所属",
  "footer.lab": "消化内科实验室",
  "footer.university": "中国医科大学",

  // Sankey
  "sankey.title": "分类流向：门 → 属",

  // Language switch
  "lang.switch": "EN",
};

export const translations = { en, zh } as const;
export type TranslationKey = keyof typeof en;
