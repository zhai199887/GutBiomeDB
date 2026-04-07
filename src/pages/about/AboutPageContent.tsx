import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import Header from "@/sections/Header";
import Footer from "@/sections/Footer";
import faqEn from "./faq.en.json";
import faqZh from "./faq.zh.json";
import classes from "../CitePage.module.css";

const PAPER_BIBTEX = `@unpublished{zhai2026gutmicrobiomeatlas,
  title   = {Gut Microbiome Atlas: an analytical platform for 168,464 human gut microbiome samples},
  author  = {Zhai, Jinxia and Li, Yingjie and Liu, Jiameng and Su, Xinyi and Cui, Runze and Zheng, Dianyu and Sun, Yuhan and Yu, Jingsheng and Dai, Cong},
  note    = {Manuscript in preparation},
  year    = {2026}
}`;

const SOFTWARE_BIBTEX = `@software{gutmicrobiomeatlas2026,
  title   = {Gut Microbiome Atlas},
  author  = {Zhai, Jinxia and Dai, Cong and collaborators},
  year    = {2026},
  url     = {https://github.com/zhai199887/gut-microbiome-atlas},
  note    = {Research platform source code}
}`;

type StatsPayload = {
  total_samples?: number;
  total_countries?: number;
  total_diseases?: number;
  total_non_nc_condition_labels?: number;
  total_condition_categories?: number;
  total_projects?: number;
  total_genera?: number;
  version?: string;
  last_updated?: string;
};

type FaqItem = {
  question: string;
  answer: string;
};

type Copy = {
  back: string;
  title: string;
  subtitle: string;
  sections: {
    team: string;
    data: string;
    cite: string;
    funding: string;
    license: string;
    fair: string;
    related: string;
    faq: string;
    version: string;
    privacy: string;
  };
  teamIntro: string;
  teamAffiliations: string;
  dataIntro: string;
  sourceLabel: string;
  pipelineLabel: string;
  statsLabel: string;
  citeIntro: string;
  paper: string;
  software: string;
  copy: string;
  copied: string;
  fundingText: string;
  licenseProcessed: string;
  licenseSource: string;
  licenseRaw: string;
  fairItems: string[];
  relatedLinks: Array<{ label: string; url: string; desc: string }>;
  versionRows: Array<{ date: string; label: string; detail: string }>;
  privacyText: string;
  disclaimerText: string;
  contactLabel: string;
  noteLabel: string;
  statsCards: Array<{ key: keyof StatsPayload | "contact"; label: string; fallback: string }>;
};

const COPY: Record<"en" | "zh", Copy> = {
  en: {
    back: "Back to Home",
    title: "About & Cite",
    subtitle: "Research context, licensing, citation guidance, and operations notes for the active Gut Microbiome Atlas deployment.",
    sections: {
      team: "Research Team",
      data: "Data Sources / Pipeline / Statistics",
      cite: "How to Cite",
      funding: "Funding & Acknowledgments",
      license: "Data License",
      fair: "FAIR Data Principles",
      related: "Related Resources",
      faq: "FAQ",
      version: "Version History",
      privacy: "Privacy / Disclaimer",
    },
    teamIntro:
      "The platform is maintained as a collaborative gut microbiome analysis resource anchored to the author group of the Gut Microbiome Atlas manuscript. Team descriptions below follow verifiable author and affiliation information only; no speculative role labels are added.",
    teamAffiliations:
      "Primary affiliations include the Department of Gastroenterology, The First Hospital of China Medical University; The First Affiliated Hospital of Jinzhou Medical University; and Peking Union Medical College Hospital.",
    dataIntro:
      "Gut Microbiome Atlas integrates processed human gut microbiome cohorts retrieved from public repositories and harmonizes them into a genus-level analysis layer for cross-study exploration, visualization, and hypothesis generation. The current release covers 225 non-NC condition labels plus one NC category, yielding 226 condition categories in total.",
    sourceLabel: "Data Sources",
    pipelineLabel: "Processing Pipeline",
    statsLabel: "Current Dataset Snapshot",
    citeIntro:
      "Use the paper citation for manuscripts that describe the atlas scientifically, and the software citation for workflows or reproducible computational usage of the platform itself.",
    paper: "Paper",
    software: "Software",
    copy: "Copy BibTeX",
    copied: "Copied",
    fundingText:
      "This work was supported by the National Natural Science Foundation of China (grant No.82270571 and grant No.82570632 to Cong Dai).",
    licenseProcessed:
      "Processed atlas-level statistics and derived aggregate visual outputs are described under a CC BY 4.0 sharing model.",
    licenseSource:
      "Platform source code is released under the MIT License, following the repository license in the active main workspace.",
    licenseRaw:
      "Original raw sequencing data remain governed by the terms of their original repositories, BioProject records, and accession-specific reuse restrictions.",
    fairItems: [
      "Findable: project-level identifiers, disease labels, and genus entities are surfaced through searchable interfaces and documented APIs.",
      "Accessible: core derived statistics are exposed through the web UI, human-readable API docs, and OpenAPI specification.",
      "Interoperable: outputs are exported in JSON, CSV, and TSV using stable field names for downstream scripting.",
      "Reusable: methods, caveats, and licensing boundaries are documented so the derived atlas outputs can be reused responsibly.",
    ],
    relatedLinks: [
      { label: "GitHub", url: "https://github.com/zhai199887/gut-microbiome-atlas", desc: "Active source repository and issue context." },
      { label: "NCBI BioProject", url: "https://www.ncbi.nlm.nih.gov/bioproject", desc: "Primary accession registry for included cohorts." },
      { label: "NCBI SRA", url: "https://www.ncbi.nlm.nih.gov/sra", desc: "Raw sequencing archive for original submissions." },
      { label: "MGnify", url: "https://www.ebi.ac.uk/metagenomics/", desc: "Reference metagenomics resource for comparative context." },
    ],
    versionRows: [
      {
        date: "2026-04-06",
        label: "Studies + Download/API/About release",
        detail: "Projects Browser, Cross-study workspace, export endpoints, API docs workspace, and About page restructuring aligned to the active main branch.",
      },
      {
        date: "2026-04-05 to 2026-04-06",
        label: "Similarity / Lifecycle / Genus Search refresh",
        detail: "Weighted GMHI, similarity preview heatmap, lifecycle compare workspace, and public-facing renaming from species search to genus-oriented search.",
      },
      {
        date: "2026-04-03 to 2026-04-05",
        label: "Home / Compare / Diseases / Network / Metabolism upgrade wave",
        detail: "Core atlas modules were rebuilt into research workspaces with richer statistics, downloadability, and cross-study behaviors.",
      },
      {
        date: "2026-03-28",
        label: "Initial public atlas baseline",
        detail: "Initial overview, map, phenotype, search, and core page scaffold released.",
      },
    ],
    privacyText:
      "This platform is intended for research use. Do not submit personally identifiable clinical data. User-provided abundance profiles should be treated as transient research inputs rather than regulated clinical records.",
    disclaimerText:
      "The platform provides exploratory statistical outputs and literature-curated interpretations. It does not provide medical advice, diagnostic judgment, or treatment recommendations.",
    contactLabel: "Contact",
    noteLabel: "Operational Note",
    statsCards: [
      { key: "total_samples", label: "Samples", fallback: "168,464" },
      { key: "total_projects", label: "Projects", fallback: "482" },
      { key: "total_condition_categories", label: "Condition categories", fallback: "226" },
      { key: "total_countries", label: "Countries", fallback: "72" },
      { key: "total_genera", label: "Genera", fallback: "4,680" },
      { key: "contact", label: "Contact", fallback: "cdai@cmu.edu.cn" },
    ],
  },
  zh: {
    back: "返回首页",
    title: "引用与关于",
    subtitle: "说明当前 Gut Microbiome Atlas 主分支部署的科研背景、许可边界、引用方式和运维注意事项。",
    sections: {
      team: "研究团队",
      data: "数据来源 / 流程 / 统计",
      cite: "如何引用",
      funding: "基金与致谢",
      license: "数据许可",
      fair: "FAIR 数据原则",
      related: "相关资源",
      faq: "常见问题",
      version: "版本历史",
      privacy: "隐私 / 免责声明",
    },
    teamIntro:
      "平台由 Gut Microbiome Atlas 稿件作者团队相关成员协作维护。这里仅展示当前页面和稿件中可以核实的作者与机构信息，不虚构职位分工。",
    teamAffiliations:
      "主要机构包括：中国医科大学附属第一医院消化内科、锦州医科大学附属第一医院、北京协和医院。",
    dataIntro:
      "Gut Microbiome Atlas 整合公开数据库中的人类肠道微生物组队列，并统一到 genus 层级分析框架中，用于跨研究浏览、可视化和假设生成。",
    sourceLabel: "数据来源",
    pipelineLabel: "处理流程",
    statsLabel: "当前数据集快照",
    citeIntro:
      "如果你是在论文中描述 Atlas 本身，请引用 Paper；如果你是在复现实验流程或平台功能，请同时给出 Software 引用。",
    paper: "论文引用",
    software: "软件引用",
    copy: "复制 BibTeX",
    copied: "已复制",
    fundingText:
      "This work was supported by the National Natural Science Foundation of China (grant No.82270571 and grant No.82570632 to Cong Dai).",
    licenseProcessed:
      "平台处理后的 atlas 级统计结果和聚合可视化输出，按 CC BY 4.0 的保守口径说明共享边界。",
    licenseSource:
      "平台源码遵循主仓库中的 MIT License。",
    licenseRaw:
      "原始测序数据仍然受其原始数据库、BioProject 记录和 accession 级许可约束，平台不改变原始许可。",
    fairItems: [
      "Findable：项目编号、疾病标签和 genus 实体均可通过页面检索和 API 检索定位。",
      "Accessible：核心衍生统计可通过网页界面、人类可读 API 文档和 OpenAPI 规范访问。",
      "Interoperable：输出支持 JSON、CSV、TSV，并使用稳定字段名以便脚本接入。",
      "Reusable：平台明确写出方法、限制和许可边界，减少衍生结果被误用的风险。",
    ],
    relatedLinks: [
      { label: "GitHub", url: "https://github.com/zhai199887/gut-microbiome-atlas", desc: "当前主仓库与源码入口。" },
      { label: "NCBI BioProject", url: "https://www.ncbi.nlm.nih.gov/bioproject", desc: "平台纳入队列的主要 accession 来源。" },
      { label: "NCBI SRA", url: "https://www.ncbi.nlm.nih.gov/sra", desc: "原始测序提交数据归档。" },
      { label: "MGnify", url: "https://www.ebi.ac.uk/metagenomics/", desc: "可用于外部比对的公共微生物组资源。" },
    ],
    versionRows: [
      {
        date: "2026-04-06",
        label: "Studies + Download/API/About 版本",
        detail: "完成 Projects Browser、Cross-study 工作台、下载接口、API 文档工作台和 About 页面重构，并全部落到主分支。",
      },
      {
        date: "2026-04-05 至 2026-04-06",
        label: "Similarity / Lifecycle / Genus Search 升级",
        detail: "完成加权 GMHI、相似性热图、生命周期对比工作台，以及对外命名从 Species Search 校正为以 genus 为主。",
      },
      {
        date: "2026-04-03 至 2026-04-05",
        label: "Home / Compare / Diseases / Network / Metabolism 升级波次",
        detail: "核心模块被重构为科研型工作台，补齐了统计、下载和跨研究行为。",
      },
      {
        date: "2026-03-28",
        label: "初始公开基线",
        detail: "发布首页、地图、表型、检索等基础模块骨架。",
      },
    ],
    privacyText:
      "平台仅用于科研探索。不要上传可识别的临床个人信息。用户提交的 abundance profile 应被视为研究输入，而不是受监管的临床记录。",
    disclaimerText:
      "平台提供的是探索性统计结果和文献策展式解释，不构成医疗建议、诊断结论或治疗意见。",
    contactLabel: "联系方式",
    noteLabel: "运维说明",
    statsCards: [
      { key: "total_samples", label: "样本数", fallback: "168,464" },
      { key: "total_projects", label: "项目数", fallback: "482" },
      { key: "total_diseases", label: "疾病数", fallback: "218" },
      { key: "total_countries", label: "国家数", fallback: "66" },
      { key: "total_genera", label: "菌属数", fallback: "4,680" },
      { key: "contact", label: "联系邮箱", fallback: "cdai@cmu.edu.cn" },
    ],
  },
};

const AUTHORS: Record<"en" | "zh", string[]> = {
  en: [
    "Jinxia Zhai",
    "Yingjie Li",
    "Jiameng Liu",
    "Xinyi Su",
    "Runze Cui",
    "Dianyu Zheng",
    "Yuhan Sun",
    "Jingsheng Yu",
    "Cong Dai",
  ],
  zh: [
    "翟锦霞",
    "李迎杰",
    "刘佳梦",
    "苏心怡",
    "崔润泽",
    "郑殿宇",
    "孙羽晗",
    "于景晟",
    "戴聪",
  ],
};

const PIPELINE_STEPS = {
  en: [
    "Collect public human gut microbiome cohorts from public repository records and project-level metadata.",
    "Normalize metadata fields across disease, country, age group, sex, and project dimensions.",
    "Map taxonomic abundance into a verified genus-level analysis layer used by the atlas.",
    "Compute disease-vs-control, lifecycle, similarity, network, and cross-study statistics on derived abundance matrices.",
  ],
  zh: [
    "从公共数据库和项目级元数据中收集人类肠道微生物组队列。",
    "统一疾病、国家、年龄段、性别和项目等元数据字段。",
    "将分类丰度整理到已经验证的 genus 层级分析框架中。",
    "在衍生丰度矩阵上计算疾病对照、生命周期、相似性、网络和跨研究统计。",
  ],
};

const DATA_SOURCES = {
  en: [
    "Public microbiome cohort metadata linked to BioProject and sequencing repository records.",
    "Derived genus-level abundance matrices used for atlas statistics and visualization.",
    "Internal harmonization layers that map project, disease, and phenotype fields into shared analysis categories.",
  ],
  zh: [
    "与 BioProject 和测序数据库记录关联的公开微生物组队列元数据。",
    "用于 atlas 统计和可视化的 genus 层级衍生丰度矩阵。",
    "把项目、疾病和表型字段整理为统一分析类别的内部标准化层。",
  ],
};

const AboutPageContent = () => {
  const { locale } = useI18n();
  const text = COPY[locale];
  const faqItems: FaqItem[] = locale === "zh" ? faqZh : faqEn;
  const [stats, setStats] = useState<StatsPayload | null>(null);
  const [citationTab, setCitationTab] = useState<"paper" | "software">("paper");
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    cachedFetch<StatsPayload>(`${API_BASE}/api/data-stats`)
      .then(setStats)
      .catch(() => {});
  }, []);

  const activeBibtex = citationTab === "paper" ? PAPER_BIBTEX : SOFTWARE_BIBTEX;
  const dataIntroText = locale === "zh"
    ? "Gut Microbiome Atlas 整合公开数据库中的人类肠道微生物组队列，并统一到 genus 层级分析框架中，用于跨研究浏览、可视化和假设生成。当前发布版包含 225 个非 NC 条件标签，再加 1 个 NC 类别，合计 226 个条件类别。"
    : text.dataIntro;
  const statsCards = locale === "zh"
    ? [
        { key: "total_samples" as const, label: "样本数", fallback: "168,464" },
        { key: "total_projects" as const, label: "项目数", fallback: "482" },
        { key: "total_condition_categories" as const, label: "条件类别", fallback: "226" },
        { key: "total_countries" as const, label: "国家数", fallback: "72" },
        { key: "total_genera" as const, label: "菌属数", fallback: "4,680" },
        { key: "contact" as const, label: "联系邮箱", fallback: "cdai@cmu.edu.cn" },
      ]
    : text.statsCards;

  const statRows = useMemo(
    () =>
      statsCards.map((item) => ({
        label: item.label,
        value:
          item.key === "contact"
            ? item.fallback
            : stats?.[item.key] !== undefined
              ? Number(stats[item.key]).toLocaleString()
              : item.fallback,
      })),
    [stats, statsCards],
  );

  const handleCopy = async () => {
    await navigator.clipboard.writeText(activeBibtex);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  return (
    <>
      <Header />
      <main className={classes.page}>
        <Link to="/" className={classes.backLink}>
          {text.back}
        </Link>

        <header className={classes.hero}>
          <div>
            <h1 className={classes.pageTitle}>{text.title}</h1>
            <p className={classes.heroText}>{text.subtitle}</p>
          </div>
          <div className={classes.heroMeta}>
            <span>{stats?.version ?? (locale === "zh" ? "主分支部署" : "main-branch deployment")}</span>
            <span>{stats?.last_updated ?? "2026-04-06"}</span>
          </div>
        </header>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.team}</h2>
          <p className={classes.cardText}>{text.teamIntro}</p>
          <div className={classes.tagList}>
            {AUTHORS[locale].map((author) => (
              <span key={author} className={classes.tag}>
                {author}
              </span>
            ))}
          </div>
          <p className={classes.cardText}>{text.teamAffiliations}</p>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.data}</h2>
          <p className={classes.cardText}>{dataIntroText}</p>

          <div className={classes.subsection}>
            <h3>{text.sourceLabel}</h3>
            <ul className={classes.list}>
              {DATA_SOURCES[locale].map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ul>
          </div>

          <div className={classes.subsection}>
            <h3>{text.pipelineLabel}</h3>
            <ol className={classes.list}>
              {PIPELINE_STEPS[locale].map((line) => (
                <li key={line}>{line}</li>
              ))}
            </ol>
          </div>

          <div className={classes.subsection}>
            <h3>{text.statsLabel}</h3>
            <div className={classes.statsGrid}>
              {statRows.map((row) => (
                <div key={row.label} className={classes.statCard}>
                  <span className={classes.statValue}>{row.value}</span>
                  <span className={classes.statLabel}>{row.label}</span>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.cite}</h2>
          <p className={classes.cardText}>{text.citeIntro}</p>
          <div className={classes.tabBar}>
            <button
              type="button"
              className={citationTab === "paper" ? classes.tabActive : classes.tab}
              onClick={() => setCitationTab("paper")}
            >
              {text.paper}
            </button>
            <button
              type="button"
              className={citationTab === "software" ? classes.tabActive : classes.tab}
              onClick={() => setCitationTab("software")}
            >
              {text.software}
            </button>
          </div>
          <div className={classes.codeWrap}>
            <pre className={classes.codeBlock}>{activeBibtex}</pre>
            <button type="button" className={classes.copyBtn} onClick={handleCopy}>
              {copied ? text.copied : text.copy}
            </button>
          </div>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.funding}</h2>
          <p className={classes.cardText}>{text.fundingText}</p>
          <p className={classes.note}>
            <strong>{text.noteLabel}:</strong>{" "}
            {locale === "zh"
              ? "当前生产前端展示的是 GitHub main 分支对应的 Vercel 部署；涉及后端测试时，必须先确认主目录 api 服务已重启到最新代码。"
              : "The production frontend is expected to reflect the latest Vercel deployment from the GitHub main branch. Any backend-dependent testing should confirm that the main api workspace has been restarted to the latest code."}
          </p>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.license}</h2>
          <ul className={classes.list}>
            <li>{text.licenseProcessed}</li>
            <li>{text.licenseSource}</li>
            <li>{text.licenseRaw}</li>
          </ul>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.fair}</h2>
          <ul className={classes.list}>
            {text.fairItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.related}</h2>
          <div className={classes.linkGrid}>
            {text.relatedLinks.map((link) => (
              <a key={link.url} href={link.url} target="_blank" rel="noreferrer" className={classes.linkCard}>
                <strong>{link.label}</strong>
                <span>{link.desc}</span>
              </a>
            ))}
          </div>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.faq}</h2>
          <div className={classes.faqList}>
            {faqItems.map((item) => (
              <details key={item.question} className={classes.faqItem}>
                <summary>{item.question}</summary>
                <p>{item.answer}</p>
              </details>
            ))}
          </div>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.version}</h2>
          <div className={classes.timeline}>
            {text.versionRows.map((row) => (
              <div key={`${row.date}-${row.label}`} className={classes.timelineRow}>
                <div className={classes.timelineDate}>{row.date}</div>
                <div>
                  <strong>{row.label}</strong>
                  <p>{row.detail}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className={classes.card}>
          <h2 className={classes.cardTitle}>{text.sections.privacy}</h2>
          <p className={classes.cardText}>{text.privacyText}</p>
          <p className={classes.cardText}>{text.disclaimerText}</p>
          <p className={classes.cardText}>
            <strong>{text.contactLabel}:</strong> <a href="mailto:cdai@cmu.edu.cn">cdai@cmu.edu.cn</a>
          </p>
        </section>
      </main>
      <Footer />
    </>
  );
};

export default AboutPageContent;
