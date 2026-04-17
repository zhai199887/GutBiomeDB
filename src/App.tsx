import { lazy, Suspense, useEffect, type ReactNode } from "react";
import { BrowserRouter, Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import {
  loadAbundance,
  loadGeoData,
  loadSummary,
} from "@/data";
import ErrorBoundary from "@/components/ErrorBoundary";
import GlobalNav from "@/components/GlobalNav";
import { I18nProvider, useI18n } from "@/i18n";
import Footer from "@/sections/Footer";
import Header from "@/sections/Header";
import FilterPanel from "@/sections/FilterPanel";
import Overview from "@/sections/Overview";
import MapSection from "@/sections/MapSection";
import PhenotypeCharts from "@/sections/PhenotypeCharts";
import SankeyChart from "@/sections/SankeyChart";
import "@/components/tooltip";
import { Analytics } from "@vercel/analytics/react";
import { trackEvent } from "@/util/tracking";
import { preloadDiseaseNames } from "@/util/diseaseNames";
import "./App.css";

// Preload disease display name mapping
preloadDiseaseNames();

// Lazy-loaded pages for code splitting
const PhenotypePage = lazy(() => import("@/sections/PhenotypePage"));
const ComparePage = lazy(() => import("@/pages/ComparePage"));
const MetabolismPage = lazy(() => import("@/pages/MetabolismPage"));
const DiseasePage = lazy(() => import("@/pages/DiseasePage"));
const NetworkPage = lazy(() => import("@/pages/NetworkPage"));
const SpeciesPage = lazy(() => import("@/pages/SpeciesPage"));
const AdminPage = lazy(() => import("@/pages/AdminPage"));
const SimilarityPage = lazy(() => import("@/pages/SimilarityPage"));
const LifecyclePage = lazy(() => import("@/pages/LifecyclePage"));
const SearchPage = lazy(() => import("@/pages/SearchPage"));
const ApiDocsPage = lazy(() => import("@/pages/ApiDocsPage"));
const CitePage = lazy(() => import("@/pages/CitePage"));
const DownloadPage = lazy(() => import("@/pages/DownloadPage"));
const StudiesPage = lazy(() => import("@/pages/StudiesPage"));
const NotFoundPage = lazy(() => import("@/pages/NotFoundPage"));

/* ── Route-level document title ── */
const ROUTE_TITLES = {
  en: {
    "/": "Home",
    "/phenotype": "Phenotype Explorer",
    "/compare": "Differential Analysis",
    "/disease": "Disease Browser",
    "/network": "Network Visualization",
    "/cooccurrence": "Network Visualization",
    "/chord": "Network Visualization",
    "/metabolism": "Metabolism Pathway",
    "/similarity": "Health Index",
    "/lifecycle": "Lifecycle Atlas",
    "/search": "Genus Search",
    "/studies": "Studies",
    "/api-docs": "API Documentation",
    "/about": "About & Cite",
    "/download": "Download",
    "/admin": "Admin",
  },
  zh: {
    "/": "首页",
    "/phenotype": "表型探索",
    "/compare": "差异分析",
    "/disease": "疾病浏览",
    "/network": "网络可视化",
    "/cooccurrence": "网络可视化",
    "/chord": "网络可视化",
    "/metabolism": "代谢功能",
    "/similarity": "健康指数",
    "/lifecycle": "生命周期",
    "/search": "菌属检索",
    "/studies": "研究项目",
    "/api-docs": "API 文档",
    "/about": "引用与关于",
    "/download": "下载",
    "/admin": "管理",
  },
} as const;

const BASE_TITLES = {
  en: "GutBiomeDB",
  zh: "GutBiomeDB",
} as const;

const DocumentTitle = ({ children }: { children: ReactNode }) => {
  const { locale } = useI18n();
  const { pathname } = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    const routeTitles = ROUTE_TITLES[locale];
    const baseTitle = BASE_TITLES[locale];
    const sub = pathname.startsWith("/species/")
      ? (locale === "zh" ? "菌属画像" : "Genus Profile")
      : routeTitles[pathname as keyof typeof routeTitles];
    document.title = sub ? `${sub} | ${baseTitle}` : baseTitle;
    trackEvent("page_view", pathname);
  }, [locale, pathname]);

  // Ctrl+K / Cmd+K global search shortcut
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        navigate("/search");
        setTimeout(() => {
          const input = document.querySelector<HTMLInputElement>("input[type='text'], input[placeholder]");
          input?.focus();
        }, 150);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [navigate]);

  return <>{children}</>;
};

const PageLoader = () => {
  const { t } = useI18n();
  return (
    <div style={{ display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center", minHeight: "60vh", gap: "1.5rem" }}>
      <div className="loading-spinner" />
      <p style={{ opacity: 0.4, fontSize: "0.9rem", margin: 0 }}>{t("common.loading")}</p>
    </div>
  );
};

const MainPage = () => {
  useEffect(() => {
    loadSummary();
    loadGeoData();
    loadAbundance();
  }, []);

  return (
    <div className="home-shell">
      <Header />
      <main id="main-content">
        <Overview />
        <FilterPanel />
        <MapSection />
        <PhenotypeCharts />
        <SankeyChart />
        <Footer />
      </main>
    </div>
  );
};

const AppShell = () => {
  const { t } = useI18n();
  const { pathname } = useLocation();
  const showGlobalNav = pathname !== "/" && pathname !== "/about" && pathname !== "/api-docs";

  return (
    <>
      <a href="#main-content" className="skip-link">{t("common.skipToContent")}</a>
      <ErrorBoundary>
      <DocumentTitle>
      {showGlobalNav && <GlobalNav />}
      <Suspense fallback={<PageLoader />}>
        <Routes>
          <Route path="/" element={<MainPage />} />
          <Route path="/phenotype" element={<PhenotypePage />} />
          <Route path="/compare" element={<ComparePage />} />
          <Route path="/disease" element={<DiseasePage />} />
          <Route path="/network" element={<NetworkPage />} />
          <Route path="/cooccurrence" element={<Navigate to="/network" replace />} />
          <Route path="/chord" element={<Navigate to="/network" replace />} />
          <Route path="/metabolism" element={<MetabolismPage />} />
          <Route path="/species/:taxon" element={<SpeciesPage />} />
          <Route path="/similarity" element={<SimilarityPage />} />
          <Route path="/lifecycle" element={<LifecyclePage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/api-docs" element={<ApiDocsPage />} />
          <Route path="/about" element={<CitePage />} />
          <Route path="/download" element={<DownloadPage />} />
          <Route path="/studies" element={<StudiesPage />} />
          <Route path="/admin" element={<AdminPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </Suspense>
      </DocumentTitle>
      </ErrorBoundary>
    </>
  );
};

const App = () => (
  <I18nProvider>
    <BrowserRouter>
      <AppShell />
      <Analytics />
    </BrowserRouter>
  </I18nProvider>
);

export default App;