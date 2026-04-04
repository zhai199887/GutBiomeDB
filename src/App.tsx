import { lazy, Suspense, useEffect, type ReactNode } from "react";
import { BrowserRouter, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import {
  loadAbundance,
  loadGeoData,
  loadSummary,
} from "@/data";
import ErrorBoundary from "@/components/ErrorBoundary";
import { I18nProvider } from "@/i18n";
import Footer from "@/sections/Footer";
import Header from "@/sections/Header";
import FilterPanel from "@/sections/FilterPanel";
import Overview from "@/sections/Overview";
import MapSection from "@/sections/MapSection";
import PhenotypeCharts from "@/sections/PhenotypeCharts";
import SankeyChart from "@/sections/SankeyChart";
// Search 已移到独立页面 /search
import "@/components/tooltip";
import { trackEvent } from "@/util/tracking";
import "./App.css";

// Lazy-loaded pages for code splitting / 懒加载页面（代码分割）
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
const NotFoundPage = lazy(() => import("@/pages/NotFoundPage"));

/* ── Route-level document title / 路由级页面标题 ── */
const ROUTE_TITLES: Record<string, string> = {
  "/": "Home",
  "/phenotype": "Phenotype Explorer",
  "/compare": "Differential Analysis",
  "/disease": "Disease Browser",
  "/network": "Network Visualization",
  "/metabolism": "Metabolism Pathway",
  "/similarity": "Sample Similarity",
  "/lifecycle": "Lifecycle Atlas",
  "/search": "Search",
  "/api-docs": "API Documentation",
  "/about": "About & Cite",
  "/download": "Download",
  "/admin": "Admin",
};
const BASE_TITLE = "Gut Microbiome Atlas";

const DocumentTitle = ({ children }: { children: ReactNode }) => {
  const { pathname } = useLocation();
  const navigate = useNavigate();
  useEffect(() => {
    const sub = ROUTE_TITLES[pathname];
    document.title = sub ? `${sub} | ${BASE_TITLE}` : BASE_TITLE;
    trackEvent("page_view", pathname);
  }, [pathname]);

  // Ctrl+K / Cmd+K 全局搜索快捷键
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

const PageLoader = () => (
  <div style={{ display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center", minHeight: "60vh", gap: "1.5rem" }}>
    <div className="loading-spinner" />
    <p style={{ opacity: 0.4, fontSize: "0.9rem", margin: 0 }}>Loading...</p>
  </div>
);

const MainPage = () => {
  useEffect(() => {
    loadSummary();
    loadGeoData();
    loadAbundance();
  }, []);

  return (
    <>
      <Header />
      <main id="main-content">
        <Overview />
        <FilterPanel />
        <MapSection />
        <PhenotypeCharts />
        <SankeyChart />
        <Footer />
      </main>
    </>
  );
};

const App = () => (
  <I18nProvider>
    <BrowserRouter>
      <a href="#main-content" className="skip-link">Skip to content</a>
      <ErrorBoundary>
      <DocumentTitle>
      <Suspense fallback={<PageLoader />}>
        <Routes>
          <Route path="/" element={<MainPage />} />
          <Route path="/phenotype" element={<PhenotypePage />} />
          <Route path="/compare" element={<ComparePage />} />
          <Route path="/disease" element={<DiseasePage />} />
          <Route path="/network" element={<NetworkPage />} />
          <Route path="/metabolism" element={<MetabolismPage />} />
          <Route path="/species/:taxon" element={<SpeciesPage />} />
          <Route path="/similarity" element={<SimilarityPage />} />
          <Route path="/lifecycle" element={<LifecyclePage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/api-docs" element={<ApiDocsPage />} />
          <Route path="/about" element={<CitePage />} />
          <Route path="/download" element={<DownloadPage />} />
          <Route path="/admin" element={<AdminPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </Suspense>
      </DocumentTitle>
      </ErrorBoundary>
    </BrowserRouter>
  </I18nProvider>
);

export default App;
