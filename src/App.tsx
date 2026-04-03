import { lazy, Suspense, useEffect } from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import {
  loadAbundance,
  loadGeoData,
  loadSummary,
} from "@/data";
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

const PageLoader = () => (
  <div style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: "60vh" }}>
    <div className="loading-spinner" />
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
      <main>
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
          <Route path="/admin" element={<AdminPage />} />
        </Routes>
      </Suspense>
    </BrowserRouter>
  </I18nProvider>
);

export default App;
