import { useEffect } from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import {
  loadAbundance,
  loadGeoData,
  loadSummary,
} from "@/data";
import Footer from "@/sections/Footer";
import Header from "@/sections/Header";
import FilterPanel from "@/sections/FilterPanel";
import Overview from "@/sections/Overview";
import MapSection from "@/sections/MapSection";
import PhenotypeCharts from "@/sections/PhenotypeCharts";
import Search from "@/sections/Search";
import PhenotypePage from "@/sections/PhenotypePage";
import "@/components/tooltip";
import "./App.css";

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
        <Search />
        <Footer />
      </main>
    </>
  );
};

const App = () => (
  <BrowserRouter>
    <Routes>
      <Route path="/" element={<MainPage />} />
      <Route path="/phenotype" element={<PhenotypePage />} />
    </Routes>
  </BrowserRouter>
);

export default App;
