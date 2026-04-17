import { useEffect, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import LangSwitch from "@/components/LangSwitch";
import { useI18n } from "@/i18n";

import classes from "./GlobalNav.module.css";

type NavLink = { to: string; label: string; match: (p: string) => boolean };
type NavGroup = { label: string; items: NavLink[] };
type NavItem = NavLink | NavGroup;

const isGroup = (item: NavItem): item is NavGroup => "items" in item;

const GlobalNav = () => {
  const { t, locale } = useI18n();
  const { pathname } = useLocation();
  const [openGroup, setOpenGroup] = useState<string | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    setOpenGroup(null);
  }, [pathname]);

  useEffect(() => {
    const onMouseDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpenGroup(null);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpenGroup(null);
    };
    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  const analysisItems: NavLink[] = [
    { to: "/phenotype", label: t("nav.explorer"), match: (p) => p.startsWith("/phenotype") },
    { to: "/compare", label: t("nav.compare"), match: (p) => p.startsWith("/compare") },
    { to: "/disease", label: t("nav.disease"), match: (p) => p.startsWith("/disease") },
    { to: "/network", label: t("nav.network"), match: (p) => p.startsWith("/network") || p.startsWith("/cooccurrence") || p.startsWith("/chord") },
    { to: "/metabolism", label: t("nav.metabolism"), match: (p) => p.startsWith("/metabolism") },
    { to: "/similarity", label: t("nav.similarity"), match: (p) => p.startsWith("/similarity") },
    { to: "/lifecycle", label: t("nav.lifecycle"), match: (p) => p.startsWith("/lifecycle") },
  ];

  const dataItems: NavLink[] = [
    { to: "/studies", label: t("nav.studies"), match: (p) => p.startsWith("/studies") },
    { to: "/download", label: t("nav.download"), match: (p) => p.startsWith("/download") },
  ];

  const analysisLabel = locale === "zh" ? "分析" : "Analysis";
  const dataLabel = locale === "zh" ? "数据" : "Data";

  const topLevel: NavItem[] = [
    { to: "/", label: t("nav.home"), match: (p) => p === "/" },
    { to: "/search", label: t("nav.search"), match: (p) => p.startsWith("/search") || p.startsWith("/species/") },
    { label: analysisLabel, items: analysisItems },
    { label: dataLabel, items: dataItems },
    { to: "/api-docs", label: t("nav.apiDocs"), match: (p) => p.startsWith("/api-docs") },
    { to: "/about", label: t("nav.cite"), match: (p) => p.startsWith("/about") },
  ];

  return (
    <header className={classes.shell}>
      <div className={classes.row} ref={wrapRef}>
        <Link to="/" className={classes.brand}>
          GutBiomeDB
        </Link>
        <nav className={classes.nav}>
          {topLevel.map((item) => {
            if (isGroup(item)) {
              const isActive = item.items.some((it) => it.match(pathname));
              const isOpen = openGroup === item.label;
              return (
                <div key={item.label} className={classes.groupWrap}>
                  <button
                    type="button"
                    className={[
                      classes.navLink,
                      classes.groupTrigger,
                      isActive ? classes.navLinkActive : "",
                      isOpen ? classes.groupOpen : "",
                    ].filter(Boolean).join(" ")}
                    onClick={() => setOpenGroup(isOpen ? null : item.label)}
                    aria-haspopup="menu"
                    aria-expanded={isOpen}
                  >
                    {item.label}
                    <span className={classes.chevron} aria-hidden="true">▾</span>
                  </button>
                  {isOpen && (
                    <div className={classes.dropdown} role="menu">
                      {item.items.map((sub) => (
                        <Link
                          key={sub.to}
                          to={sub.to}
                          role="menuitem"
                          className={sub.match(pathname) ? `${classes.dropdownLink} ${classes.dropdownLinkActive}` : classes.dropdownLink}
                        >
                          {sub.label}
                        </Link>
                      ))}
                    </div>
                  )}
                </div>
              );
            }
            return (
              <Link
                key={item.to}
                to={item.to}
                className={item.match(pathname) ? `${classes.navLink} ${classes.navLinkActive}` : classes.navLink}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
        <LangSwitch />
      </div>
    </header>
  );
};

export default GlobalNav;
