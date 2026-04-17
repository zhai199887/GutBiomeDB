import { Link, useLocation } from "react-router-dom";

import LangSwitch from "@/components/LangSwitch";
import { useI18n } from "@/i18n";

import classes from "./GlobalNav.module.css";

const GlobalNav = () => {
  const { t } = useI18n();
  const { pathname } = useLocation();

  const links = [
    { to: "/", label: t("nav.home"), match: (p: string) => p === "/" },
    { to: "/search", label: t("nav.search"), match: (p: string) => p.startsWith("/search") || p.startsWith("/species/") },
    { to: "/phenotype", label: t("nav.explorer"), match: (p: string) => p.startsWith("/phenotype") },
    { to: "/disease", label: t("nav.disease"), match: (p: string) => p.startsWith("/disease") },
    { to: "/compare", label: t("nav.compare"), match: (p: string) => p.startsWith("/compare") },
    { to: "/network", label: t("nav.network"), match: (p: string) => p.startsWith("/network") || p.startsWith("/cooccurrence") || p.startsWith("/chord") },
    { to: "/metabolism", label: t("nav.metabolism"), match: (p: string) => p.startsWith("/metabolism") },
    { to: "/similarity", label: t("nav.similarity"), match: (p: string) => p.startsWith("/similarity") },
    { to: "/lifecycle", label: t("nav.lifecycle"), match: (p: string) => p.startsWith("/lifecycle") },
    { to: "/studies", label: t("nav.studies"), match: (p: string) => p.startsWith("/studies") },
    { to: "/download", label: t("nav.download"), match: (p: string) => p.startsWith("/download") },
    { to: "/api-docs", label: t("nav.apiDocs"), match: (p: string) => p.startsWith("/api-docs") },
    { to: "/about", label: t("nav.cite"), match: (p: string) => p.startsWith("/about") },
  ];

  return (
    <header className={classes.shell}>
      <div className={classes.row}>
        <Link to="/" className={classes.brand}>
          GutBiomeDB
        </Link>
        <nav className={classes.nav}>
          {links.map(({ to, label, match }) => (
            <Link
              key={to}
              to={to}
              className={match(pathname) ? `${classes.navLink} ${classes.navLinkActive}` : classes.navLink}
            >
              {label}
            </Link>
          ))}
        </nav>
        <LangSwitch />
      </div>
    </header>
  );
};

export default GlobalNav;
