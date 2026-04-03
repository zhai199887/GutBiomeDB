import { useState } from "react";
import { Link } from "react-router-dom";
import Logo from "@/assets/logo.svg?react";
import LangSwitch from "@/components/LangSwitch";
import { useI18n } from "@/i18n";
import HeaderBg from "@/sections/HeaderBg.tsx";
import classes from "./Header.module.css";

const Header = () => {
  const { t } = useI18n();
  const [menuOpen, setMenuOpen] = useState(false);

  const NAV_LINKS = [
    { to: "/",           label: t("nav.home") },
    { to: "/phenotype",  label: t("nav.explorer") },
    { to: "/compare",    label: t("nav.compare") },
    { to: "/disease",    label: t("nav.disease") },
    { to: "/network",    label: t("nav.network") },
    { to: "/metabolism", label: t("nav.metabolism") },
    { to: "/lifecycle",  label: t("nav.lifecycle") },
    { to: "/similarity", label: t("nav.similarity") },
    { to: "/search",     label: t("nav.search") },
    { to: "/download",   label: t("nav.download") },
    { to: "/api-docs",   label: t("nav.apiDocs") },
    { to: "/about",     label: t("nav.cite") },
  ] as const;

  return (
    <header className={classes.header}>
      <HeaderBg />

      <div className={classes.title}>
        <Logo className={classes.logo} />
        <div className={classes.divider} />
        <h1 className={classes.h1}>{t("header.title")}</h1>
      </div>

      <p className={classes.subtitle}>{t("header.subtitle")}</p>

      {/* Hamburger button for mobile / 移动端汉堡菜单按钮 */}
      <button
        className={classes.hamburger}
        onClick={() => setMenuOpen(!menuOpen)}
        aria-label="Toggle navigation menu"
        aria-expanded={menuOpen}
      >
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
      </button>

      <nav className={`${classes.buttons} ${menuOpen ? classes.buttonsOpen : ""}`}>
        {NAV_LINKS.map(({ to, label }) => (
          <Link
            key={to}
            to={to}
            className={classes.navLink}
            onClick={() => setMenuOpen(false)}
          >
            {label}
          </Link>
        ))}
        <LangSwitch />
      </nav>
    </header>
  );
};

export default Header;
