import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import Logo from "@/assets/logo.svg?react";
import LangSwitch from "@/components/LangSwitch";
import { useI18n } from "@/i18n";

import HeaderBg from "./HeaderBg";
import classes from "./Header.module.css";

const Header = () => {
  const { t } = useI18n();
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    if (!menuOpen) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenuOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [menuOpen]);

  const navLinks = useMemo(
    () => [
      { to: "/", label: t("nav.home") },
      { to: "/phenotype", label: t("nav.explorer") },
      { to: "/disease", label: t("nav.disease") },
      { to: "/search", label: t("nav.search") },
      { to: "/studies", label: t("nav.studies") },
      { to: "/compare", label: t("nav.compare") },
      { to: "/network", label: t("nav.network") },
      { to: "/metabolism", label: t("nav.metabolism") },
      { to: "/lifecycle", label: t("nav.lifecycle") },
      { to: "/similarity", label: t("nav.similarity") },
      { to: "/download", label: t("nav.download") },
      { to: "/api-docs", label: t("nav.apiDocs") },
      { to: "/about", label: t("nav.cite") },
    ],
    [t],
  );

  return (
    <header className={classes.header}>
      <HeaderBg />

      <div className={classes.heroShell}>
        <section className={classes.heroCopy}>
          <div className={classes.heroTop}>
            <span className={classes.kicker}>{t("header.kicker")}</span>
            <div className={classes.heroActions}>
              <Link to="/analysis-jobs" className={classes.jobsLink}>{t("nav.jobs")}</Link>
              <LangSwitch />
            </div>
          </div>

          <div className={classes.title}>
            <Logo className={classes.logo} />
            <div className={classes.divider} />
            <div className={classes.brandCopy}>
              <h1 className={classes.h1}>{t("header.title")}</h1>
              <p className={classes.subtitle}>{t("header.subtitle")}</p>
            </div>
          </div>
        </section>
      </div>

      <button
        className={classes.hamburger}
        onClick={() => setMenuOpen((open) => !open)}
        aria-label={t("header.toggleNav")}
        aria-expanded={menuOpen}
      >
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
        <span className={`${classes.hamburgerLine} ${menuOpen ? classes.hamburgerOpen : ""}`} />
      </button>

      <nav className={`${classes.buttons} ${menuOpen ? classes.buttonsOpen : ""}`}>
        {navLinks.map(({ to, label }) => (
          <Link
            key={to}
            to={to}
            className={classes.navLink}
            onClick={() => setMenuOpen(false)}
          >
            {label}
          </Link>
        ))}
        <Link to="/analysis-jobs" className={classes.navLink} onClick={() => setMenuOpen(false)}>
          {t("nav.jobs")}
        </Link>
        <LangSwitch />
      </nav>
    </header>
  );
};

export default Header;
