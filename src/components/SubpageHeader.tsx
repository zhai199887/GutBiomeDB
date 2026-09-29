import { Link } from "react-router-dom";

import LangSwitch from "@/components/LangSwitch";
import { useI18n } from "@/i18n";

import classes from "./SubpageHeader.module.css";

type SubpageHeaderProps = {
  title: string;
  subtitle: string;
};

const SubpageHeader = ({ title, subtitle }: SubpageHeaderProps) => {
  const { t } = useI18n();

  const links = [
    { to: "/", label: t("nav.home") },
    { to: "/phenotype", label: t("nav.explorer") },
    { to: "/compare", label: t("nav.compare") },
    { to: "/search", label: t("nav.search") },
    { to: "/studies", label: t("nav.studies") },
    { to: "/api-docs", label: t("nav.apiDocs") },
    { to: "/about", label: t("nav.cite") },
  ];

  return (
    <header className={classes.shell}>
      <div className={classes.topRow}>
        <div className={classes.brandBlock}>
          <Link to="/" className={classes.brand}>
            GutBiomeDB
          </Link>
          <p className={classes.subtitle}>{subtitle}</p>
        </div>
        <LangSwitch />
      </div>

      <div className={classes.bottomRow}>
        <h1 className={classes.title}>{title}</h1>
        <nav className={classes.nav}>
          {links.map(({ to, label }) => (
            <Link key={to} to={to} className={classes.navLink}>
              {label}
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
};

export default SubpageHeader;