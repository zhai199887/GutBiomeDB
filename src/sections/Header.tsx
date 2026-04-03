import { Link } from "react-router-dom";
import Logo from "@/assets/logo.svg?react";
import HeaderBg from "@/sections/HeaderBg.tsx";
import classes from "./Header.module.css";

const NAV_LINKS = [
  { to: "/",           label: "Home" },
  { to: "/phenotype",  label: "Explorer" },
  { to: "/compare",    label: "Compare" },
  { to: "/metabolism", label: "Metabolism" },
] as const;

const Header = () => (
  <header className={classes.header}>
    <HeaderBg />

    <div className={classes.title}>
      <Logo className={classes.logo} />
      <div className={classes.divider} />
      <h1 className={classes.h1}>{import.meta.env.VITE_TITLE}</h1>
    </div>

    <p className={classes.subtitle}>{import.meta.env.VITE_DESCRIPTION}</p>

    <nav className={classes.buttons}>
      {NAV_LINKS.map(({ to, label }) => (
        <Link key={to} to={to} className={classes.navLink}>
          {label}
        </Link>
      ))}
    </nav>
  </header>
);

export default Header;
