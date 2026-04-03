import { useI18n } from "@/i18n";
import classes from "./Footer.module.css";

const Footer = () => {
  const { t } = useI18n();

  return (
    <footer>
      <p>
        {t("footer.project")}{" "}
        <strong>{t("footer.lab")}</strong>,{" "}
        <a href="https://www.cmu.edu.cn" target="_blank" rel="noreferrer">
          {t("footer.university")}
        </a>
        .
      </p>

      <div className={classes.cols}>
        <a
          href="https://www.cmu.edu.cn"
          target="_blank"
          rel="noreferrer"
          data-tooltip={`${t("footer.university")} · ${t("footer.lab")}`}
          className={classes.logoBlock}
        >
          <div className={classes.logoMark} aria-hidden="true">
            <span>CMU</span>
          </div>
          <div className={classes.logoText}>
            <span className={classes.logoEn}>China Medical University</span>
            <span className={classes.logoCn}>中国医科大学</span>
            <span className={classes.logoDept}>{t("footer.lab")}</span>
          </div>
        </a>
      </div>
    </footer>
  );
};

export default Footer;
