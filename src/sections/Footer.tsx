import { useI18n } from "@/i18n";
import classes from "./Footer.module.css";

const Footer = () => {
  const { t } = useI18n();

  return (
    <footer aria-label="Site footer">
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
          aria-label={`${t("footer.university")} · ${t("footer.lab")}`}
          className={classes.logoBlock}
        >
          <div className={classes.logoMark} aria-hidden="true">
            <span>{t("footer.universityShort")}</span>
          </div>
          <div className={classes.logoText}>
            <span className={classes.logoEn}>{t("footer.university")}</span>
            <span className={classes.logoDept}>{t("footer.lab")}</span>
          </div>
        </a>
      </div>

      {/* Contact / 联系方式 */}
      <div className={classes.contact}>
        <p className={classes.contactTitle}>
          {t("footer.contactIntro")}
        </p>
        <div className={classes.contactList}>
          <span className={classes.contactItem}>
            <strong>{t("footer.authorCong")}</strong>
            {t("footer.corresponding")}
            {" · "}
            <a href="mailto:cdai@cmu.edu.cn">cdai@cmu.edu.cn</a>
          </span>
          <span className={classes.contactDivider}>|</span>
          <span className={classes.contactItem}>
            <strong>{t("footer.authorZhai")}</strong>
            {" · "}
            <a href="mailto:zhaijinxia07@gmail.com">zhaijinxia07@gmail.com</a>
          </span>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
