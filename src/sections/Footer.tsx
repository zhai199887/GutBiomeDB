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

      {/* Contact */}
      <div className={classes.contact}>
        <p className={classes.contactTitle}>
          {t("footer.contactIntro")}
        </p>
        <div className={classes.contactList}>
          <span className={classes.contactItem}>
            <strong>{t("footer.authorCong")}</strong>
            {t("footer.corresponding")}
            {" | "}
            <a href="mailto:cdai@cmu.edu.cn">cdai@cmu.edu.cn</a>
          </span>
          <span className={classes.contactDivider}>|</span>
          <span className={classes.contactItem}>
            <strong>{t("footer.authorZhai")}</strong>
            {" | "}
            <a href="mailto:zhaijinxia07@gmail.com">zhaijinxia07@gmail.com</a>
          </span>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
