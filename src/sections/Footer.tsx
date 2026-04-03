import { useI18n } from "@/i18n";
import classes from "./Footer.module.css";

const Footer = () => {
  const { t, locale } = useI18n();

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

      {/* Contact / 联系方式 */}
      <div className={classes.contact}>
        <p className={classes.contactTitle}>
          {locale === "zh" ? "如有问题或合作意向，请联系：" : "For inquiries or collaborations, please contact:"}
        </p>
        <div className={classes.contactList}>
          <span className={classes.contactItem}>
            <strong>{locale === "zh" ? "戴聪" : "Cong Dai"}</strong>
            {locale === "zh" ? "（通讯作者）" : " (Corresponding Author)"}
            {" · "}
            <a href="mailto:congdai2006@sohu.com">congdai2006@sohu.com</a>
          </span>
          <span className={classes.contactDivider}>|</span>
          <span className={classes.contactItem}>
            <strong>{locale === "zh" ? "翟金霞" : "Jinxia Zhai"}</strong>
            {" · "}
            <a href="mailto:zhaijinxia07@gmail.com">zhaijinxia07@gmail.com</a>
          </span>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
