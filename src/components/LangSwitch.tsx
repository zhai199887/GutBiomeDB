/**
 * Language toggle button (EN / CN)
 */
import { useI18n } from "@/i18n";

const LangSwitch = ({ style }: { style?: React.CSSProperties }) => {
  const { locale, setLocale, t } = useI18n();

  return (
    <button
      onClick={() => setLocale(locale === "en" ? "zh" : "en")}
      style={{
        background: "rgba(255,255,255,0.1)",
        border: "1px solid rgba(255,255,255,0.25)",
        color: "inherit",
        borderRadius: "4px",
        padding: "0.25rem 0.6rem",
        cursor: "pointer",
        fontSize: "0.8rem",
        fontWeight: 500,
        transition: "background 0.2s",
        ...style,
      }}
      title={locale === "en" ? "切换到中文" : "Switch to English"}
    >
      {t("lang.switch")}
    </button>
  );
};

export default LangSwitch;
