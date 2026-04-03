import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";

const NotFoundPage = () => {
  const { t } = useI18n();

  return (
    <main
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "60vh",
        gap: "1.5rem",
        padding: "2rem",
      }}
    >
      <h1 style={{ fontSize: "4rem", fontWeight: 700, margin: 0 }}>404</h1>
      <p style={{ fontSize: "1.2rem", opacity: 0.7 }}>{t("notFound.message")}</p>
      <Link
        to="/"
        style={{
          color: "var(--secondary-light)",
          fontSize: "1rem",
          textDecoration: "underline",
        }}
      >
        {t("notFound.backHome")}
      </Link>
    </main>
  );
};

export default NotFoundPage;
