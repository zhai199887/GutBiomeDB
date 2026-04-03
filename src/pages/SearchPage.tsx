/**
 * SearchPage.tsx — 物种搜索独立页面
 * 包装 Search section 为独立路由页面
 */
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import Search from "@/sections/Search";

const SearchPage = () => {
  const { t } = useI18n();

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "2rem", minHeight: "100vh", color: "var(--white)" }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <Link to="/" style={{ color: "var(--accent)", textDecoration: "none", fontSize: "0.9rem" }}>
          ← {t("lifecycle.back")}
        </Link>
      </div>
      <Search />
    </div>
  );
};

export default SearchPage;
