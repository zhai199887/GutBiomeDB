import { countryName, AGE_GROUP_ZH, SEX_ZH } from "@/util/countries";

import type { ProjectDetailResult, ProjectCountRow } from "./types";

const BarList = ({
  rows,
  valueKey,
  total,
  formatter,
}: {
  rows: ProjectCountRow[];
  valueKey: "country" | "age_group" | "sex" | "disease";
  total: number;
  formatter?: (value: string) => string;
}) => (
  <div style={{ display: "grid", gap: 6 }}>
    {rows.map((row) => {
      const rawValue = String(row[valueKey] ?? "Unknown");
      const label = formatter ? formatter(rawValue) : rawValue;
      const width = `${Math.max(6, (row.count / Math.max(total, 1)) * 100)}%`;
      return (
        <div key={`${valueKey}-${rawValue}`} style={{ display: "grid", gridTemplateColumns: "140px minmax(0,1fr) 44px", gap: 8, alignItems: "center" }}>
          <span style={{ fontSize: "0.8rem", color: "var(--white)" }}>{label}</span>
          <div style={{ height: 10, borderRadius: 999, background: "rgba(255,255,255,0.08)" }}>
            <div style={{ width, height: "100%", borderRadius: 999, background: "linear-gradient(90deg, var(--primary), color-mix(in hsl, var(--primary), white 20%))" }} />
          </div>
          <span style={{ fontSize: "0.78rem", color: "var(--light-gray)", textAlign: "right" }}>{row.count}</span>
        </div>
      );
    })}
  </div>
);

const ProjectDetailPanel = ({
  detail,
  locale,
  loading = false,
}: {
  detail: ProjectDetailResult | null;
  locale: string;
  loading?: boolean;
}) => {
  if (loading || !detail) {
    return (
      <div style={{ padding: "18px 0", color: "var(--light-gray)" }}>
        {loading
          ? (locale === "zh" ? "正在加载项目详情…" : "Loading project detail...")
          : (locale === "zh" ? "项目详情暂不可用。" : "Project detail is currently unavailable.")}
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 18 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", fontSize: "0.82rem", color: "var(--light-gray)" }}>
        <span>{locale === "zh" ? "总样本" : "Samples"}: <b style={{ color: "var(--white)" }}>{detail.total_samples.toLocaleString("en-US")}</b></span>
        <span style={{ color: "#22c55e" }}>NC: <b>{detail.nc_count.toLocaleString("en-US")}</b></span>
        <span style={{ color: "#f97316" }}>{locale === "zh" ? "疾病样本" : "Disease"}: <b>{detail.disease_count.toLocaleString("en-US")}</b></span>
        <span>{locale === "zh" ? "年份" : "Year"}: <b style={{ color: "var(--white)" }}>{detail.year ?? "—"}</b></span>
        <span>{locale === "zh" ? "国家" : "Countries"}: <b style={{ color: "var(--white)" }}>{detail.country_list.map((item) => countryName(item, locale)).join(", ") || "—"}</b></span>
        <span>{locale === "zh" ? "仪器" : "Instrument"}: <b style={{ color: "var(--white)" }}>{detail.instrument}</b></span>
        <a href={detail.ncbi_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)", textDecoration: "none" }}>
          {locale === "zh" ? "NCBI BioProject" : "NCBI BioProject"}
        </a>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1.2fr 1fr", gap: 20 }}>
        <section>
          <h4 style={{ margin: "0 0 10px", fontSize: "0.92rem" }}>{locale === "zh" ? "疾病组成" : "Disease Breakdown"}</h4>
          <BarList rows={detail.by_disease} valueKey="disease" total={detail.total_samples} />
        </section>
        <section>
          <h4 style={{ margin: "0 0 10px", fontSize: "0.92rem" }}>{locale === "zh" ? "国家分布" : "Country Distribution"}</h4>
          <BarList rows={detail.by_country} valueKey="country" total={detail.total_samples} formatter={(value) => `${countryName(value, locale)} (${value})`} />
        </section>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
        <section>
          <h4 style={{ margin: "0 0 10px", fontSize: "0.92rem" }}>{locale === "zh" ? "年龄组分布" : "Age Groups"}</h4>
          <BarList
            rows={detail.by_age_group}
            valueKey="age_group"
            total={detail.total_samples}
            formatter={(value) => (locale === "zh" ? (AGE_GROUP_ZH[value] ?? value) : value)}
          />
        </section>
        <section>
          <h4 style={{ margin: "0 0 10px", fontSize: "0.92rem" }}>{locale === "zh" ? "性别分布" : "Sex Distribution"}</h4>
          <BarList
            rows={detail.by_sex}
            valueKey="sex"
            total={detail.total_samples}
            formatter={(value) => {
              if (locale !== "zh") return value;
              return SEX_ZH[value.toLowerCase()] ?? value;
            }}
          />
        </section>
      </div>
    </div>
  );
};

export default ProjectDetailPanel;
