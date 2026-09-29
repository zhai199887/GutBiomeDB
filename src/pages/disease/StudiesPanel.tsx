import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";
import { API_BASE } from "@/util/apiBase";
import { countryName } from "@/util/countries";
import type { DiseaseStudiesResult } from "./types";
import classes from "../DiseasePage.module.css";

interface Props {
  disease: string;
}

const cscsColor = (score: number) => {
  if (score >= 75) return "#22c55e";
  if (score >= 50) return "#f59e0b";
  return "#ef4444";
};

const StudiesPanel = ({ disease }: Props) => {
  const { t, locale } = useI18n();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<DiseaseStudiesResult | null>(null);

  useEffect(() => {
    if (!disease) return;
    setLoading(true);
    setError(null);
    setResult(null);
    cachedFetch<DiseaseStudiesResult>(`${API_BASE}/api/disease-studies?disease=${encodeURIComponent(disease)}`)
      .then(setResult)
      .catch(() => {
        setError(locale === "zh" ? "研究列表加载失败" : "Failed to load study breakdown");
      })
      .finally(() => setLoading(false));
  }, [disease, locale]);

  const totals = useMemo(() => {
    if (!result) return { disease: 0, control: 0 };
    return result.projects.reduce(
      (acc, project) => {
        acc.disease += project.n_disease;
        acc.control += project.n_control;
        return acc;
      },
      { disease: 0, control: 0 },
    );
  }, [result]);

  if (loading) return <div className={classes.loading}>{t("biomarker.running")}</div>;
  if (error) return <div className={classes.errorMsg}>{error}</div>;
  if (!result) return null;

  return (
    <div>
      <div className={classes.profileHeader}>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{result.n_projects}</span>
          <span className={classes.statLabel}>{t("disease.studies.nProjects")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{totals.disease.toLocaleString("en")}</span>
          <span className={classes.statLabel}>{t("disease.studies.diseaseN")}</span>
        </div>
        <div className={classes.statCard}>
          <span className={classes.statValue}>{totals.control.toLocaleString("en")}</span>
          <span className={classes.statLabel}>{t("disease.studies.controlN")}</span>
        </div>
      </div>

      <div className={classes.chartCard}>
        <h3>{t("disease.tabStudies")}</h3>
        <table className={classes.generaTable}>
          <thead>
            <tr>
              <th>{t("disease.studies.projectId")}</th>
              <th>{t("disease.studies.diseaseN")}</th>
              <th>{t("disease.studies.controlN")}</th>
              <th>{t("disease.studies.country")}</th>
              <th>{t("disease.studies.cscs")}</th>
              <th>{t("disease.studies.topMarker")}</th>
            </tr>
          </thead>
          <tbody>
            {result.projects.map((project) => (
              <tr key={project.project_id}>
                <td>{project.project_id}</td>
                <td>{project.n_disease.toLocaleString("en")}</td>
                <td>{project.n_control.toLocaleString("en")}</td>
                <td>{countryName(project.country, locale)}</td>
                <td>
                  <div className={classes.cscsCell}>
                    <div className={classes.cscsTrack}>
                      <div
                        className={classes.cscsFill}
                        style={{
                          width: `${Math.max(project.cscs_score, 0)}%`,
                          background: cscsColor(project.cscs_score),
                        }}
                      />
                    </div>
                    <span>{project.cscs_score.toFixed(1)}</span>
                  </div>
                </td>
                <td>
                  {project.top_marker ? (
                    <Link to={`/species/${encodeURIComponent(project.top_marker)}`} className={classes.genusLink}>
                      {project.top_marker}
                    </Link>
                  ) : (
                    <span className={classes.mutedText}>-</span>
                  )}
                </td>
              </tr>
            ))}
            <tr>
              <td className={classes.totalRow}>{t("disease.studies.total")}</td>
              <td className={classes.totalRow}>{totals.disease.toLocaleString("en")}</td>
              <td className={classes.totalRow}>{totals.control.toLocaleString("en")}</td>
              <td className={classes.totalRow}>-</td>
              <td className={classes.totalRow}>-</td>
              <td className={classes.totalRow}>-</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default StudiesPanel;
