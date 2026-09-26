import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { useI18n } from "@/i18n";
import {
  getAnalysisJob,
  rememberedAnalysisJobs,
  rememberAnalysisJob,
  type AnalysisJob,
  type AnalysisJobKind,
  type AnalysisJobStatus,
} from "@/util/analysisJobs";

import classes from "./AnalysisJobsPage.module.css";

type TrackedJob = AnalysisJob & { kind: AnalysisJobKind };

const KIND_LABELS: Record<AnalysisJobKind, { en: string; zh: string; route: string }> = {
  "diff-analysis": { en: "Differential analysis", zh: "差异分析", route: "/compare" },
  "spearman-analysis": { en: "Spearman analysis", zh: "Spearman 分析", route: "/compare" },
  "cross-study": { en: "Cross-study meta-analysis", zh: "跨研究元分析", route: "/compare?tab=crossstudy" },
  "phenotype-association": { en: "Phenotype association", zh: "表型关联分析", route: "/phenotype" },
  "biomarker-discovery": { en: "Biomarker discovery", zh: "生物标志物发现", route: "/disease" },
  "lollipop-data": { en: "Lollipop analysis", zh: "棒棒糖图分析", route: "/disease" },
  network: { en: "Network analysis", zh: "网络分析", route: "/network" },
  cooccurrence: { en: "Co-occurrence analysis", zh: "共现分析", route: "/network" },
  "network-compare": { en: "Network comparison", zh: "网络比较", route: "/network" },
  lifecycle: { en: "Lifecycle atlas", zh: "生命周期图谱", route: "/lifecycle" },
  "lifecycle-compare": { en: "Lifecycle comparison", zh: "生命周期比较", route: "/lifecycle" },
  "similarity-search": { en: "Similarity search", zh: "相似性检索", route: "/similarity" },
  "health-index": { en: "Health index", zh: "健康指数", route: "/similarity" },
  "health-score": { en: "Health score", zh: "健康评分", route: "/similarity" },
  "disease-profile": { en: "Disease profile", zh: "疾病画像", route: "/disease" },
  "disease-studies": { en: "Disease studies", zh: "疾病研究", route: "/disease" },
  "biomarker-profile": { en: "Biomarker profile", zh: "标志物画像", route: "/species" },
  "species-cooccurrence": { en: "Species co-occurrence", zh: "菌属共现", route: "/species" },
  "chord-data": { en: "Chord analysis", zh: "弦图分析", route: "/network" },
  "phenotype-taxa-profile": { en: "Phenotype taxa profile", zh: "表型菌群画像", route: "/phenotype" },
  "metabolism-overview": { en: "Metabolism overview", zh: "代谢总览", route: "/metabolism" },
  "metabolism-category-profile": { en: "Metabolism category", zh: "代谢类别", route: "/metabolism" },
};

const STATUS_LABELS: Record<AnalysisJobStatus, { en: string; zh: string }> = {
  queued: { en: "Queued", zh: "排队中" },
  running: { en: "Running", zh: "运行中" },
  completed: { en: "Completed", zh: "已完成" },
  failed: { en: "Failed", zh: "失败" },
};

const AnalysisJobsPage = () => {
  const { locale } = useI18n();
  const navigate = useNavigate();
  const [jobs, setJobs] = useState<TrackedJob[]>([]);

  const refresh = async () => {
    const remembered = rememberedAnalysisJobs();
    const placeholders = remembered.map((item) => ({
      job_id: item.job_id,
      kind: item.kind,
      status: "queued" as const,
      created_at: "",
      updated_at: "",
    }));
    setJobs(placeholders);
    remembered.forEach(async (item) => {
      try {
        const record = await getAnalysisJob(item.job_id) as TrackedJob;
        setJobs((current) => current.map((job) => job.job_id === item.job_id ? record : job));
      } catch {
        setJobs((current) => current.map((job) => job.job_id === item.job_id
          ? { ...job, status: "failed" as const, error: locale === "zh" ? "任务记录已失效，请重新提交分析" : "This task record is no longer available; submit a new analysis" }
          : job));
      }
    });
  };

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 1500);
    const onJob = () => void refresh();
    window.addEventListener("gutbiomedb:analysis-job", onJob);
    window.addEventListener("storage", onJob);
    return () => {
      window.clearInterval(timer);
      window.removeEventListener("gutbiomedb:analysis-job", onJob);
      window.removeEventListener("storage", onJob);
    };
  }, [locale]);

  const sortedJobs = useMemo(() => jobs, [jobs]);

  const openJob = (job: TrackedJob) => {
    rememberAnalysisJob(job.kind, job.job_id);
    const separator = KIND_LABELS[job.kind].route.includes("?") ? "&" : "?";
    navigate(`${KIND_LABELS[job.kind].route}${separator}job_id=${encodeURIComponent(job.job_id)}&job_kind=${encodeURIComponent(job.kind)}`);
  };

  return (
    <main className={classes.page} id="main-content">
      <div className={classes.header}>
        <Link to="/" className={classes.back}>{locale === "zh" ? "← 返回首页" : "← Back to Home"}</Link>
        <div className={classes.titleRow}>
          <div>
            <h1>{locale === "zh" ? "任务中心" : "Analysis Jobs"}</h1>
            <p>{locale === "zh" ? "查看和重新打开你提交过的分析任务。" : "Find and reopen analysis jobs submitted from this browser."}</p>
          </div>
          <button type="button" className={classes.refresh} onClick={() => void refresh()}>
            {locale === "zh" ? "刷新状态" : "Refresh status"}
          </button>
        </div>
      </div>

      {sortedJobs.length === 0 ? (
        <section className={classes.empty}>
          {locale === "zh" ? "还没有保存的分析任务。提交任务后，ID 会出现在这里。" : "No saved analysis jobs yet. Submitted jobs will appear here with their IDs."}
        </section>
      ) : (
        <section className={classes.list} aria-label={locale === "zh" ? "分析任务列表" : "Analysis job list"}>
          {sortedJobs.map((job) => {
            const kind = KIND_LABELS[job.kind];
            const status = STATUS_LABELS[job.status];
            return (
              <article className={classes.card} key={`${job.kind}:${job.job_id}`}>
                <div className={classes.cardTop}>
                  <div>
                    <h2>{locale === "zh" ? kind.zh : kind.en}</h2>
                    <code>{job.job_id}</code>
                  </div>
                  <span className={`${classes.status} ${classes[`status_${job.status}`]}`}>
                    {locale === "zh" ? status.zh : status.en}
                  </span>
                </div>
                <div className={classes.meta}>
                  {job.updated_at
                    ? `${locale === "zh" ? "更新时间" : "Updated"}: ${new Date(job.updated_at).toLocaleString()}`
                    : (locale === "zh" ? "暂时无法读取服务端状态" : "Server status unavailable")}
                </div>
                {job.error ? <p className={classes.error}>{job.error}</p> : null}
                {job.status === "completed" ? (
                  <p className={classes.savedResult}>
                    {locale === "zh" ? "结果已保存；打开对应分析页查看可视化结果，不会重新运行。" : "Result saved; open the analysis workspace to view the visualization without rerunning."}
                  </p>
                ) : null}
                <button type="button" className={classes.open} onClick={() => openJob(job)}>
                  {locale === "zh" ? "打开对应分析页" : "Open analysis workspace"}
                </button>
              </article>
            );
          })}
        </section>
      )}
    </main>
  );
};

export default AnalysisJobsPage;
