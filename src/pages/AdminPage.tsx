/**
 * AdminPage.tsx
 * Data management dashboard – upload metadata, view stats, manage versions
 * 数据管理面板 – 上传元数据、查看统计、版本管理
 */
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { API_BASE } from "@/util/apiBase";
import classes from "./AdminPage.module.css";

interface DataStats {
  total_samples: number;
  total_countries: number;
  total_diseases: number;
  total_condition_categories?: number;
  last_updated: string;
  version: string;
}

interface UploadResult {
  status: string;
  message?: string;
  new_rows?: number;
  total_rows?: number;
}

interface ValidateResult {
  valid: boolean;
  rows?: number;
  columns?: string[];
  errors?: string[];
}

const AdminPage = () => {
  const [token, setToken] = useState("");
  const [authed, setAuthed] = useState(false);
  const [authError, setAuthError] = useState("");
  const [stats, setStats] = useState<DataStats | null>(null);
  const [backendAlive, setBackendAlive] = useState<boolean | null>(null);

  // Upload state / 上传状态
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [validateResult, setValidateResult] = useState<ValidateResult | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  // Check backend health / 检查后端健康状态
  useEffect(() => {
    fetch(`${API_BASE}/api/health`, { signal: AbortSignal.timeout(5000) })
      .then((r) => r.json())
      .then(() => setBackendAlive(true))
      .catch(() => setBackendAlive(false));
  }, []);

  // Load stats when authed / 登录后加载统计
  useEffect(() => {
    if (!authed) return;
    fetch(`${API_BASE}/api/data-stats`)
      .then((r) => r.json())
      .then(setStats)
      .catch(() => {});
  }, [authed]);

  const doAuth = async () => {
    setAuthError("");
    try {
      const res = await fetch(`${API_BASE}/api/admin/check`, {
        headers: { "X-Admin-Token": token },
      });
      if (res.ok) {
        setAuthed(true);
      } else {
        setAuthError("Invalid token / 无效的管理员密钥");
      }
    } catch {
      setAuthError("Cannot reach backend / 无法连接后端");
    }
  };

  const doValidate = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setValidateResult(null);
    setUploadResult(null);
    const form = new FormData();
    form.append("file", file);
    try {
      const res = await fetch(`${API_BASE}/api/admin/validate-metadata`, {
        method: "POST",
        headers: { "X-Admin-Token": token },
        body: form,
      });
      const data = await res.json();
      setValidateResult(res.ok ? data : { valid: false, errors: [data.detail ?? "Validation failed"] });
    } catch {
      setValidateResult({ valid: false, errors: ["Network error / 网络错误"] });
    }
  };

  const doUpload = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    const form = new FormData();
    form.append("file", file);
    try {
      const res = await fetch(`${API_BASE}/api/admin/upload-metadata`, {
        method: "POST",
        headers: { "X-Admin-Token": token },
        body: form,
      });
      const data = await res.json();
      setUploadResult(data);
      // Refresh stats / 刷新统计
      fetch(`${API_BASE}/api/data-stats`)
        .then((r) => r.json())
        .then(setStats)
        .catch(() => {});
    } catch {
      setUploadResult({ status: "error", message: "Upload failed / 上传失败" });
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className={classes.page}>
      <div className={classes.nav}>
        <Link to="/" className={classes.back}>← Back to Home</Link>
        <h1 className={classes.title}>Data Management</h1>
        <span className={classes.subtitle}>Upload metadata, validate format, track data versions</span>
      </div>

      {/* Backend status / 后端状态 */}
      <div className={classes.statusBar}>
        <span className={classes.statusDot} data-alive={backendAlive === true} />
        <span>
          Backend: {backendAlive === null ? "Checking…" : backendAlive ? `Online (${API_BASE})` : "Offline"}
        </span>
      </div>

      {/* Auth gate / 认证 */}
      {!authed ? (
        <section className={classes.authSection}>
          <h2>Admin Authentication</h2>
          <p>Enter admin token to access data management features.</p>
          <div className={classes.authRow}>
            <input
              type="password"
              className={classes.tokenInput}
              placeholder="Admin Token…"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && doAuth()}
            />
            <button className={classes.authBtn} onClick={doAuth} disabled={!token}>
              Verify
            </button>
          </div>
          {authError && <p className={classes.error}>{authError}</p>}
        </section>
      ) : (
        <>
          {/* Stats dashboard / 统计仪表盘 */}
          <section className={classes.statsSection}>
            <h2>Current Dataset</h2>
            {stats ? (
              <div className={classes.statsGrid}>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{stats.total_samples.toLocaleString()}</span>
                  <span className={classes.statLabel}>Total Samples</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{stats.total_countries}</span>
                  <span className={classes.statLabel}>Countries</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{stats.total_condition_categories ?? stats.total_diseases}</span>
                  <span className={classes.statLabel}>Condition Categories</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{stats.version}</span>
                  <span className={classes.statLabel}>Version</span>
                </div>
                <div className={classes.statCard}>
                  <span className={classes.statValue}>{stats.last_updated}</span>
                  <span className={classes.statLabel}>Last Updated</span>
                </div>
              </div>
            ) : (
              <p>Loading stats…</p>
            )}
          </section>

          {/* Upload section / 上传区域 */}
          <section className={classes.uploadSection}>
            <h2>Upload Metadata</h2>
            <p>Upload a CSV file with new metadata to merge into the existing dataset.</p>

            <div className={classes.uploadRow}>
              <input ref={fileRef} type="file" accept=".csv" className={classes.fileInput} />
              <button className={classes.validateBtn} onClick={doValidate}>
                Validate Format
              </button>
              <button
                className={classes.uploadBtn}
                onClick={doUpload}
                disabled={uploading}
              >
                {uploading ? "Uploading…" : "Upload & Merge"}
              </button>
            </div>

            {/* Validate result / 验证结果 */}
            {validateResult && (
              <div className={classes.resultBox} data-valid={validateResult.valid}>
                {validateResult.valid ? (
                  <>
                    <strong>Format valid</strong>
                    <span>{validateResult.rows} rows, {validateResult.columns?.length} columns</span>
                    <span>Columns: {validateResult.columns?.join(", ")}</span>
                  </>
                ) : (
                  <>
                    <strong>Validation failed</strong>
                    {validateResult.errors?.map((e, i) => <span key={i}>{e}</span>)}
                  </>
                )}
              </div>
            )}

            {/* Upload result / 上传结果 */}
            {uploadResult && (
              <div className={classes.resultBox} data-valid={uploadResult.status === "ok"}>
                <strong>{uploadResult.status === "ok" ? "Upload successful" : "Upload failed"}</strong>
                {uploadResult.message && <span>{uploadResult.message}</span>}
                {uploadResult.new_rows != null && <span>New rows: {uploadResult.new_rows}</span>}
                {uploadResult.total_rows != null && <span>Total rows: {uploadResult.total_rows}</span>}
              </div>
            )}
          </section>

          {/* API info / API信息 */}
          <section className={classes.apiSection}>
            <h2>API Endpoints</h2>
            <table className={classes.apiTable}>
              <thead>
                <tr><th>Endpoint</th><th>Method</th><th>Description</th></tr>
              </thead>
              <tbody>
                <tr><td><code>/api/health</code></td><td>GET</td><td>Health check</td></tr>
                <tr><td><code>/api/filter-options</code></td><td>GET</td><td>Filter options (countries, diseases, age groups)</td></tr>
                <tr><td><code>/api/data-stats</code></td><td>GET</td><td>Dataset statistics</td></tr>
                <tr><td><code>/api/diff-analysis</code></td><td>POST</td><td>Differential abundance analysis</td></tr>
                <tr><td><code>/api/admin/check</code></td><td>GET</td><td>Verify admin token</td></tr>
                <tr><td><code>/api/admin/validate-metadata</code></td><td>POST</td><td>Validate CSV format</td></tr>
                <tr><td><code>/api/admin/upload-metadata</code></td><td>POST</td><td>Upload & merge metadata</td></tr>
              </tbody>
            </table>
          </section>
        </>
      )}
    </div>
  );
};

export default AdminPage;