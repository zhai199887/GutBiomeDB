"""
analysis.py — Statistical analysis utilities for GutBiomeDB
统计分析工具：标志物检验、相关性计算、相似性搜索
"""
import os
import math
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import cdist, braycurtis

try:
    import networkx as nx
except ImportError:  # pragma: no cover - dependency is optional during local smoke tests
    nx = None

try:
    import community as community_louvain
except ImportError:  # pragma: no cover - dependency is optional during local smoke tests
    community_louvain = None

FASTSPAR_WRAPPERS = {
    "fastspar": Path(r"E:\tools\fastspar\fastspar.cmd"),
    "bootstrap": Path(r"E:\tools\fastspar\fastspar_bootstrap.cmd"),
    "pvalues": Path(r"E:\tools\fastspar\fastspar_pvalues.cmd"),
}

FASTSPAR_MAIN_ITERATIONS = 50
FASTSPAR_BOOTSTRAP_CORRELATION_ITERATIONS = 5
FASTSPAR_EXCLUSION_ITERATIONS = 10
FASTSPAR_THRESHOLD = 0.1
FASTSPAR_BOOTSTRAPS = 100
FASTSPAR_SEED = 42

NETWORK_METHOD_NOTES = {
    "spearman": "",
    "sparcc": "SparCC via FastSpar with 100 bootstrap correlations and BH-FDR correction.",
}


def wilcoxon_marker_test(
    disease_matrix: np.ndarray,
    control_matrix: np.ndarray,
    taxa_names: list[str],
    p_threshold: float = 0.05,
) -> list[dict]:
    """
    Wilcoxon rank-sum test + BH FDR correction for biomarker discovery.
    Wilcoxon 秩和检验 + BH FDR 校正用于标志物发现
    """
    n_taxa = len(taxa_names)
    results = []
    p_values = []

    for i in range(n_taxa):
        d_vals = disease_matrix[:, i]
        c_vals = control_matrix[:, i]

        mean_d = float(np.mean(d_vals))
        mean_c = float(np.mean(c_vals))

        if np.std(d_vals) == 0 and np.std(c_vals) == 0:
            p_values.append(1.0)
            results.append(None)
            continue

        try:
            stat, p = stats.mannwhitneyu(d_vals, c_vals, alternative="two-sided")
        except (ValueError, TypeError):
            p_values.append(1.0)
            results.append(None)
            continue

        pseudo = 1e-6
        log2fc = math.log2((mean_d + pseudo) / (mean_c + pseudo))

        grand_mean = float(np.mean(np.concatenate([d_vals, c_vals])))
        n_d, n_c = len(d_vals), len(c_vals)
        between_var = (n_d * (mean_d - grand_mean) ** 2 +
                       n_c * (mean_c - grand_mean) ** 2) / (n_d + n_c)
        within_var = (n_d * float(np.var(d_vals)) +
                      n_c * float(np.var(c_vals))) / (n_d + n_c)

        if within_var > 0:
            lda_score = math.log10(1 + abs(between_var / within_var) * abs(mean_d - mean_c) * 1e6)
        else:
            lda_score = math.log10(1 + abs(mean_d - mean_c) * 1e6)

        prev_d = float((d_vals > 0).sum() / n_d) if n_d > 0 else 0
        prev_c = float((c_vals > 0).sum() / n_c) if n_c > 0 else 0

        diff = mean_d - mean_c
        se = float(np.sqrt(np.var(d_vals) / n_d + np.var(c_vals) / n_c))
        ci_low = diff - 1.96 * se
        ci_high = diff + 1.96 * se

        p_values.append(float(p))
        results.append({
            "taxon": taxa_names[i],
            "mean_disease": round(mean_d, 6),
            "mean_control": round(mean_c, 6),
            "log2fc": round(log2fc, 4),
            "lda_score": round(lda_score, 4),
            "p_value": round(float(p), 8),
            "adjusted_p": 0.0,
            "prevalence_disease": round(prev_d, 4),
            "prevalence_control": round(prev_c, 4),
            "enriched_in": "disease" if mean_d > mean_c else "control",
            "ci_low": round(ci_low, 6),
            "ci_high": round(ci_high, 6),
        })

    adj_p = _bh_correction(p_values)
    significant = []
    for i, r in enumerate(results):
        if r is not None:
            r["adjusted_p"] = round(adj_p[i], 8)
            if adj_p[i] < p_threshold:
                significant.append(r)

    significant.sort(key=lambda x: x["lda_score"], reverse=True)
    return significant


def spearman_cooccurrence(
    abundance_matrix: np.ndarray,
    taxa_names: list[str],
    phylum_map: dict[str, str] | None = None,
    min_prevalence: float = 0.1,
    min_abs_r: float = 0.3,
    fdr_threshold: float = 0.05,
    max_pairs: int = 500,
) -> dict:
    """
    Spearman correlation-based co-occurrence network.
    基于 Spearman 相关性的共现网络
    """
    n_samples, n_taxa = abundance_matrix.shape

    prevalences = (abundance_matrix > 0).sum(axis=0) / n_samples
    keep = prevalences >= min_prevalence
    filtered_matrix = abundance_matrix[:, keep]
    filtered_names = [taxa_names[i] for i in range(n_taxa) if keep[i]]
    n_filtered = len(filtered_names)

    if n_filtered < 2:
        return {
            "taxa": [],
            "edges": [],
            "method": "spearman",
            "method_note": NETWORK_METHOD_NOTES["spearman"],
            "tested_taxa": [],
        }

    tested_taxa = []
    for i, name in enumerate(filtered_names):
        values = filtered_matrix[:, i]
        tested_taxa.append({
            "taxon": name,
            "phylum": (phylum_map or {}).get(name, "Other"),
            "mean_abundance": round(float(values.mean()), 6),
            "prevalence": round(float((values > 0).sum() / n_samples), 4),
        })

    candidates: list[dict] = []
    p_values: list[float] = []
    for i in range(n_filtered):
        for j in range(i + 1, n_filtered):
            r, p = stats.spearmanr(filtered_matrix[:, i], filtered_matrix[:, j])
            if np.isnan(r) or np.isnan(p):
                continue
            candidates.append({
                "source": filtered_names[i],
                "target": filtered_names[j],
                "source_phylum": (phylum_map or {}).get(filtered_names[i], "Other"),
                "target_phylum": (phylum_map or {}).get(filtered_names[j], "Other"),
                "r": float(r),
                "p_value": float(p),
                "type": "positive" if r > 0 else "negative",
                "method": "spearman",
            })
            p_values.append(float(p))

    adjusted = _bh_correction(p_values)
    edges: list[dict] = []
    for candidate, adjusted_p in zip(candidates, adjusted):
        if abs(candidate["r"]) < min_abs_r or adjusted_p > fdr_threshold:
            continue
        edges.append({
            "source": candidate["source"],
            "target": candidate["target"],
            "source_phylum": candidate["source_phylum"],
            "target_phylum": candidate["target_phylum"],
            "r": round(candidate["r"], 4),
            "p_value": round(candidate["p_value"], 6),
            "adjusted_p": round(float(adjusted_p), 6),
            "type": candidate["type"],
            "method": "spearman",
        })

    edges.sort(key=lambda x: abs(x["r"]), reverse=True)
    edges = edges[:max_pairs]
    connected = {edge["source"] for edge in edges} | {edge["target"] for edge in edges}

    return {
        "taxa": [taxon for taxon in tested_taxa if taxon["taxon"] in connected],
        "edges": edges,
        "method": "spearman",
        "method_note": NETWORK_METHOD_NOTES["spearman"],
        "tested_taxa": tested_taxa,
    }


def _fastspar_available() -> bool:
    return all(path.exists() for path in FASTSPAR_WRAPPERS.values())


def _windows_to_wsl_path(path: str | Path) -> str:
    resolved = Path(path).resolve()
    drive = resolved.drive.rstrip(":").lower()
    if not drive:
        raise ValueError(f"Cannot convert path without drive letter to WSL path: {resolved}")
    suffix = str(resolved).replace("\\", "/")[2:]
    return f"/mnt/{drive}{suffix}"


def _run_fastspar_command(command: list[str], stage: str) -> None:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        return

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    detail = stderr or stdout or f"exit code {result.returncode}"
    raise RuntimeError(f"FastSpar {stage} failed: {detail}")


def _write_fastspar_otu_table(
    abundance_matrix: np.ndarray,
    taxa_names: list[str],
    sample_names: list[str],
    output_path: Path,
) -> None:
    counts = np.rint(abundance_matrix.T).astype(int)
    otu_df = pd.DataFrame(counts, index=taxa_names, columns=sample_names)
    otu_df.insert(0, "#OTU ID", otu_df.index)
    otu_df.to_csv(output_path, sep="\t", index=False)


def _read_fastspar_matrix(matrix_path: Path) -> pd.DataFrame:
    matrix = pd.read_csv(matrix_path, sep="\t", index_col=0)
    matrix.index = matrix.index.astype(str)
    matrix.columns = matrix.columns.astype(str)
    return matrix


def _run_single_bootstrap_correlation(
    otu_path: Path,
    correlation_path: Path,
    covariance_path: Path,
    seed: int,
) -> None:
    _run_fastspar_command(
        [
            str(FASTSPAR_WRAPPERS["fastspar"]),
            "-c", _windows_to_wsl_path(otu_path),
            "-r", _windows_to_wsl_path(correlation_path),
            "-a", _windows_to_wsl_path(covariance_path),
            "-i", str(FASTSPAR_BOOTSTRAP_CORRELATION_ITERATIONS),
            "-x", str(FASTSPAR_EXCLUSION_ITERATIONS),
            "-e", str(FASTSPAR_THRESHOLD),
            "-t", "1",
            "-s", str(seed),
            "-y",
        ],
        stage=f"bootstrap correlation ({otu_path.name})",
    )


def fastspar_cooccurrence(
    abundance_matrix: np.ndarray,
    taxa_names: list[str],
    phylum_map: dict[str, str] | None = None,
    min_prevalence: float = 0.1,
    min_abs_r: float = 0.3,
    fdr_threshold: float = 0.05,
    max_pairs: int = 500,
) -> dict:
    """
    SparCC/ FastSpar-based co-occurrence network for compositional count data.
    鍩轰簬 FastSpar 鐨?SparCC 鍏辩幇缃戠粶锛岀敤浜庣粍鎴愭暟鎹殑鐪熷疄鐩稿叧鎬ц绠?
    """
    if not _fastspar_available():
        raise RuntimeError(
            "SparCC is unavailable because E:\\tools\\fastspar\\fastspar*.cmd wrappers were not found."
        )

    n_samples, n_taxa = abundance_matrix.shape
    prevalences = (abundance_matrix > 0).sum(axis=0) / n_samples
    keep = prevalences >= min_prevalence
    filtered_matrix = abundance_matrix[:, keep]
    filtered_names = [taxa_names[i] for i in range(n_taxa) if keep[i]]

    if len(filtered_names) < 2:
        return {
            "taxa": [],
            "edges": [],
            "method": "sparcc",
            "method_note": NETWORK_METHOD_NOTES["sparcc"],
            "tested_taxa": [],
        }

    tested_taxa = []
    for i, name in enumerate(filtered_names):
        values = filtered_matrix[:, i]
        tested_taxa.append({
            "taxon": name,
            "phylum": (phylum_map or {}).get(name, "Other"),
            "mean_abundance": round(float(values.mean()), 6),
            "prevalence": round(float((values > 0).sum() / n_samples), 4),
        })

    threads = max(1, min(os.cpu_count() or 1, 8))
    sample_names = [f"sample_{idx + 1}" for idx in range(filtered_matrix.shape[0])]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        otu_path = temp_path / "otu_counts.tsv"
        correlation_path = temp_path / "sparcc_correlation.tsv"
        covariance_path = temp_path / "sparcc_covariance.tsv"
        bootstrap_counts_dir = temp_path / "bootstrap_counts"
        bootstrap_corr_dir = temp_path / "bootstrap_correlation"
        bootstrap_counts_dir.mkdir(parents=True, exist_ok=True)
        bootstrap_corr_dir.mkdir(parents=True, exist_ok=True)

        bootstrap_prefix = bootstrap_counts_dir / "otu_"
        bootstrap_corr_prefix = bootstrap_corr_dir / "cor_"
        bootstrap_cov_prefix = bootstrap_corr_dir / "cov_"
        pvalue_path = temp_path / "sparcc_pvalues.tsv"

        _write_fastspar_otu_table(filtered_matrix, filtered_names, sample_names, otu_path)

        _run_fastspar_command(
            [
                str(FASTSPAR_WRAPPERS["fastspar"]),
                "-c", _windows_to_wsl_path(otu_path),
                "-r", _windows_to_wsl_path(correlation_path),
                "-a", _windows_to_wsl_path(covariance_path),
                "-i", str(FASTSPAR_MAIN_ITERATIONS),
                "-x", str(FASTSPAR_EXCLUSION_ITERATIONS),
                "-e", str(FASTSPAR_THRESHOLD),
                "-t", str(threads),
                "-s", str(FASTSPAR_SEED),
                "-y",
            ],
            stage="main correlation",
        )

        _run_fastspar_command(
            [
                str(FASTSPAR_WRAPPERS["bootstrap"]),
                "-c", _windows_to_wsl_path(otu_path),
                "-n", str(FASTSPAR_BOOTSTRAPS),
                "-p", _windows_to_wsl_path(bootstrap_prefix),
                "-t", str(threads),
                "-s", str(FASTSPAR_SEED),
            ],
            stage="bootstrap generation",
        )

        bootstrap_files = sorted(bootstrap_counts_dir.glob("otu__*.tsv"))
        if len(bootstrap_files) != FASTSPAR_BOOTSTRAPS:
            raise RuntimeError(
                f"FastSpar bootstrap generation returned {len(bootstrap_files)} files, expected {FASTSPAR_BOOTSTRAPS}."
            )

        max_workers = max(1, min(4, threads))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for index, bootstrap_file in enumerate(bootstrap_files):
                futures.append(
                    executor.submit(
                        _run_single_bootstrap_correlation,
                        bootstrap_file,
                        Path(f"{bootstrap_corr_prefix}_{index}.tsv"),
                        Path(f"{bootstrap_cov_prefix}_{index}.tsv"),
                        FASTSPAR_SEED + index,
                    )
                )
            for future in futures:
                future.result()

        _run_fastspar_command(
            [
                str(FASTSPAR_WRAPPERS["pvalues"]),
                "-c", _windows_to_wsl_path(otu_path),
                "-r", _windows_to_wsl_path(correlation_path),
                "-p", _windows_to_wsl_path(bootstrap_corr_prefix),
                "-n", str(FASTSPAR_BOOTSTRAPS),
                "-o", _windows_to_wsl_path(pvalue_path),
                "-t", str(threads),
            ],
            stage="p-value estimation",
        )

        correlation_df = _read_fastspar_matrix(correlation_path)
        pvalue_df = _read_fastspar_matrix(pvalue_path)

    ordered_taxa = [taxon for taxon in filtered_names if taxon in correlation_df.index and taxon in pvalue_df.index]
    correlation_df = correlation_df.loc[ordered_taxa, ordered_taxa]
    pvalue_df = pvalue_df.loc[ordered_taxa, ordered_taxa]

    candidates: list[dict] = []
    raw_p_values: list[float] = []
    for i in range(len(ordered_taxa)):
        for j in range(i + 1, len(ordered_taxa)):
            source = ordered_taxa[i]
            target = ordered_taxa[j]
            r = float(correlation_df.iloc[i, j])
            p_value = float(pvalue_df.iloc[i, j])
            if np.isnan(r) or np.isnan(p_value):
                continue
            candidates.append({
                "source": source,
                "target": target,
                "source_phylum": (phylum_map or {}).get(source, "Other"),
                "target_phylum": (phylum_map or {}).get(target, "Other"),
                "r": r,
                "p_value": p_value,
                "type": "positive" if r > 0 else "negative",
                "method": "sparcc",
            })
            raw_p_values.append(p_value)

    adjusted = _bh_correction(raw_p_values)
    edges: list[dict] = []
    for candidate, adjusted_p in zip(candidates, adjusted):
        if abs(candidate["r"]) < min_abs_r or adjusted_p > fdr_threshold:
            continue
        edges.append({
            "source": candidate["source"],
            "target": candidate["target"],
            "source_phylum": candidate["source_phylum"],
            "target_phylum": candidate["target_phylum"],
            "r": round(candidate["r"], 4),
            "p_value": round(candidate["p_value"], 6),
            "adjusted_p": round(float(adjusted_p), 6),
            "type": candidate["type"],
            "method": "sparcc",
        })

    edges.sort(key=lambda edge: abs(edge["r"]), reverse=True)
    edges = edges[:max_pairs]
    connected = {edge["source"] for edge in edges} | {edge["target"] for edge in edges}

    return {
        "taxa": [taxon for taxon in tested_taxa if taxon["taxon"] in connected],
        "edges": edges,
        "method": "sparcc",
        "method_note": NETWORK_METHOD_NOTES["sparcc"],
        "tested_taxa": tested_taxa,
    }


def available_network_methods() -> dict[str, bool]:
    """Return network methods that are actually runnable in the current backend."""
    return {
        "spearman": True,
        "sparcc": _fastspar_available(),
    }


def compute_network_topology(taxa: list[dict], edges: list[dict]) -> dict:
    """
    Compute graph-level and node-level topology statistics.
    计算网络拓扑指标（度、介数中心性、群落、枢纽节点等）
    """
    degree = {taxon["taxon"]: 0 for taxon in taxa}
    betweenness = {taxon["taxon"]: 0.0 for taxon in taxa}
    community_map = {taxon["taxon"]: 0 for taxon in taxa}

    if not taxa:
        return {
            "degree": degree,
            "betweenness": betweenness,
            "community": community_map,
            "hub_nodes": [],
            "n_communities": 0,
            "network_density": 0.0,
            "positive_edge_count": 0,
            "negative_edge_count": 0,
        }

    if nx is None:
        for edge in edges:
            degree[edge["source"]] = degree.get(edge["source"], 0) + 1
            degree[edge["target"]] = degree.get(edge["target"], 0) + 1
        hub_nodes = _hub_nodes_from_degree(degree)
        return {
            "degree": degree,
            "betweenness": betweenness,
            "community": community_map,
            "hub_nodes": hub_nodes,
            "n_communities": len({community_map[name] for name in community_map}) if community_map else 0,
            "network_density": 0.0,
            "positive_edge_count": sum(1 for edge in edges if edge["type"] == "positive"),
            "negative_edge_count": sum(1 for edge in edges if edge["type"] == "negative"),
        }

    graph = nx.Graph()
    for taxon in taxa:
        graph.add_node(taxon["taxon"])
    for edge in edges:
        weight = max(abs(float(edge["r"])), 1e-6)
        graph.add_edge(
            edge["source"],
            edge["target"],
            weight=weight,
            distance=1.0 / weight,
            sign=edge["type"],
        )

    degree = {node: int(val) for node, val in graph.degree()}
    if graph.number_of_edges() > 0:
        betweenness = {
            node: round(float(value), 6)
            for node, value in nx.betweenness_centrality(graph, weight="distance", normalized=True).items()
        }
    else:
        betweenness = {node: 0.0 for node in graph.nodes}

    if graph.number_of_edges() > 0 and community_louvain is not None:
        partition = community_louvain.best_partition(graph, weight="weight", random_state=42)
    else:
        partition = {}
        for community_idx, component in enumerate(nx.connected_components(graph)):
            for node in component:
                partition[node] = community_idx
        for node in graph.nodes:
            partition.setdefault(node, 0)

    hub_nodes = _hub_nodes_from_degree(degree)
    return {
        "degree": degree,
        "betweenness": betweenness,
        "community": partition,
        "hub_nodes": hub_nodes,
        "n_communities": len(set(partition.values())) if partition else 0,
        "network_density": round(float(nx.density(graph)), 6),
        "positive_edge_count": sum(1 for edge in edges if edge["type"] == "positive"),
        "negative_edge_count": sum(1 for edge in edges if edge["type"] == "negative"),
    }


def compare_network_edges(disease_edges: list[dict], control_edges: list[dict]) -> dict:
    """
    Compare disease and control networks by edge membership and direction.
    对比疾病网络与健康对照网络的边集变化
    """
    disease_map = {_edge_key(edge): edge for edge in disease_edges}
    control_map = {_edge_key(edge): edge for edge in control_edges}

    gained = []
    lost = []
    sign_switched = []

    for key, edge in disease_map.items():
        control_edge = control_map.get(key)
        if control_edge is None:
            gained.append(_serialize_compare_edge(edge, "disease_only"))
        elif control_edge["type"] != edge["type"]:
            sign_switched.append({
                "source": edge["source"],
                "target": edge["target"],
                "disease_type": edge["type"],
                "control_type": control_edge["type"],
                "disease_r": edge["r"],
                "control_r": control_edge["r"],
            })

    for key, edge in control_map.items():
        if key not in disease_map:
            lost.append(_serialize_compare_edge(edge, "control_only"))

    rewired = sorted(gained + lost, key=lambda item: abs(item["r"]), reverse=True)
    return {
        "gained_edges": len(gained),
        "lost_edges": len(lost),
        "rewired_edges": rewired,
        "sign_switched_edges": sign_switched,
    }


def sample_similarity_search(
    query_vector: np.ndarray,
    abundance_matrix: np.ndarray,
    sample_keys: list[str],
    metric: str = "braycurtis",
    top_k: int = 10,
) -> list[dict]:
    """
    Find top-K most similar samples to a query abundance vector.
    查找与查询丰度向量最相似的 Top-K 样本
    """
    q_total = query_vector.sum()
    if q_total > 0:
        query_rel = query_vector / q_total * 100
    else:
        return []

    ref_totals = abundance_matrix.sum(axis=1, keepdims=True)
    ref_totals[ref_totals == 0] = 1
    ref_rel = abundance_matrix / ref_totals * 100

    distances = cdist(query_rel.reshape(1, -1), ref_rel, metric=metric)[0]

    top_indices = np.argsort(distances)[:top_k]
    results = []
    for idx in top_indices:
        similarity_pct = max(0.0, float(1 - distances[idx]) * 100.0)
        results.append({
            "sample_key": sample_keys[idx],
            "distance": round(float(distances[idx]), 6),
            "similarity_pct": round(similarity_pct, 2),
        })
    return results


def _bh_correction(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg FDR correction."""
    n = len(p_values)
    if n == 0:
        return []
    indexed = sorted(enumerate(p_values), key=lambda x: x[1])
    adjusted = [0.0] * n
    prev_adj = 1.0
    for rank, (orig_idx, p) in enumerate(reversed(indexed)):
        adj = min(prev_adj, p * n / (n - rank))
        adjusted[orig_idx] = min(adj, 1.0)
        prev_adj = adj
    return adjusted


def _hub_nodes_from_degree(degree: dict[str, int]) -> list[str]:
    if not degree:
        return []
    ranked = sorted(degree.values(), reverse=True)
    cutoff_index = max(0, math.ceil(len(ranked) * 0.1) - 1)
    cutoff = ranked[cutoff_index]
    return [node for node, value in degree.items() if value >= cutoff and value > 0]


def _edge_key(edge: dict) -> tuple[str, str]:
    return tuple(sorted((str(edge["source"]), str(edge["target"]))))


def _serialize_compare_edge(edge: dict, present_in: str) -> dict:
    return {
        "source": edge["source"],
        "target": edge["target"],
        "present_in": present_in,
        "r": edge["r"],
        "adjusted_p": edge.get("adjusted_p", edge.get("p_value", 1.0)),
        "type": edge["type"],
        "source_phylum": edge.get("source_phylum", "Other"),
        "target_phylum": edge.get("target_phylum", "Other"),
    }