# GutBiomeDB

GutBiomeDB integrates human gut microbiome profiles with curated sample metadata for comparisons across countries, health conditions, age groups, and sex.

**168,464 samples | 482 projects | 72 countries | 225 condition categories | 4,680 taxonomic features (3,142 genera) | 7 life stages**

Website: [gutbiomedb.online](https://gutbiomedb.online)

The condition count includes the healthy-control category. The seven named life stages are accompanied by an Unknown category for samples without an age-stage assignment.

## Features

- **Differential Analysis** — Mann–Whitney U (Wilcoxon rank-sum), Welch’s t-test, linear mixed models (LMM), LEfSe, and Bray–Curtis PERMANOVA.
- **Cross-Study Meta-Analysis** — Inverse-variance-weighted DerSimonian–Laird random-effects meta-analysis with I² heterogeneity estimates.
- **GutBiomeDB Health Index (GBHI)** — A 0–100 score defined as 100 × P(NC), where NC denotes healthy controls, from a multinomial softmax classifier distinguishing NC from nine disease/condition classes.
- **Lifecycle Atlas** — Age-stratified microbiome composition and diversity across seven named life stages.
- **Genus Profiling** — Abundance and prevalence profiles across conditions, countries, age groups, and sex.
- **Biomarker Discovery** — Mann–Whitney U testing with BH correction and a custom effect score.
- **Co-occurrence Networks** — Genus-level associations inferred using SparCC via FastSpar by default, with Spearman correlation available as an alternative.
- **Chord Diagrams** — Visualisation of condition–microbe associations.
- **Sample Similarity Search** — Sample matching using Bray–Curtis or Jaccard distance.
- **Metabolic Function Browser** — Literature-curated genus-to-function annotations and abundance summaries.
- **REST API** — Interactive Swagger UI and ReDoc documentation, with Python and R examples below.
- **Data Export** — Summary statistics and analysis results in CSV, TSV, or JSON, with SVG and PNG chart export.
- **Bilingual Interface** — English and Chinese.

## Analysis methods

The Compare workspace supports user-defined groups and genus, family, or phylum aggregation. Default statistical settings are summarised below.

| Method | Test or model | Multiple-testing correction |
|---|---|---|
| Wilcoxon | Two-sided Mann–Whitney U test | Benjamini–Hochberg (BH) across taxa |
| t-test | Welch’s independent two-sample t-test | BH across taxa |
| LMM | Group as a fixed effect, with random intercepts for project, amplicon, read length, and instrument | BH across fitted taxa |
| LEfSe | Two-group analysis using `microeco::trans_diff(method="lefse")`, with Kruskal–Wallis screening and bootstrap LDA | None by default |
| PERMANOVA | Bray–Curtis distance-based group comparison with 999 permutations, using at most 300 randomly selected samples per group | Unadjusted permutation p-value |

The LEfSe option screens features at Kruskal–Wallis p < 0.05 and selects up to 20 features, ordered by screening p-value, before fitting LDA. It uses 30 bootstrap iterations with a sampling fraction of 0.6667. The two-group interface does not supply subclasses, so subclass-level Wilcoxon testing is not performed. Positive and negative LDA values in the plots indicate enrichment in groups A and B, respectively.

Compare also reports alpha diversity, Bray–Curtis or Aitchison PCoA, taxonomic composition, and a separate Spearman correlation analysis. These summaries depend on the selected samples rather than the differential test. PCoA uses up to 150 samples per group, and Compare's Spearman analysis uses up to 2,000 matched samples. PERMANOVA reports an overall community comparison; the accompanying taxon-wise panels use Wilcoxon results.

LMM coefficients are estimated on transformed and standardised abundance data. The LMM table reports these coefficients, whereas the common abundance bar chart and volcano plot use descriptive log2 fold changes. The separate Biomarker Discovery module uses a custom effect score rather than the LEfSe LDA score.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript 5, Vite 6, D3.js 7 |
| Backend | FastAPI, Python, NumPy, SciPy, pandas; R for LEfSe and LMM |
| Styling | CSS Modules |
| Deployment | Vercel frontend and a FastAPI backend on JD Cloud |
| Rate Limiting | Endpoint-specific limits implemented with slowapi |

## Quick Start

### Prerequisites

- Node.js 20+ and Bun 1+
- Python 3.10+
- R and `Rscript` for LEfSe and LMM
- FastSpar with compatible command wrappers for SparCC network analysis

### Data and configuration

Dataset links and exports of summary statistics and analysis results are provided on the [Download page](https://gutbiomedb.online/download).

For a local deployment, obtain the sample metadata and taxonomic count matrix and create `.env.local` in the project root:

```dotenv
METADATA_PATH=/path/to/metadata.csv
ABUNDANCE_PATH=/path/to/unfiltered_abundance.csv
VITE_API_URL=http://localhost:8000
```

The input files must use the platform's sample identifiers and taxonomy-column format. Data files and pretrained GBHI assets are not included in this source repository. GBHI additionally requires scikit-learn and the model and feature-cache files configured in `api/main.py`. GBHI population-distribution views also require `openpyxl` and the precomputed sample-score workbook specified by `SUPP_TABLE6_XLSX` in `api/main.py`.

If `Rscript` is not on the system path, set `LEFSE_RSCRIPT` and `LMM_RSCRIPT` to its full executable path. For a production backend, set `DEBUG=false` and `FRONTEND_URL` to the allowed frontend origin. Administrative endpoints use `ADMIN_TOKEN`.

### Backend

```bash
python -m pip install -r api/requirements.txt
```

Install the R packages used by the analysis runners:

```r
install.packages(c("microeco", "data.table", "lme4", "lmerTest", "emmeans"))
```

SparCC uses [FastSpar](https://github.com/scwatts/fastspar). The current source adapter expects Windows/WSL command wrappers named `fastspar.cmd`, `fastspar_bootstrap.cmd`, and `fastspar_pvalues.cmd` in `FASTSPAR_DIR`. Configure these wrappers for the local FastSpar installation and set `FASTSPAR_DIR` in the process environment before starting the API; they are required for the SparCC option.

Start the API:

```bash
cd api
python main.py
```

The development API listens on `http://localhost:8000`.

### Frontend

In a second terminal, run from the project root:

```bash
bun install
bun run dev
```

To build the frontend:

```bash
bun run build
```

## API Documentation

Interactive API documentation is available at:

- [Swagger UI](https://gutbiomedb.online/api/docs)
- [ReDoc](https://gutbiomedb.online/api/redoc)
- [OpenAPI specification](https://gutbiomedb.online/api/openapi.json)

### Example (Python)

Install `requests` to run the Python example.

```python
import requests

base = "https://gutbiomedb.online"

response = requests.get(
    f"{base}/api/species-profile",
    params={"genus": "Bacteroides"},
    timeout=60,
)
response.raise_for_status()
profile = response.json()
print(f"Prevalence: {profile['prevalence']:.1%}")

# Submit an analysis job, as used by the web interface.
response = requests.post(
    f"{base}/api/analysis-jobs",
    json={
        "kind": "diff-analysis",
        "payload": {
            "group_a_filter": {"disease": "IBD"},
            "group_b_filter": {"disease": "NC"},
            "taxonomy_level": "genus",
            "method": "wilcoxon",
        },
    },
    timeout=60,
)
response.raise_for_status()
job_id = response.json()["job_id"]
print(f"Job status and result: {base}/api/analysis-jobs/{job_id}")
```

Retrieve the job URL to check its status. Completed jobs include their analysis output in the `result` field.

### Example (R)

```r
library(httr)
library(jsonlite)

response <- GET(
  "https://gutbiomedb.online/api/species-profile",
  query = list(genus = "Bacteroides"),
  timeout(60)
)
stop_for_status(response)
profile <- fromJSON(content(response, "text", encoding = "UTF-8"))
cat("Prevalence (%):", 100 * profile$prevalence, "\n")
```

## Figure code

Scripts for the manuscript figures and source-data tables are maintained in a separate repository:

https://github.com/zhai199887/gutbiomedb-paper-code

Follow that repository's setup and run-order instructions. Scripts that import the platform backend require its project root or `api/` directory on `PYTHONPATH`, depending on the import statement.

## Citation

Zhai J, Li Y, Liu J, Su X, Cui R, Zheng D, Sun Y, Yu J, Dai C.
GutBiomeDB: an integrated human gut microbiome database of 168,464 samples.
Manuscript, 2026.

```bibtex
@unpublished{zhai2026gutbiomedb,
  title  = {GutBiomeDB: an integrated human gut microbiome database of 168,464 samples},
  author = {Zhai, Jinxia and Li, Yingjie and Liu, Jiameng and Su, Xinyi
            and Cui, Runze and Zheng, Dianyu and Sun, Yuhan and Yu, Jingsheng
            and Dai, Cong},
  year   = {2026},
  note   = {Manuscript}
}
```

## Contact

- Correspondence: cdai@cmu.edu.cn (Prof. Cong Dai, China Medical University)
- GitHub Issues: [Report a bug](https://github.com/zhai199887/GutBiomeDB/issues)

## License

MIT License. See [LICENSE](LICENSE) for details.
