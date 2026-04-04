# Gut Microbiome Atlas

A comprehensive platform for exploring the human gut microbiome across diseases, geography, and lifespan.

**168,464 samples | 4,680 genera | 69 countries | 217+ diseases | 8 life stages**

## Features

- **Differential Analysis** — Wilcoxon rank-sum, t-test, LEfSe (LDA effect size), PERMANOVA with BH FDR correction
- **Cross-Study Meta-Analysis** — Inverse-variance weighted random effects model (DerSimonian-Laird) with I² heterogeneity assessment
- **Gut Microbiome Health Index (GMHI)** — Novel health scoring system (0-100) based on 168K samples
- **Lifecycle Atlas** — Age-stratified microbiome composition across 8 life stages (unique feature)
- **Species Profiling** — Genus-level abundance across diseases, countries, age groups, and sex
- **Biomarker Discovery** — Wilcoxon + LDA effect size + BH FDR for differential taxa identification
- **Co-occurrence Network** — Spearman correlation-based microbial interaction networks
- **Chord Diagram** — Disease-microbe association visualization
- **Sample Similarity Search** — Bray-Curtis / Jaccard distance-based sample matching
- **Metabolic Function Browser** — Microbiota organized by metabolic role and clinical relevance
- **RESTful API** — Swagger/ReDoc documentation with Python/R code examples
- **Data Export** — CSV/TSV/JSON download + SVG/PNG chart export
- **Bilingual i18n** — English and Chinese interface

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript 5, Vite 6, D3.js 7 |
| Backend | FastAPI, Python, NumPy, SciPy, pandas |
| Styling | CSS Modules |
| Deployment | Vercel (frontend) + FastAPI (backend) |
| Rate Limiting | slowapi (120/min general, 20/min analysis) |

## Quick Start

### Prerequisites

- Node.js v18+ or Bun v1+
- Python 3.10+

### Frontend

```bash
bun install
bun run dev       # development server
bun run build     # production build
```

### Backend

```bash
cd api
pip install fastapi uvicorn pandas numpy scipy slowapi python-dotenv
python main.py    # starts on http://localhost:8000
```

### Environment Variables

Create `.env.local` in the project root:

```
METADATA_PATH=/path/to/metadata.csv
ABUNDANCE_PATH=/path/to/abundance.csv
ADMIN_TOKEN=your_admin_token
VITE_API_URL=http://localhost:8000
```

## API Documentation

Interactive API documentation is available at:
- Swagger UI: `/api/docs`
- ReDoc: `/api/redoc`
- OpenAPI spec: `/api/openapi.json`

### Example (Python)

```python
import requests

# Species profile
profile = requests.get("https://your-api/api/species-profile?genus=Bacteroides").json()
print(f"Prevalence: {profile['prevalence']:.1%}")

# Differential analysis
result = requests.post("https://your-api/api/diff-analysis", json={
    "group_a_filter": {"disease": "IBD"},
    "group_b_filter": {"disease": "NC"},
    "method": "wilcoxon"
}).json()
```

### Example (R)

```r
library(httr)
library(jsonlite)

profile <- fromJSON(content(
  GET("https://your-api/api/species-profile", query = list(genus = "Bacteroides")), "text"))
cat("Prevalence:", profile$prevalence, "\n")
```

## Citation

If you use Gut Microbiome Atlas in your research, please cite:

> Zhai J, Li Y, Liu J, Su X, Cui R, Zheng D, Sun Y, Yu J, Dai C. Gut Microbiome Atlas: a comprehensive platform for exploring human gut microbiome across diseases, geography, and lifespan. *Manuscript in preparation*, 2025.

```bibtex
@unpublished{zhai2025gutmicrobiomeatlas,
  title   = {Gut Microbiome Atlas: a comprehensive platform for exploring
             human gut microbiome across diseases, geography, and lifespan},
  author  = {Zhai, Jinxia and Li, Yingjie and Liu, Jiameng and Su, Xinyi
             and Cui, Runze and Zheng, Dianyu and Sun, Yuhan and Yu, Jingsheng
             and Dai, Cong},
  note    = {Manuscript in preparation},
  year    = {2025}
}
```

## Contact

- Correspondence: cdai@cmu.edu.cn (Prof. Cong Dai, China Medical University)
- GitHub Issues: [Report a bug](https://github.com/zhai199887/gut-microbiome-atlas/issues)

## License

MIT License. See [LICENSE](LICENSE) for details.
