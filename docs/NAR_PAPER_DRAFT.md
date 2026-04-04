# Gut Microbiome Atlas: a comprehensive platform for exploring human gut microbiome diversity across diseases, geography, and lifespan

**Jinxia Zhai^1^, Yingjie Li^2^, Jiameng Liu^1^, Xinyi Su^1^, Runze Cui^1^, Dianyu Zheng^1^, Yuhan Sun^1^, Jingsheng Yu^3^, Cong Dai^1,\*^**

^1^ Department of Gastroenterology, First Hospital of China Medical University, Shenyang City, Liaoning Province, China.

^2^ Department of Gastroenterology, First Affiliated Hospital, Jinzhou Medical University, Jinzhou City, Liaoning Province, China.

^3^ Department of Rare Diseases, Peking Union Medical College Hospital, Peking Union Medical College and Chinese Academy of Medical Science, Beijing, China.

\* To whom correspondence should be addressed. Email: cdai@cmu.edu.cn

---

## ABSTRACT

The human gut microbiome plays a pivotal role in health and disease, yet no existing resource provides an integrated platform that simultaneously enables comprehensive cross-disease, cross-geography, and cross-lifespan exploration of large-scale 16S rRNA gene sequencing data. Here we present Gut Microbiome Atlas (https://compendiumwebsite.vercel.app), an open-access database integrating 168,464 human gut 16S amplicon samples from 482 BioProjects spanning 66 countries, 218 disease conditions, and 8 life stages from Infant to Centenarian. All samples were uniformly processed through a standardized bioinformatics pipeline, yielding genus-level taxonomic profiles for 4,680 taxa. The platform provides an extensive suite of analytical tools including two-group differential analysis (Wilcoxon rank-sum test, t-test, LEfSe, PERMANOVA), cross-study meta-analysis with DerSimonian-Laird random effects model, Gut Microbiome Health Index (GMHI) scoring, sample similarity search (Bray-Curtis/Jaccard), disease biomarker discovery, microbial co-occurrence network analysis, lifecycle composition atlas, and disease-microbe association network visualization. All results are exportable in CSV, SVG, and PNG formats. A disease ontology system maps 204 diseases to MeSH and ICD-10 codes. Gut Microbiome Atlas fills a critical gap by combining the largest curated gut microbiome sample collection with the most comprehensive analytical toolkit among comparable resources, providing researchers with a one-stop platform for hypothesis generation and validation in gut microbiome research.

**Database URL:** https://compendiumwebsite.vercel.app

---

## INTRODUCTION

The human gut harbors a dense and diverse microbial community that profoundly influences host physiology, immunity, metabolism, and susceptibility to disease (1-3). Advances in 16S rRNA gene sequencing have generated vast quantities of gut microbiome data deposited in public repositories such as the NCBI Sequence Read Archive (SRA) and European Nucleotide Archive (ENA). However, the heterogeneity of experimental protocols, metadata annotation, and analytical pipelines across thousands of independent studies presents a formidable barrier to meaningful data reuse and cross-study comparison (4, 5).

Several databases have been developed to address aspects of this challenge. GMrepo, the most closely related resource, curates human gut metagenomes with consistent annotation and has grown from 71,642 samples in v2 (6) to 118,965 samples in v3 (7), identifying 1,299 marker taxa across 302 phenotypes using LEfSe analysis and a Marker Consistency Index (MCI). gutMDisorder (8) catalogues literature-curated dysbiosis associations between microbes and disorders but does not provide sample-level analysis. MicrobiomeDB (9) offers an integrated mining platform but is limited in scale and analytical depth. Disbiome (10) provides a manually curated collection of microbe-disease associations. MASI (11) focuses on microbiota-active substance interactions. Peryton (12) collects experimentally validated microbe-disease associations. VMH (13) integrates metabolic reconstructions with nutrition data. gutMEGA (14) provides a metagenomic atlas but with limited interactive analysis tools.

Despite these valuable resources, significant gaps remain. First, no existing database provides a comprehensive analytical toolkit that integrates differential analysis, cross-study meta-analysis, health index scoring, sample similarity search, and network analysis within a single platform. Second, geographic and lifespan coverage remains incomplete—few resources systematically characterize gut microbiome variation across the full human lifespan and diverse populations simultaneously. Third, the capacity for cross-cohort statistical analysis that accounts for study-level heterogeneity is largely absent from current platforms.

To address these limitations, we developed Gut Microbiome Atlas, a comprehensive platform integrating 168,464 human gut 16S rRNA gene sequencing samples from 482 BioProjects spanning 66 countries, 218 disease conditions, and 8 life stages. The platform provides the largest curated gut microbiome sample collection and the most extensive analytical toolkit among comparable resources, enabling researchers to conduct sophisticated cross-disease, cross-geography, and cross-lifespan analyses from a single unified interface.

## DATABASE CONTENT AND CONSTRUCTION

### Data collection and curation

We systematically retrieved human gut microbiome datasets from the NCBI SRA following a search strategy adapted from recent large-scale meta-analyses (15). Using the query "human gut metagenome," we initially identified 245,627 samples from 1,437 BioProjects with library_source = genomic/metagenomic and library_strategy = amplicon. A multi-step quality control pipeline was applied:

1. **Project-level filtering**: BioProjects with fewer than 50 samples were excluded, retaining 234,875 samples from 811 BioProjects.
2. **Data type verification**: 31,887 samples representing fungal/archaeal amplicons, incorrectly labeled metagenomes, or nanopore-only runs were removed.
3. **Chimera detection**: For each BioProject, the first 10 samples were screened; projects where ≥5 of 10 samples contained >25% chimeric sequences were excluded (31,509 samples removed).
4. **Empty sample removal**: 807 samples with zero reads after quality control were discarded.
5. **Read depth filtering**: Samples with fewer than 10,000 reads were removed (16,781 samples, 10%).
6. **Low-abundance taxon filtering**: Taxa with total reads <1,000 (2,485 taxa) or detected in fewer than 100 samples (681 taxa) were excluded.
7. **Unclassified read filtering**: Samples with >10% phylum-level unclassified reads were removed (30,061 samples).

This yielded a final high-quality analytical dataset of 121,601 samples × 1,514 taxa for downstream statistical analysis. The full display dataset of 168,464 samples × 4,680 genera is accessible through the platform for exploratory browsing and visualization.

### Metadata harmonization

Sample metadata was systematically curated across 27 fields including accession identifiers (SRR, SRS, BioProject), sequencing parameters (library_strategy, library_source, instrument, total_bases), geographic information (geo_loc_name, ISO country code, world region), demographic variables (age, sex, age_group), and disease phenotype annotations (inform0 through inform11, supporting multi-disease co-occurrence). Age groups were standardized into eight categories: Infant (0-2 years), Child (3-12), Adolescent (13-17), Adult (18-59), Older Adult (60-79), Oldest Old (80-99), Centenarian (100+), and Unknown. Country codes were harmonized to ISO 3166-1 alpha-2 format, with Taiwan (TW), Hong Kong (HK), and Macau (MO) merged into China (CN), yielding 66 distinct countries. Disease phenotypes were mapped to a standardized ontology of 204 conditions with Medical Subject Headings (MeSH) identifiers and ICD-10 codes organized into 15 clinical categories.

### Taxonomic profiling

All 16S rRNA gene sequences were processed through a uniform bioinformatics pipeline. Raw reads were quality-filtered and denoised using DADA2, with chimera removal via consensus-based detection. Taxonomy was assigned using the SILVA 138 reference database at 99% sequence identity. Genus-level relative abundances were computed for all samples, yielding profiles across 4,680 genera spanning six taxonomic kingdoms. Sample identifiers were standardized to the format BioProject_RunAccession (e.g., PRJDB10485_DRR243823) to enable unambiguous cross-project linkage.

## WEB INTERFACE AND ANALYTICAL TOOLS

Gut Microbiome Atlas provides a feature-rich web interface accessible at https://compendiumwebsite.vercel.app. The platform is organized into 12 functional modules (Figure 1):

### Home page and data overview

The landing page presents an interactive overview of the database contents, including a D3.js-powered choropleth world map displaying geographic sample distribution, a Sankey diagram illustrating phylum-to-genus taxonomic flow, bar charts of disease and age group distributions, and a global filter panel enabling real-time data subsetting by sex, age group, and disease condition.

### Species/genus search engine

Users can search for any genus by name with autocomplete support. Search results display a comprehensive profile including detection statistics (total samples, prevalence, mean abundance), D3.js horizontal bar charts showing distribution across diseases, countries, age groups, and sex categories, and clickable links to detailed genus pages. A search history feature with localStorage persistence enables rapid revisiting of prior queries.

### Two-group differential analysis

The differential analysis module enables researchers to compare microbiome composition between any two user-defined sample groups. Groups can be specified by disease condition, country, age group, and sex. Four statistical methods are available:

- **Wilcoxon rank-sum test** with Benjamini-Hochberg false discovery rate (BH-FDR) correction
- **Student's t-test** with BH-FDR correction
- **LEfSe** (Linear discriminant analysis Effect Size): Kruskal-Wallis test followed by LDA effect size estimation
- **PERMANOVA** (Permutational Multivariate Analysis of Variance): 999 permutations using Bray-Curtis distance

Results are visualized as interactive volcano plots, LEfSe bar charts, alpha diversity (Shannon/Simpson) box plots, and beta diversity (Bray-Curtis) PCoA ordination plots. All visualizations support CSV data export and SVG/PNG image export.

### Cross-study meta-analysis

A unique feature of Gut Microbiome Atlas is the cross-study meta-analysis module, which addresses the critical challenge of synthesizing results across heterogeneous cohorts. Users select a disease condition and multiple BioProjects to compare. The analysis pipeline:

1. Performs independent per-project differential analysis (disease vs. healthy control)
2. Computes study-level effect sizes (log~2~ fold change) and standard errors
3. Applies inverse-variance weighted fixed-effects meta-analysis
4. Estimates between-study heterogeneity using Cochran's Q test and the I^2^ statistic
5. Computes DerSimonian-Laird random-effects model estimates when I^2^ > 25%
6. Identifies consensus markers with meta-analysis P < 0.05

Results are displayed as forest plots showing per-project and pooled effect sizes with 95% confidence intervals, and heatmaps visualizing taxa-by-project effect patterns. This approach avoids the pitfalls of naive data pooling by treating each study as an independent unit, consistent with established meta-analytic methodology (16).

### Gut Microbiome Health Index (GMHI)

The GMHI module provides a personalized health assessment tool. Users upload or paste their own genus-level abundance data, and the system calculates a score from 0 to 100 based on the ratio of health-associated to disease-associated genera derived from the non-disease control (NC) population in our database. The score is visualized as a D3.js semicircular gauge chart with red-yellow-green gradient coloring. A detailed deviation table shows how each genus in the user's sample compares to the NC reference population (mean and median abundances), with status badges indicating enrichment (↑), depletion (↓), or normal range (—).

### Disease browser with biomarker discovery

The disease browser presents a searchable catalog of 218 disease conditions with sample counts. Selecting a disease displays a comprehensive profile including:

- **Composition overview**: Top 20 genera with disease vs. healthy control comparison (mean abundance, prevalence, log~2~ fold change)
- **Biomarker discovery**: Wilcoxon rank-sum test with BH-FDR correction and LDA effect size estimation, identifying candidate biomarker genera with forest plot visualization
- **Differential abundance**: Lollipop plot showing log~2~ fold change for all significantly altered genera, with point size proportional to -log~10~(P) and color encoding phylum-level taxonomy
- **Demographic breakdown**: Distribution by country, age group, and sex

Disease names are standardized using a comprehensive ontology mapping 204 conditions to MeSH identifiers and ICD-10 codes across 15 clinical categories.

### Network visualization

Three complementary network views are provided:

- **Disease-microbe association network**: D3.js force-directed graph showing top diseases and their most strongly associated genera, with node size proportional to sample count and edge weight proportional to mean abundance
- **Chord diagram**: D3.js chord layout displaying the full disease-genus association matrix, with phylum-level color encoding and interactive hover highlighting
- **Co-occurrence network**: Spearman rank correlation-based co-occurrence analysis with configurable |r| threshold, positive/negative correlation edge coloring, and disease-specific network exploration

### Lifecycle atlas

The lifecycle module visualizes gut microbiome composition changes across 8 life stages from Infant to Centenarian using D3.js stacked area charts. Users can filter by disease or country to observe how age-related microbiome trajectories differ across populations and health conditions.

### Sample similarity search

Users can upload their own genus-level abundance profiles and search for the most similar samples in the database using Bray-Curtis dissimilarity or Jaccard distance metrics. The top 10 most similar samples are returned with their metadata (disease, country, age group, sex), enabling hypothesis generation about disease status based on microbiome composition.

### Metabolic function browser

The metabolic function module categorizes gut microbiome genera into 15 functional categories including short-chain fatty acid (SCFA) producers, bile acid metabolizers, tryptophan metabolizers, sulfur metabolizers, TMAO producers, LPS producers, folate/B12 producers, neurotransmitter-related genera, mucin degraders, amino acid fermenters, hydrogen/methane metabolizers, lactate producers, oxalate degraders, polysaccharide degraders, and pathobionts.

### Data download

Aggregated summary statistics, disease profiles, species profiles, and genus lists are available for download in CSV format. Individual sample-level raw data is not provided to protect data contributor privacy, but all underlying BioProject accessions are listed for users requiring raw sequence access.

### API documentation

A RESTful API with 33 endpoints provides programmatic access to all platform functionality. Interactive documentation is available through Swagger UI and ReDoc interfaces. Usage examples in Python, R, and cURL are provided. Rate limiting (5 tiers from 10 to 120 requests/minute) ensures equitable resource sharing.

## COMPARISON WITH EXISTING DATABASES

Table 1 compares Gut Microbiome Atlas with nine existing gut microbiome databases.

| Feature | **Gut Microbiome Atlas** | GMrepo v3 | ResMicroDb | MicrobiomeDB | gutMDisorder v2 | gutMEGA | Disbiome | VMH | Peryton | MASI |
|---------|------------------------|-----------|-----------|--------------|----------------|---------|----------|-----|---------|------|
| **Body site** | Gut | Gut | Respiratory | Multiple | Gut | Gut | Multiple | Gut | Multiple | Gut |
| **Total samples** | **168,464** | 118,965 | 106,464 | ~20,000 | N/A (literature) | ~3,500 | N/A | N/A | N/A | N/A |
| **Countries** | **66** | Not reported | 54 | Limited | N/A | Limited | N/A | N/A | N/A | N/A |
| **Diseases/phenotypes** | 218 | 302 | 146 | Limited | 316 | 209 | 300+ | N/A | 43 | N/A |
| **Taxa profiled** | **4,680 genera** | 1,299 markers | 2,373 species | Variable | N/A | Limited | N/A | ~800 species | N/A | N/A |
| **Ontology mapping** | MeSH + ICD-10 | MeSH | EFO/Mondo/DO | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Diff. analysis** | **4 methods** | LEfSe | MaAsLin2 + 4 | Limited | N/A | N/A | N/A | N/A | N/A | N/A |
| **Cross-study meta** | **DerSimonian-Laird** | MCI | Cross-study | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Health index** | **GMHI** | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Similarity search** | **Bray-Curtis/Jaccard** | N/A | Bray-Curtis/Jaccard/JSD | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Network analysis** | **3 types** | N/A | SparCC | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Lifecycle atlas** | **8 stages** | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Export formats** | CSV/SVG/PNG | Download | Download | Download | N/A | Download | N/A | Download | N/A | N/A |
| **i18n** | **EN/ZH bilingual** | EN/ZH | EN | EN | EN/ZH | EN | EN | EN | EN | EN/ZH |
| **Data type** | Sample-level | Sample-level | Sample-level | Sample-level | Literature | Sample-level | Literature | Model | Literature | Literature |
| **Open source** | **Yes (MIT)** | Partial | No | Yes | No | No | No | No | No | No |

Gut Microbiome Atlas offers several unique advantages: (i) the largest sample collection (168,464) among gut-specific databases, (ii) the broadest geographic coverage (66 countries), (iii) the most comprehensive analytical toolkit integrating 10 statistical methods, (iv) the only platform offering cross-study meta-analysis with formal heterogeneity assessment, (v) the first gut microbiome database with a built-in health index scoring system, and (vi) the only platform providing lifecycle-spanning composition visualization across 8 age stages.

## IMPLEMENTATION

### Frontend architecture

The web interface is built with React 19, TypeScript 5, and Vite 6 as a single-page application (SPA). All interactive visualizations are implemented using D3.js v7, providing 27 distinct chart types across 15 pages. The application employs CSS Modules for scoped styling, React Context-based internationalization (754 translation keys in English and Chinese), Intersection Observer API for lazy rendering, and route-level code splitting to optimize initial load performance. The frontend is deployed on Vercel with automatic continuous deployment from the GitHub repository.

### Backend architecture

The backend API server is built with FastAPI (Python), providing 33 RESTful endpoints. Data processing leverages pandas for metadata operations, scipy for statistical testing (Wilcoxon, Spearman), and scikit-learn for distance computations (Bray-Curtis, Jaccard). Rate limiting is implemented via slowapi with 5 tiers. Security headers (X-Content-Type-Options, X-Frame-Options, Referrer-Policy) are applied via custom middleware. CORS is configured for the production frontend domain. The backend serves the 168,464-sample metadata (28.7 MB) and 1.5 GB genus-level abundance matrix.

### Data availability and reproducibility

The Gut Microbiome Atlas is freely available at https://compendiumwebsite.vercel.app. The complete source code is released under the MIT License at https://github.com/zhai199887/gut-microbiome-atlas. API documentation is accessible at the /api-docs route and via Swagger UI. All underlying data derives from publicly deposited BioProjects in the NCBI SRA; individual accession numbers are provided in the database for full reproducibility.

## FUTURE DIRECTIONS

Planned enhancements include: (i) integration of whole-genome shotgun metagenomic profiles using MetaPhlAn4 for species-level resolution, (ii) expansion of the cross-study meta-analysis module with batch-effect correction via ComBat-seq, (iii) addition of functional pathway prediction using PICRUSt2, (iv) implementation of machine learning-based disease classifiers, and (v) deployment on a dedicated cloud server for improved performance and availability.

## FUNDING

This work was supported by the National Natural Science Foundation of China (Grant No. 82270571 and Grant No. 82570632 to C.D.).

## CONFLICT OF INTEREST

The authors declare no competing interests.

## REFERENCES

1. Lynch, S.V. and Pedersen, O. (2016) The Human Intestinal Microbiome in Health and Disease. *N. Engl. J. Med.*, **375**, 2369-2379.
2. Fan, Y. and Pedersen, O. (2021) Gut microbiota in human metabolic health and disease. *Nat. Rev. Microbiol.*, **19**, 55-71.
3. Gilbert, J.A., Blaser, M.J., Caporaso, J.G., et al. (2018) Current understanding of the human microbiome. *Nat. Med.*, **24**, 392-400.
4. Schloss, P.D. (2018) Identifying and Overcoming Threats to Reproducibility, Replicability, Robustness, and Generalizability in Microbiome Research. *mBio*, **9**, e00525-18.
5. Mirzayi, C., Renson, A., Genomic Standards Consortium, et al. (2021) Reporting guidelines for human microbiome research: the STORMS checklist. *Nat. Med.*, **27**, 1885-1892.
6. Wu, S., Sun, C., Li, Y., et al. (2022) GMrepo v2: a curated human gut microbiome database with special focus on disease markers and cross-dataset comparison. *Nucleic Acids Res.*, **50**, D1089-D1099.
7. GMrepo Team (2026) GMrepo v3: a curated human gut microbiome database with expanded disease coverage and enhanced cross-dataset biomarker analysis. *Nucleic Acids Res.*, DOI: 10.1093/nar/gkaf1190.
8. Cheng, L., Qi, C., Zhuang, H., et al. (2023) gutMDisorder v2.0: a comprehensive database for dysbiosis of gut microbiota in phenotypes and interventions. *Nucleic Acids Res.*, **51**, D1603-D1613.
9. Oliveira, F.S., Brestelli, J., Cade, S., et al. (2018) MicrobiomeDB: a systems biology platform for integrating, mining and analyzing microbiome experiments. *Nucleic Acids Res.*, **46**, D684-D691.
10. Janssens, Y., Nielandt, J., Bronselaer, A., et al. (2018) Disbiome database: linking the microbiome to disease. *BMC Microbiol.*, **18**, 50.
11. Zeng, X., Yang, X., Fan, J., et al. (2021) MASI: microbiota-active substance interactions database. *Nucleic Acids Res.*, **49**, D776-D782.
12. Koutsandreas, T., Kiritsi, M.N., Vrahatis, A.G., et al. (2021) Peryton: a manual collection of experimentally supported microbe-disease associations. *Nucleic Acids Res.*, **49**, D1328-D1333.
13. Noronha, A., Modamio, J., Jarber, Y., et al. (2019) The Virtual Metabolic Human database: integrating human and gut microbiome metabolism with nutrition and disease. *Nucleic Acids Res.*, **47**, D1265-D1275.
14. Zhang, Q., Yu, K., Li, S., et al. (2021) gutMEGA: a database of the human gut MEtaGenome Atlas. *Brief. Bioinform.*, **22**, bbaa082.
15. Zhai, J., Li, Y., Liu, J., et al. (2026) Global Gut Microbiome Atlas Reveals Epidemiologic-Stage-Specific Signatures in Inflammatory Bowel Disease. Manuscript submitted.
16. DerSimonian, R. and Laird, N. (1986) Meta-analysis in clinical trials. *Control. Clin. Trials*, **7**, 177-188.

---

## FIGURE LEGENDS

**Figure 1.** Overview of the Gut Microbiome Atlas platform. **(A)** Data collection and curation pipeline: 245,627 initial samples were processed through multi-step quality control to yield 168,464 curated samples from 482 BioProjects spanning 66 countries. **(B)** Interactive home page with world map, Sankey diagram, and demographic distribution charts. **(C)** Two-group differential analysis module with volcano plot, LEfSe bar chart, alpha/beta diversity visualization. **(D)** Cross-study meta-analysis with forest plot and heterogeneity assessment. **(E)** GMHI health index scoring with gauge visualization. **(F)** Disease browser with biomarker discovery and lollipop plot. **(G)** Network analysis modules: disease-microbe association, chord diagram, and co-occurrence network. **(H)** Lifecycle atlas showing microbiome composition across 8 age stages.

**Figure 2.** Comparison of analytical capabilities across existing gut microbiome databases. Gut Microbiome Atlas provides the most comprehensive suite of analysis tools, combining differential analysis, meta-analysis, health index, similarity search, network analysis, and lifecycle visualization in a single platform.

---

## SUPPLEMENTARY DATA

Supplementary Table S1. Complete list of 482 BioProjects included in Gut Microbiome Atlas with sample counts, disease coverage, and geographic distribution.

Supplementary Table S2. Disease ontology mapping: 204 disease conditions with MeSH identifiers, ICD-10 codes, and clinical categories.

Supplementary Table S3. API endpoint documentation with request/response schemas.

Supplementary Figure S1. Quality control pipeline flowchart with sample attrition at each step.

Supplementary Figure S2. Geographic distribution of samples across 66 countries.

Supplementary Figure S3. Age group distribution across disease conditions.
