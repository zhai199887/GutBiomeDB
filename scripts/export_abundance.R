# Export abundance summary from unfiltered.rds
# Input:  D:\R代码\unfiltered.rds
# Output: public/data/abundance_summary.json

library(jsonlite)

# ── Paths ────────────────────────────────────────────────────────────────────
rds_files <- list.files("D:/", pattern = "unfiltered\\.rds", recursive = TRUE, full.names = TRUE)
if (length(rds_files) == 0) stop("Cannot find unfiltered.rds under D:\\")
RDS_PATH <- rds_files[1]
cat("RDS path:", RDS_PATH, "\n")

META_PATH <- list.files("D:/", pattern = "result_with_age_sex_with_age_group_meta\\.csv",
                        recursive = TRUE, full.names = TRUE)[1]
cat("CSV path:", META_PATH, "\n")

SCRIPT_DIR <- "E:/microbiomap_clone/compendium_website/scripts"
OUT_PATH <- file.path(SCRIPT_DIR, "..", "public", "data", "abundance_summary.json")
OUT_PATH <- normalizePath(OUT_PATH, mustWork = FALSE)
cat("Output:", OUT_PATH, "\n\n")

# ── Load data ─────────────────────────────────────────────────────────────────
cat("Loading RDS (168k x 4680, may take a moment)...\n")
abund <- readRDS(RDS_PATH)
cat("  Loaded:", nrow(abund), "samples x", ncol(abund), "taxa\n")

cat("Loading metadata CSV...\n")
meta <- read.csv(META_PATH, fileEncoding = "latin1", stringsAsFactors = FALSE)
cat("  Loaded:", nrow(meta), "rows\n")

# ── Align sample IDs ──────────────────────────────────────────────────────────
# RDS rownames format: "PRJDB10485_DRR243823"  → extract SRR part after last "_"
rds_srr <- sub("^[^_]+_", "", rownames(abund))
rownames(abund) <- rds_srr

# Keep intersection
common_ids <- intersect(rds_srr, meta$srr)
cat("  Common samples:", length(common_ids), "\n")

abund_common <- abund[common_ids, , drop = FALSE]
meta_common  <- meta[match(common_ids, meta$srr), ]

# ── Convert to relative abundance ────────────────────────────────────────────
cat("Converting to relative abundance...\n")
row_sums <- rowSums(abund_common)
row_sums[row_sums == 0] <- 1  # avoid /0
rel_abund <- sweep(abund_common, 1, row_sums, "/")

# ── Aggregate to Genus level ─────────────────────────────────────────────────
# Column name format: "Bacteria.Phylum.Class.Order.Family.Genus"
# Extract genus = last element after splitting by "."
cat("Extracting genus names...\n")
all_genera <- sapply(colnames(rel_abund), function(x) {
  parts <- strsplit(x, "\\.")[[1]]
  tail(parts, 1)
})

# Sum columns with same genus
cat("Aggregating to genus level...\n")
unique_genera <- unique(all_genera)
genus_mat <- matrix(0, nrow = nrow(rel_abund), ncol = length(unique_genera),
                    dimnames = list(rownames(rel_abund), unique_genera))
for (g in unique_genera) {
  cols <- which(all_genera == g)
  if (length(cols) == 1) {
    genus_mat[, g] <- rel_abund[, cols]
  } else {
    genus_mat[, g] <- rowSums(rel_abund[, cols, drop = FALSE])
  }
}
cat("  Genus matrix:", nrow(genus_mat), "samples x", ncol(genus_mat), "genera\n")

# ── Pick Top 30 genera by overall mean abundance ─────────────────────────────
# Filter out artifact/ambiguous genus names
invalid_genera <- c("NA", "group", "Sedis", "incertae", "bacterium",
                    "unclassified", "uncultured", "metagenome")
numeric_pattern <- "^[0-9]"
overall_mean <- colMeans(genus_mat)
overall_mean <- overall_mean[
  !names(overall_mean) %in% invalid_genera &
  !grepl(numeric_pattern, names(overall_mean)) &
  !is.na(names(overall_mean))
]
top30_genera <- names(sort(overall_mean, decreasing = TRUE))[seq_len(min(30, length(overall_mean)))]
cat("Top 30 genera selected (after filtering artifacts).\n")
genus_top <- genus_mat[, top30_genera, drop = FALSE]

# ── Helper: calc group means ──────────────────────────────────────────────────
group_mean_list <- function(grouping_col, top_n = NULL) {
  groups <- sort(unique(meta_common[[grouping_col]]))
  result <- list()
  for (g in groups) {
    idx <- which(meta_common[[grouping_col]] == g)
    if (length(idx) == 0) next
    means <- colMeans(genus_top[idx, , drop = FALSE])
    result[[g]] <- as.list(round(means, 6))
  }
  result
}

# ── Compute group means ───────────────────────────────────────────────────────
cat("Computing group means...\n")

# Clean meta columns to match process_metadata.py logic
meta_common$disease_clean <- ifelse(
  is.na(meta_common$inform.all) | trimws(meta_common$inform.all) == "",
  "unknown",
  trimws(meta_common$inform.all)
)

meta_common$sex_clean <- tolower(trimws(meta_common$sex))
meta_common$sex_clean[!meta_common$sex_clean %in% c("male", "female")] <- "unknown"

age_means     <- group_mean_list("age_group")
sex_means     <- group_mean_list("sex_clean")

# Disease: top 20 diseases + NC + unknown
disease_counts <- sort(table(meta_common$disease_clean), decreasing = TRUE)
top_diseases <- names(disease_counts)[1:min(22, length(disease_counts))]
meta_common$disease_top <- ifelse(
  meta_common$disease_clean %in% top_diseases,
  meta_common$disease_clean,
  "other"
)
disease_means <- group_mean_list("disease_top")

# ── Assemble output ───────────────────────────────────────────────────────────
output <- list(
  genera = top30_genera,
  by_age_group  = age_means,
  by_sex        = sex_means,
  by_disease    = disease_means
)

cat("Writing JSON...\n")
json_str <- toJSON(output, auto_unbox = TRUE, pretty = FALSE)
writeLines(json_str, OUT_PATH, useBytes = FALSE)
size_kb <- file.info(OUT_PATH)$size / 1024
cat("  Written:", round(size_kb, 1), "KB\n")
cat("\nDone:", OUT_PATH, "\n")
