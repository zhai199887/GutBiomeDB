#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(lme4)
  library(lmerTest)
  library(emmeans)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 4) {
  stop("Usage: lmm_runner.R abundance.tsv.gz metadata.tsv results.tsv summary.tsv")
}

abundance_file <- args[[1]]
metadata_file <- args[[2]]
results_file <- args[[3]]
summary_file <- args[[4]]

ab <- fread(abundance_file, check.names = FALSE)
meta <- fread(metadata_file, check.names = FALSE)

required_ab <- c("sample_key")
required_meta <- c("sample_key", "disease", "project", "amplicon", "length", "instrument")
if (!all(required_ab %in% names(ab))) stop("abundance input is missing sample_key")
if (!all(required_meta %in% names(meta))) {
  stop("metadata input is missing one or more required columns")
}

taxa <- setdiff(names(ab), "sample_key")
if (length(taxa) < 1L) stop("No taxon columns were supplied")

ab[, sample_key := as.character(sample_key)]
meta[, sample_key := as.character(sample_key)]
if (anyDuplicated(ab$sample_key)) stop("Duplicate sample_key values in abundance input")
if (anyDuplicated(meta$sample_key)) stop("Duplicate sample_key values in metadata input")

meta[, disease := factor(as.character(disease), levels = c("B", "A"))]
meta[, project := factor(ifelse(is.na(project) | project == "", "Unknown", as.character(project)))]
meta[, amplicon := factor(ifelse(is.na(amplicon) | amplicon == "", "Unknown", as.character(amplicon)))]
meta[, length := factor(ifelse(is.na(length) | length == "", "Unknown", as.character(length)))]
meta[, instrument := factor(ifelse(is.na(instrument) | instrument == "", "Unknown", as.character(instrument)))]

if (anyNA(meta$disease)) stop("Disease group must contain both B and A levels")

ab_mat <- as.matrix(ab[, ..taxa])
storage.mode(ab_mat) <- "double"
if (any(!is.finite(ab_mat))) stop("Non-finite abundance values detected")

row_totals <- rowSums(ab_mat, na.rm = TRUE)
row_totals[row_totals <= 0] <- 1
rel <- ab_mat / row_totals * 100

mean_abundance <- colMeans(rel, na.rm = TRUE)
prevalence <- colMeans(rel > 0, na.rm = TRUE)
taxon_keep <- which(mean_abundance >= 0.01 & prevalence >= 0.05)
if (length(taxon_keep) < 1L) stop("No taxa passed mean-abundance/prevalence filtering")

selected_reads <- rowSums(ab_mat[, taxon_keep, drop = FALSE], na.rm = TRUE)
sample_keep <- which(selected_reads >= 1000)
if (length(sample_keep) < 20L) stop("Too few samples remained after read-depth filtering")

rel <- rel[sample_keep, taxon_keep, drop = FALSE]
taxa <- taxa[taxon_keep]
sample_keys <- ab$sample_key[sample_keep]
meta <- meta[match(sample_keys, meta$sample_key)]
if (anyNA(meta$sample_key)) stop("Abundance and metadata sample keys do not align")
if (length(unique(meta$disease)) != 2L) stop("Both disease groups A and B are required")

# Match the reference pipeline: log(x + 1), row CLR centering, then
# taxon-wise standardization before fitting each LMM.
log_mat <- log(rel + 1)
clr_mat <- log_mat - rowMeans(log_mat, na.rm = TRUE)
scaled_mat <- apply(clr_mat, 2, function(values) {
  s <- sd(values, na.rm = TRUE)
  if (is.na(s) || s == 0) rep(0, length(values))
  else (values - mean(values, na.rm = TRUE)) / s
})
colnames(scaled_mat) <- taxa

formula_text <- "y ~ disease + (1 | project) + (1 | amplicon) + (1 | length) + (1 | instrument)"
result_rows <- vector("list", length(taxa))
failed <- 0L
singular <- 0L

for (i in seq_along(taxa)) {
  dat <- meta
  dat$y <- as.numeric(scaled_mat[, i])
  fit <- tryCatch(
    suppressWarnings(
      lmer(
        y ~ disease + (1 | project) + (1 | amplicon) +
          (1 | length) + (1 | instrument),
        data = dat,
        REML = TRUE,
        control = lmerControl(
          optimizer = "bobyqa",
          optCtrl = list(maxfun = 100000),
          check.rankX = "silent.drop.cols"
        )
      )
    ),
    error = function(e) NULL
  )
  if (is.null(fit)) {
    failed <- failed + 1L
    next
  }

  is_singular <- isSingular(fit, tol = 1e-04)
  if (is_singular) singular <- singular + 1L

  contrast <- tryCatch({
    # Large cohorts exceed emmeans' default 3,000-observation df limits.
    # Asymptotic inference avoids the expensive small-sample df correction
    # while retaining the same disease contrast and fixed/random effects.
    means <- emmeans(fit, ~ disease, lmer.df = "asymptotic")
    as.data.frame(summary(contrast(means, method = list("A-B" = c(-1, 1)), adjust = "none")))
  }, error = function(e) NULL)
  if (is.null(contrast) || nrow(contrast) < 1L || !"estimate" %in% names(contrast)) {
    failed <- failed + 1L
    next
  }

  ratio_column <- if ("t.ratio" %in% names(contrast)) "t.ratio" else "z.ratio"
  required_contrast_columns <- c("estimate", "SE", "df", ratio_column, "p.value")
  if (!all(required_contrast_columns %in% names(contrast))) {
    failed <- failed + 1L
    next
  }
  estimate_value <- suppressWarnings(as.numeric(contrast$estimate[[1]]))
  se_value <- suppressWarnings(as.numeric(contrast$SE[[1]]))
  df_value <- suppressWarnings(as.numeric(contrast$df[[1]]))
  ratio_value <- suppressWarnings(as.numeric(contrast[[ratio_column]][[1]]))
  p_value <- suppressWarnings(as.numeric(contrast$p.value[[1]]))
  if (length(estimate_value) != 1L || length(se_value) != 1L ||
      length(df_value) != 1L || length(ratio_value) != 1L ||
      length(p_value) != 1L || !is.finite(estimate_value) ||
      !is.finite(se_value) || !is.finite(p_value)) {
    failed <- failed + 1L
    next
  }
  # Asymptotic emmeans reports df = Inf.  Keep the field, but encode it as
  # missing so the JSON API remains strict-JSON compliant.
  if (!is.finite(df_value)) df_value <- NA_real_

  result_rows[[i]] <- data.frame(
    taxon = taxa[[i]],
    estimate = estimate_value,
    std_error = se_value,
    df = df_value,
    t_ratio = ratio_value,
    p_value = p_value,
    singular_fit = is_singular,
    stringsAsFactors = FALSE
  )
  rm(fit, contrast, dat)
  gc(verbose = FALSE)
}

results <- rbindlist(result_rows, fill = TRUE)
if (nrow(results) > 0L) {
  results[, adjusted_p := p.adjust(p_value, method = "BH")]
  results[, enriched_in := ifelse(estimate > 0, "A", "B")]
  setorder(results, adjusted_p, p_value)
} else {
  results <- data.table(
    taxon = character(), estimate = numeric(), std_error = numeric(),
    df = numeric(), t_ratio = numeric(), p_value = numeric(),
    singular_fit = logical(), adjusted_p = numeric(), enriched_in = character()
  )
}

fwrite(results, results_file, sep = "\t", na = "NA")
summary_lines <- c(
  paste0("formula\t", formula_text),
  paste0("n_samples\t", nrow(meta)),
  paste0("n_taxa_tested\t", length(taxa)),
  paste0("n_fitted\t", nrow(results)),
  paste0("n_failed\t", failed),
  paste0("n_singular\t", singular),
  paste0("n_significant\t", sum(results$adjusted_p < 0.05, na.rm = TRUE)),
  "filter\tmean_relative_abundance>=0.01%; prevalence>=5%; selected_reads>=1000",
  "transform\tlog(x+1), row CLR centering, taxon-wise z-score"
)
writeLines(summary_lines, summary_file, useBytes = TRUE)
