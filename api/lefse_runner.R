# GutBiomeDB LEfSe runner
#
# This runner intentionally delegates the statistical implementation to the
# same microeco::trans_diff(method = "lefse") implementation used by the
# Figure 3 analysis. Python prepares a taxa x samples abundance table and a
# sample-level group table, while this file performs within-sample
# normalization, Kruskal-Wallis screening, LDA bootstrap scoring, and optional
# p-value adjustment.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 11) {
  stop("Expected abundance, metadata, taxonomy, output, summary, taxa level, alpha, p-adjust, boots, nresam, seed")
}

abundance_path <- args[[1]]
metadata_path <- args[[2]]
taxonomy_path <- args[[3]]
output_path <- args[[4]]
summary_path <- args[[5]]
taxa_level <- args[[6]]
alpha <- as.numeric(args[[7]])
p_adjust_method <- args[[8]]
boots <- as.integer(args[[9]])
nresam <- as.numeric(args[[10]])
seed <- args[[11]]

if (nzchar(seed)) set.seed(as.integer(seed))

suppressPackageStartupMessages(library(microeco))

otu <- read.delim(abundance_path, row.names = 1, check.names = FALSE,
                  stringsAsFactors = FALSE)
sample_table <- read.delim(metadata_path, row.names = 1, check.names = FALSE,
                           stringsAsFactors = FALSE)
tax_table <- read.delim(taxonomy_path, row.names = 1, check.names = FALSE,
                        stringsAsFactors = FALSE)

sample_table$group <- as.character(sample_table$group)
sample_table$group <- factor(sample_table$group,
                             levels = unique(sample_table$group))

# Apply the official screening order explicitly before the LDA stage. The
# platform keeps the top 20 eligible features for the plot/result payload;
# this is a deliberate report-size limit, while the KW gate remains first.
unknown_pattern <- "__$|uncultured$|Incertae..edis$|_sp$"
keep_features <- !grepl(unknown_pattern, rownames(otu), ignore.case = TRUE)
otu <- otu[keep_features, , drop = FALSE]
tax_table <- tax_table[rownames(otu), , drop = FALSE]
sample_totals <- colSums(otu, na.rm = TRUE)
sample_totals[sample_totals <= 0] <- 1
kw_abund <- sweep(as.matrix(otu), 2, sample_totals, "/") * 1e6
kw_abund[!is.finite(kw_abund)] <- 0
kw_p_raw <- vapply(seq_len(nrow(kw_abund)), function(i) {
  suppressWarnings(kruskal.test(kw_abund[i, ], sample_table$group)$p.value)
}, numeric(1))
kw_p_adj <- p.adjust(kw_p_raw, method = p_adjust_method)
eligible <- which(!is.na(kw_p_adj) & kw_p_adj < alpha)
eligible <- eligible[order(kw_p_adj[eligible], kw_p_raw[eligible],
                           rownames(otu)[eligible])]
top_idx <- head(eligible, 20L)
if (length(top_idx) == 0L) {
  stop("No features passed the Kruskal-Wallis p_adjusted < alpha filter")
}
otu_top <- otu[top_idx, , drop = FALSE]
tax_top <- tax_table[rownames(otu_top), , drop = FALSE]

dataset <- microtable$new(
  sample_table = sample_table,
  otu_table = otu_top,
  tax_table = tax_top
)
dataset <- tidy_taxonomy(dataset)

result <- trans_diff$new(
  dataset = dataset,
  method = "lefse",
  group = "group",
  taxa_level = taxa_level,
  # Screening was performed above; alpha=1 prevents a second different gate
  # from discarding the selected top-20 features before bootstrap LDA.
  p_adjust_method = "none",
  alpha = 1,
  lefse_norm = 1e6,
  nresam = nresam,
  boots = boots,
  remove_unknown = TRUE
)

write.csv(result$res_diff, output_path, row.names = FALSE, na = "")

summary_lines <- c(
  paste0("method\t", "microeco::trans_diff(method=lefse)"),
  paste0("taxa_level\t", taxa_level),
  paste0("alpha\t", format(alpha, scientific = TRUE)),
  paste0("p_adjust_method\t", p_adjust_method),
  paste0("kw_filter\t", "p_adjusted < alpha"),
  paste0("lefse_norm\t", "1e6"),
  paste0("boots\t", boots),
  paste0("nresam\t", format(nresam, digits = 12)),
  paste0("n_input_features\t", nrow(otu)),
  paste0("n_kw_passed\t", length(eligible)),
  paste0("n_top20\t", nrow(otu_top)),
  paste0("n_output_rows\t", nrow(result$res_diff)),
  paste0("hierarchical_wilcoxon\t", "not_run_without_lefse_subgroup")
)
writeLines(summary_lines, summary_path, useBytes = TRUE)
