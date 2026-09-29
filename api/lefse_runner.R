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

dataset <- microtable$new(
  sample_table = sample_table,
  otu_table = otu,
  tax_table = tax_table
)
dataset <- tidy_taxonomy(dataset)

result <- trans_diff$new(
  dataset = dataset,
  method = "lefse",
  group = "group",
  taxa_level = taxa_level,
  p_adjust_method = p_adjust_method,
  alpha = alpha,
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
  paste0("n_kw_passed\t", nrow(result$res_diff)),
  paste0("n_output_rows\t", nrow(result$res_diff)),
  paste0("hierarchical_wilcoxon\t", "not_run_without_lefse_subgroup")
)
writeLines(summary_lines, summary_path, useBytes = TRUE)
