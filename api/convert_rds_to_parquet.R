# convert_rds_to_parquet.R
# Convert the Abdill et al. (2025) abundance matrix from RDS to Parquet
# format for the Python API.
# Usage: Rscript convert_rds_to_parquet.R <input.rds> <output.parquet>

# Install arrow package if not available
if (!requireNamespace("arrow", quietly = TRUE)) {
  install.packages("arrow", repos = "https://cloud.r-project.org")
}

library(arrow)

args <- commandArgs(trailingOnly = TRUE)
input_path  <- if (length(args) >= 1) args[1] else stop("Usage: Rscript convert_rds_to_parquet.R <input.rds> <output.parquet>")
output_path <- if (length(args) >= 2) args[2] else sub("\\.rds$", ".parquet", input_path)

cat("Loading", input_path, "...\n")
# Load abundance data frame (rows = samples, columns = taxa)
abund <- readRDS(input_path)

cat(sprintf("Dimensions: %d samples x %d taxa\n", nrow(abund), ncol(abund)))

# Add sample_id column from row names
abund$sample_id <- rownames(abund)

# Move sample_id to the first column
abund <- abund[, c("sample_id", setdiff(names(abund), "sample_id"))]

cat(sprintf("Writing to %s...\n", output_path))

# Write to Parquet format
write_parquet(abund, output_path)

cat("Done! File size: ")
cat(file.size(output_path) / 1024 / 1024, "MB\n")
