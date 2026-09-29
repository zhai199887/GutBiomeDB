# convert_rds_to_csv.R
# Convert the Abdill et al. (2025) abundance matrix from RDS to CSV format.
# Usage: Rscript convert_rds_to_csv.R <input.rds> <output.csv>
# Note: output file is ~1 GB; no additional R packages required.

args <- commandArgs(trailingOnly = TRUE)
input_path  <- if (length(args) >= 1) args[1] else stop("Usage: Rscript convert_rds_to_csv.R <input.rds> <output.csv>")
output_path <- if (length(args) >= 2) args[2] else sub("\\.rds$", ".csv", input_path)

cat("Loading", input_path, "...\n")
abund <- readRDS(input_path)
cat(sprintf("Dimensions: %d x %d\n", nrow(abund), ncol(abund)))

# Extract sample_id from row names
sample_ids <- rownames(abund)

cat(sprintf("Writing to %s (this may take a few minutes)...\n", output_path))

# Write CSV with sample_id as the first column
write.csv(abund, file = output_path, row.names = TRUE)

cat("Done! File size: ")
cat(round(file.size(output_path) / 1024 / 1024, 1), "MB\n")
