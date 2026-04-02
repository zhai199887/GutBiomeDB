# convert_rds_to_csv.R
# Convert unfiltered.rds to CSV for Python API use
# 将丰度矩阵RDS转换为CSV格式
# Note: large file ~1GB, but no extra R packages needed
# 注意：文件较大约1GB，但不需要额外R包

cat("Loading unfiltered.rds...\n")
abund <- readRDS("D:/R代码/unfiltered.rds")
cat(sprintf("Dimensions: %d x %d\n", nrow(abund), ncol(abund)))

# Add sample_id from rownames
# 从行名提取sample_id
sample_ids <- rownames(abund)

output_path <- "D:/R代码/unfiltered_abundance.csv"
cat(sprintf("Writing to %s (this may take a few minutes)...\n", output_path))

# Write CSV with sample_id as first column
# 写CSV，sample_id作为第一列
write.csv(abund, file = output_path, row.names = TRUE)

cat("Done! File size: ")
cat(round(file.size(output_path) / 1024 / 1024, 1), "MB\n")
