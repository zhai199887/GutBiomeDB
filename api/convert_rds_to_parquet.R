# convert_rds_to_parquet.R
# Convert unfiltered.rds abundance matrix to Parquet format for Python API
# 将丰度矩阵从RDS格式转换为Parquet格式供Python API使用

# Install arrow if not available
# 如果没有arrow包则安装
if (!requireNamespace("arrow", quietly = TRUE)) {
  install.packages("arrow", repos = "https://cloud.r-project.org")
}

library(arrow)

cat("Loading unfiltered.rds...\n")
# Load abundance data frame (rows = samples, cols = taxa)
# 加载丰度数据框（行=样本，列=物种）
abund <- readRDS("D:/R代码/unfiltered.rds")

cat(sprintf("Dimensions: %d samples x %d taxa\n", nrow(abund), ncol(abund)))

# Add sample_id column from rownames
# 从行名添加sample_id列
abund$sample_id <- rownames(abund)

# Move sample_id to first column
# 将sample_id移到第一列
abund <- abund[, c("sample_id", setdiff(names(abund), "sample_id"))]

output_path <- "D:/R代码/unfiltered_abundance.parquet"
cat(sprintf("Writing to %s...\n", output_path))

# Write as parquet
# 写入parquet格式
write_parquet(abund, output_path)

cat("Done! File size: ")
cat(file.size(output_path) / 1024 / 1024, "MB\n")
