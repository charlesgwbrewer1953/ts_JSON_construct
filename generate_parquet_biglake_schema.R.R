# ------------------------------------------------------------------------------
# Script: generate_parquet_biglake_schema.R
#
# Description:
# This R script processes a large nested NDJSON file of UK geography-level data,
# flattens it, cleans column names, converts all numeric fields to a consistent
# FLOAT type, and writes multiple Parquet files sorted by selected geographic codes.
# 
# Additionally, it generates a BigQuery/BigLake-compatible JSON schema by inferring
# each column's type, and provides tooling to inspect Parquet structure.
#
# Output:
# - Unsorted and sorted Parquet files
# - BigLake-compatible schema JSON
# - Printed summary of column types for verification
#
# Dependencies: arrow, jsonlite, stringr
# ------------------------------------------------------------------------------



library(jsonlite)
library(arrow)
library(stringr)

# File paths
input_json <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_FULL_15_5_25.ndjson"
output_dir <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/parquet_file"
base_name <- "OA_level_FULL_15_5_25_cleaned"

# Fields to sort by
sort_fields <- c("pcon25cd", "wd24cd", "lad24cd", "sener24cd", "utla22cd")

# Read and flatten JSON
cat("📥 Reading and flattening NDJSON...\n")
df <- stream_in(file(input_json), flatten = TRUE, verbose = FALSE)

# Clean column names
clean_names <- function(names_vec) {
  names_vec |>
    tolower() |>
    str_replace_all("[^a-z0-9_]+", "_") |>
    str_replace_all("_+", "_") |>
    str_replace_all("^_|_$", "")  # Remove leading/trailing underscores
}
names(df) <- clean_names(names(df))

# Ensure output directory exists
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Write original flattened version
cat("💾 Writing unsorted original Parquet file...\n")
write_parquet(df, file.path(output_dir, paste0(base_name, ".parquet")))

# Write one sorted file per geography field
for (field in sort_fields) {
  if (!field %in% names(df)) {
    cat("⚠️  Field not found, skipping:", field, "\n")
    next
  }
  
  cat("📤 Writing sorted Parquet file for:", field, "...\n")
  
  # Convert all integer columns to numeric
  df[] <- lapply(df, function(x) {
    if (inherits(x, "integer")) as.numeric(x) else x
  })
  
  sorted_df <- df[order(df[[field]]), ]
  output_file <- file.path(output_dir, paste0(base_name, "_sorted_by_", field, ".parquet"))
  write_parquet(sorted_df, output_file)
}

cat("✅ Done. All Parquet files written to:\n", output_dir, "\n")




######################
#
#. Generate schema
#

library(arrow)
library(jsonlite)

# Load the Parquet file
df <- read_parquet("/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/parquet_file/OA_level_FULL_15_5_25_cleaned_sorted_by_utla22cd.parquet")

# Map R types to BigQuery types
map_type <- function(r_type) {
  if (r_type %in% c("integer", "integer64")) return("INTEGER")
  if (r_type == "numeric") return("FLOAT")
  if (r_type == "logical") return("BOOLEAN")
  if (r_type == "character") return("STRING")
  if (grepl("POSIX", r_type)) return("TIMESTAMP")
  return("STRING")
}

schema <- lapply(names(df), function(name) {
  list(name = name,
       type = map_type(class(df[[name]])[1]),
       mode = "NULLABLE")
})

write(toJSON(schema, pretty = TRUE, auto_unbox = TRUE), "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/parquet_file/oa_biglake_schema.json")





############################
#
#
#. Examine parquet fles
#


# Replace with your local file path
df <- read_parquet("/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/parquet_file/OA_level_FULL_15_5_25_cleaned_sorted_by_utla22cd.parquet")

# Show column names and R types
schema <- sapply(df, class)
schema_df <- data.frame(column = names(schema), type = unname(schema))

print(schema_df)
