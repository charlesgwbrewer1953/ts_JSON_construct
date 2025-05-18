# Load the necessary library
library(bigrquery)

# Authenticate using your service account key
bq_auth(path = "/Users/charlesbrewer/Library/CloudStorage/OneDrive-Personal/Development/Political_Analysis_Commercial/Security/politicalmaps-e50685d66c12.json")

# Define the project ID
project_id <- "politicalmaps"  # inferred from your service account

# Define the query
query <- "
  SELECT OA21CD, utla22cd, ts_data
  FROM `politicalmaps.ts_JSONL.OA_level_complete`
  WHERE utla22cd = 'W06000021'
"

# Run the query and download the results
results <- bq_project_query(project_id, query)
raw_data <- bq_table_download(results)

# Recursive function to convert nested list/data.frame structure
to_recursive_list <- function(x) {
  if (is.data.frame(x)) {
    if (nrow(x) == 1) {
      lapply(x[1, , drop = FALSE], to_recursive_list)
    } else {
      apply(x, 1, function(row) {
        lapply(row, to_recursive_list)
      }, simplify = FALSE)
    }
  } else if (is.list(x)) {
    lapply(x, to_recursive_list)
  } else {
    x
  }
}

# Apply recursive conversion to each row
nested_oa_list <- apply(raw_data, 1, function(row) {
  list(
    OA21CD = row[["OA21CD"]],
    utla22cd = row[["utla22cd"]],
    ts_data = to_recursive_list(row[["ts_data"]])
  )
})

# Optional: name the list by OA code
names(nested_oa_list) <- raw_data$OA21CD

# Inspect a sample entry
str(nested_oa_list[[1]])