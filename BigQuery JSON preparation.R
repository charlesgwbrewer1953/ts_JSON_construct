library(jsonlite)
library(progress)
library(purrr)
library(furrr)
library(future)

# ---------------------------
# CONFIGURATION
# ---------------------------
input_file <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_FULL.ndjson"
ndjson_output <- "OA_level_FULL_15_5_25.ndjson"
schema_output <- "OA_level_SCHEMA_15_5_25.json"
schema_sample_size <- 2000
chunk_size <- 1000
use_parallel <- TRUE  # Toggle for multi-core support

# ---------------------------
# CLEAN FIELD NAMES
# ---------------------------
clean_names_recursive <- function(x) {
  if (is.list(x)) {
    cleaned_names <- names(x)
    cleaned_names <- gsub("[^A-Za-z0-9_]", "_", cleaned_names)
    cleaned_names <- ifelse(grepl("^[0-9]", cleaned_names),
                            paste0("X_1", cleaned_names),
                            cleaned_names)
    names(x) <- cleaned_names
    x <- setNames(lapply(x, clean_names_recursive), names(x))
  }
  return(x)
}

# ---------------------------
# INFER BQ SCHEMA
# ---------------------------
infer_bq_type <- function(values) {
  classify_value <- function(val) {
    if (isTRUE(is.null(val)) || isTRUE(is.na(val))) return(NULL)
    if (is.list(val)) return("RECORD")
    if (is.logical(val)) return("BOOLEAN")
    if (is.numeric(val)) return("FLOAT")
    if (is.character(val)) return("STRING")
    return("STRING")
  }
  
  if (!is.list(values)) values <- list(values)
  flat_vals <- unlist(values, recursive = FALSE)
  sample_vals <- Filter(Negate(is.null), flat_vals)[1:min(10, length(flat_vals))]
  if (length(sample_vals) == 0) return(list(type = "STRING", mode = "NULLABLE"))
  
  types <- unique(vapply(sample_vals, classify_value, character(1)))
  if ("STRING" %in% types) return(list(type = "STRING", mode = "REPEATED"))
  if ("FLOAT" %in% types) return(list(type = "FLOAT", mode = "REPEATED"))
  if ("BOOLEAN" %in% types) return(list(type = "BOOLEAN", mode = "REPEATED"))
  if ("RECORD" %in% types) return(list(type = "RECORD", mode = "REPEATED"))
  return(list(type = "STRING", mode = "REPEATED"))
}

build_bq_schema <- function(records) {
  message("📐 [SCHEMA] Inferring schema from sampled records...")
  keys <- unique(unlist(lapply(records, names)))
  
  get_field_schema <- function(key) {
    values <- lapply(records, function(rec) rec[[key]])
    values <- Filter(Negate(is.null), values)
    if (length(values) == 0) return(NULL)
    
    type_info <- infer_bq_type(values)
    if (is.null(type_info)) return(NULL)
    
    field <- list(name = key, type = type_info$type, mode = type_info$mode)
    if (type_info$type == "RECORD") {
      subfields <- unique(unlist(lapply(values, function(v) names(v)), use.names = FALSE))
      field$fields <- map(subfields, function(subkey) {
        subvals <- map(values, ~ .x[[subkey]])
        subvals <- Filter(Negate(is.null), subvals)
        if (length(subvals) == 0) return(NULL)
        subtype <- infer_bq_type(subvals)
        list(name = subkey, type = subtype$type, mode = subtype$mode)
      }) %>% compact()
    }
    
    return(field)
  }
  
  map(keys, get_field_schema) %>% compact()
}

# ---------------------------
# PROCESSING
# ---------------------------
message("🔎 [LOAD] Reading NDJSON lines...")
lines <- readLines(input_file)
n_total <- length(lines)
message(sprintf("📄 [INFO] %d lines detected. Chunk size = %d", n_total, chunk_size))

# Sampling for schema
set.seed(42)
message("📐 [SCHEMA] Sampling %d random records...", schema_sample_size)
sampled_indices <- sample(seq_len(n_total), size = schema_sample_size)
sampled_lines <- lines[sampled_indices]

schema_sample <- map(sampled_lines, ~{
  parsed <- tryCatch(fromJSON(.x, simplifyVector = FALSE), error = function(e) NULL)
  if (!is.null(parsed)) clean_names_recursive(parsed) else NULL
}) %>% compact()

# Prepare output
message("📝 [OUTPUT] Opening output file for cleaned NDJSON...")
con_out <- file(ndjson_output, open = "w")
chunks <- split(lines, ceiling(seq_along(lines) / chunk_size))
progress <- progress_bar$new(
  format = "🚀 Processing [:bar] :current/:total ETA: :eta",
  total = length(chunks), clear = FALSE, width = 60
)

# Parallel setup
if (use_parallel) {
  message("🧠 [PARALLEL] Enabling multicore via furrr...")
  plan(multisession, workers = max(1, parallel::detectCores() - 1))
}

# MAIN LOOP
message("🔁 [LOOP] Beginning chunked processing...")
for (chunk in chunks) {
  cleaned_lines <- if (use_parallel) {
    future_map_chr(chunk, function(line) {
      parsed <- tryCatch(fromJSON(line, simplifyVector = FALSE), error = function(e) NULL)
      if (is.null(parsed)) return(NA_character_)
      cleaned <- clean_names_recursive(parsed)
      as.character(toJSON(cleaned, auto_unbox = TRUE, na = "null"))
    }, .progress = FALSE) %>% na.omit()
  } else {
    # Uncomment if you want non-parallel fallback:
    # map_chr(chunk, function(line) {
    #   parsed <- tryCatch(fromJSON(line, simplifyVector = FALSE), error = function(e) NULL)
    #   if (is.null(parsed)) return(NA_character_)
    #   cleaned <- clean_names_recursive(parsed)
    #   as.character(toJSON(cleaned, auto_unbox = TRUE, na = "null"))
    # }) %>% na.omit()
  }
  
  writeLines(cleaned_lines, con = con_out)
  progress$tick()
}

close(con_out)
message("✅ [DONE] Cleaned NDJSON written to: ", ndjson_output)

# ---------------------------
# WRITE SCHEMA
# ---------------------------
schema <- build_bq_schema(schema_sample)
write(toJSON(schema, pretty = TRUE, auto_unbox = TRUE), file = schema_output)
message("✅ [DONE] BigQuery schema saved to: ", schema_output)