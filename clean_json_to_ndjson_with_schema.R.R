# ----------------------------------------
# Libraries
# ----------------------------------------
library(jsonlite)

# ----------------------------------------
# Configuration
# ----------------------------------------
input_path <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_SELECT_5K.json"
ndjson_output <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_FINAL.ndjson"
schema_output <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_FINAL_schema.json"
keys_to_remove <- c("ts0HSE", "ts0IND", "ts0INV")

# ----------------------------------------
# Helper Functions
# ----------------------------------------

# Normalize field names: lowercase, alphanumeric + underscores
normalize_name <- function(name) {
  name <- gsub("[ -]", "_", name)
  name <- gsub("[^A-Za-z0-9_]", "", name)
  name <- gsub("_+", "_", name)
  tolower(name)
}

# Recursively normalize names
clean_names <- function(obj) {
  if (is.list(obj)) {
    if (!is.null(names(obj))) {
      names(obj) <- vapply(names(obj), normalize_name, character(1))
    }
    for (i in seq_along(obj)) {
      obj[[i]] <- clean_names(obj[[i]])
    }
  }
  return(obj)
}

# Recursively remove unwanted keys
remove_keys <- function(obj, keys) {
  if (is.list(obj)) {
    if (!is.null(names(obj))) {
      obj <- obj[!(names(obj) %in% keys)]
    }
    for (i in seq_along(obj)) {
      obj[[i]] <- remove_keys(obj[[i]], keys)
    }
  }
  return(obj)
}

# Recursively replace NA with "0"
replace_na <- function(obj) {
  if (is.list(obj)) {
    for (i in seq_along(obj)) {
      obj[[i]] <- replace_na(obj[[i]])
    }
  } else if (is.atomic(obj)) {
    obj[is.na(obj)] <- "0"
  }
  return(obj)
}

# Recursively infer BigQuery schema
infer_schema <- function(obj, name = NULL) {
  if (is.list(obj) && !is.null(names(obj))) {
    # Named list = RECORD
    fields <- list()
    for (key in names(obj)) {
      field <- infer_schema(obj[[key]], name = key)
      if (!is.null(field)) {
        fields <- append(fields, list(field))
      }
    }
    return(list(name = name, type = "RECORD", mode = "NULLABLE", fields = fields))
  } else if (is.list(obj) && is.null(names(obj))) {
    # Repeated element (array)
    if (length(obj) == 0) return(NULL)  # Can't infer empty array
    field <- infer_schema(obj[[1]], name)
    if (!is.null(field)) field$mode <- "REPEATED"
    return(field)
  } else if (is.atomic(obj)) {
    field_type <- if (is.numeric(obj)) {
      "FLOAT"
    } else if (is.character(obj)) {
      "STRING"
    } else if (is.logical(obj)) {
      "BOOLEAN"
    } else {
      return(NULL)
    }
    return(list(name = name, type = field_type, mode = "NULLABLE"))
  }
  return(NULL)
}

# ----------------------------------------
# Load Data
# ----------------------------------------

cat("📥 Reading JSON file...\n")
data <- fromJSON(input_path, simplifyVector = FALSE)

# ----------------------------------------
# Clean Data
# ----------------------------------------

cat("🧹 Cleaning records...\n")
cleaned_data <- vector("list", length(data))

for (i in seq_along(data)) {
  entry <- data[[i]]
  if (!is.null(entry$ts_data)) {
    entry$ts_data <- remove_keys(entry$ts_data, keys_to_remove)
  }
  entry <- clean_names(entry)
  entry <- replace_na(entry)
  cleaned_data[[i]] <- entry
}

# ----------------------------------------
# Write NDJSON
# ----------------------------------------

cat("📄 Writing NDJSON file...\n")
con <- file(ndjson_output, open = "w")
for (entry in cleaned_data) {
  line <- toJSON(entry, auto_unbox = TRUE)
  writeLines(line, con)
}
close(con)
cat("✅ NDJSON written to:", ndjson_output, "\n")

# ----------------------------------------
# Generate Schema
# ----------------------------------------

cat("📐 Generating BigQuery schema...\n")
schema_raw <- infer_schema(cleaned_data[[1]])
schema <- schema_raw$fields  # Drop wrapper

write_json(schema, schema_output, pretty = TRUE, auto_unbox = TRUE)
cat("✅ Schema written to:", schema_output, "\n")