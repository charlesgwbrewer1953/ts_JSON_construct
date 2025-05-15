

library(jsonlite)

# File path
file_path <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_BIGQUERY.jsonl"




# Use wc -l and parse properly
line_info <- system(paste("wc -l", shQuote(file_path)), intern = TRUE)
# Extract the numeric line count from the output (e.g., "123456 yourfile.json")
total_lines <- as.integer(strsplit(trimws(line_info), "\\s+")[[1]][1])

# Open file for reading
con <- file(file_path, "r")

# Set up progress bar
pb <- txtProgressBar(min = 0, max = total_lines, style = 3)
line_number <- 0

repeat {
  line <- readLines(con, n = 1, warn = FALSE)
  if (length(line) == 0) break
  line_number <- line_number + 1
  
  # Try parsing the JSON
  tryCatch({
    fromJSON(line)
  }, error = function(e) {
    cat(sprintf("\n❌ Invalid JSON on line %d:\n%s\n", line_number, e$message))
    break
  })
  
  # Update progress
  setTxtProgressBar(pb, line_number)
  
  # Print every 10,000 lines
  if (line_number %% 10000 == 0) {
    cat(sprintf("\nRead %d lines...\n", line_number))
  }
}

close(pb)
close(con)
cat("\n✅ Finished validation.\n")




lines <- readLines(file_path, n = 500)
writeLines(lines, "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_filesample.json")


###############
# Remoove NA values

library(jsonlite)

# File paths
input_file <- file_path
output_file <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_BIGQUERY.jsonl_cleaned.json"

# First, count total lines for progress bar
message("Counting total lines...")
line_count_cmd <- system(paste("wc -l", shQuote(input_file)), intern = TRUE)
total_lines <- as.integer(strsplit(trimws(line_count_cmd), "\\s+")[[1]][1])

# Open input and output file connections
in_con <- file(input_file, "r")
out_con <- file(output_file, "w")

# Progress bar setup
pb <- txtProgressBar(min = 0, max = total_lines, style = 3)

line_number <- 0
na_count <- 0

# Recursive function to replace NAs and count them
clean_json <- function(x) {
  if (is.list(x)) {
    lapply(x, clean_json)
  } else if (is.atomic(x)) {
    n_na <- sum(is.na(x))
    if (n_na > 0) {
      assign("na_count", na_count + n_na, envir = .GlobalEnv)
    }
    ifelse(is.na(x), 0, x)
  } else {
    x
  }
}

repeat {
  line <- readLines(in_con, n = 1, warn = FALSE)
  if (length(line) == 0) break
  line_number <- line_number + 1
  
  # Try parsing JSON
  parsed <- tryCatch(fromJSON(line), error = function(e) NULL)
  
  if (!is.null(parsed)) {
    cleaned <- clean_json(parsed)
    writeLines(toJSON(cleaned, auto_unbox = TRUE), out_con)
  } else {
    cat(sprintf("\n❌ Skipped invalid JSON on line %d\n", line_number))
  }
  
  # Print every 10,000 lines
  if (line_number %% 10000 == 0) {
    cat(sprintf("\nProcessed %d lines...\n", line_number))
  }
  
  setTxtProgressBar(pb, line_number)
}

close(pb)
close(in_con)
close(out_con)

# Final summary
cat(sprintf("\n✅ Finished processing.\nTotal lines: %d\nTotal NAs replaced: %d\n", line_number, na_count))
