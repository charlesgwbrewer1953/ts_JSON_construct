#####
#    	•	Move all dbGetQuery() calls outside the future_lapply() block.
#.    •	Pass only simple filtered lists (not DB connections) into workers.
#.    •	Keep each worker’s task pure and fast.
#
#
#
# Required libraries
# Required libraries
library(DBI)
library(RMySQL)
library(jsonlite)

# Helper function to detect OA column
detect_oa_column <- function(df) {
  possible_oa_names <- c("OA21CD", "OA", "OA11CD")
  match <- possible_oa_names[possible_oa_names %in% names(df)]
  if (length(match) == 0) stop("No OA column found in table.")
  return(match[1])
}

# Load geographic metadata
geo_df <- read.csv("/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/OA_all_Basic_Source_noScot_4_25.csv",
                   stringsAsFactors = FALSE)
sample_oa_codes <- geo_df$OA21CD
print("Sectioin 1 complete base files read")
# Static variables for DB connection
mysql_params <- list(
  dbname = "PAC2_central",
  host = "10.0.0.8",
  user = "metis_local",
  password = "metis_pw"
)
print("Section 2 COmplete - con complete")
# Connect and load all ts0 tables
con <- do.call(dbConnect, c(drv = RMySQL::MySQL(), mysql_params))
all_tables <- dbListTables(con)
ts0_tables <- all_tables[grepl("^ts0", all_tables)]
special_table <- "ts10102"
print("Section 2 Complete - con complete")
print(all_tables)
# Preload ts10102
ts10102_df <- dbGetQuery(con, paste0("SELECT * FROM `", special_table, "`"))
oa_col_ts10102 <- detect_oa_column(ts10102_df)
ts10102_df <- ts10102_df[ts10102_df[[oa_col_ts10102]] %in% sample_oa_codes, ]

ts10102_long <- reshape(ts10102_df,
                        varying = setdiff(names(ts10102_df), oa_col_ts10102),
                        direction = "long",
                        timevar = "full_name",
                        times = setdiff(names(ts10102_df), oa_col_ts10102),
                        v.names = "value",
                        idvar = oa_col_ts10102)

ts10102_long$group <- substr(ts10102_long$full_name, 1, 3)
ts10102_long$name <- sub("^[^_]+_", "", ts10102_long$full_name)

precomputed_ts10102 <- split(ts10102_long, ts10102_long[[oa_col_ts10102]])
ts10102_grouped <- lapply(precomputed_ts10102, function(df) {
  split_data <- split(df, df$group)
  lapply(split_data, function(subdf) {
    setNames(subdf$value, subdf$name)
  })
})
print("Section 2a complete ts10102 read")
# Load all other ts0 tables into memory
get_ts_table_data <- function(table_name, con, filter_oas) {
  query <- paste0("SELECT * FROM `", table_name, "`")
  df <- dbGetQuery(con, query)
  oa_col <- detect_oa_column(df)
  df <- df[df[[oa_col]] %in% filter_oas, ]
  result <- setNames(
    lapply(seq_len(nrow(df)), function(i) {
      as.list(df[i, setdiff(names(df), oa_col), drop = FALSE])
    }),
    df[[oa_col]]
  )
  return(result)
}

other_ts0_data <- lapply(ts0_tables[ts0_tables != special_table], function(tbl) {
  get_ts_table_data(tbl, con, sample_oa_codes)
})
names(other_ts0_data) <- ts0_tables[ts0_tables != special_table]
dbDisconnect(con)
print("Section 2b complete ts files read")
# Build JSON structure using single-threaded loop
final_list <- lapply(sample_oa_codes, function(oa) {
  base <- geo_df[geo_df$OA21CD == oa, , drop = FALSE]
  base_list <- as.list(base[1, ])
  ts_data <- list()
  
  if (oa %in% names(ts10102_grouped)) {
    for (group in names(ts10102_grouped[[oa]])) {
      ts_data[[paste0("ts0", group)]] <- ts10102_grouped[[oa]][[group]]
    }
  }
  
  for (group in names(other_ts0_data)) {
    if (!is.null(other_ts0_data[[group]][[oa]])) {
      ts_data[[group]] <- other_ts0_data[[group]][[oa]]
    }
  }
  
  return(c(base_list, list(ts_data = ts_data)))
})
print("Section 3 complete - JSON single threat complete")
# Save JSON output
json_output <- toJSON(unname(final_list), pretty = TRUE, auto_unbox = TRUE)
write(json_output, file = "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_FULL.json")
print("Section 4 - Proccess complete")
