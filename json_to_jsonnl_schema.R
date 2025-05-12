#########
#
#.  Reads json file and creates jsonnl and schema
#
#




library(jsonlite)

# Path to your input JSON file
input_path <- "/Volumes/dK_T7/demographiKonT7/NSPL_4_25_use/JSON_file/OA_level_data_FULL.json"

# Read the file into R as a list
json_data <- fromJSON(input_path, simplifyVector = FALSE)

# Optional: preview the structure
str(json_data, max.level = 2)