# 1. SETUP
source("scripts/pipeline_engine.R")
library(duckdb)
library(purrr)
library(dplyr)
library(readr)
library(writexl)

# Get external path from .Renviron
base_data_path <- get_base_path()
db_path <- file.path(base_data_path, "bioeconomy_latam.duckdb")

# 2. CRAWL REPO FOR MANIFESTS
# We look inside the versioned 'manifests/' folder in your repository
manifest_files <- list.files(
  path = "manifests",
  pattern = "manifest_.*\\.csv$",
  full.names = TRUE
)

if (length(manifest_files) == 0) {
  stop("No manifests found in the manifests/ folder!")
}

# 3. OPEN DATABASE CONNECTION
con <- dbConnect(duckdb(), db_path)

# 4. GLOBAL PROCESSING LOOP
for (m_path in manifest_files) {
  # Load the manifest file
  manifest <- read_csv(
    m_path,
    show_col_types = FALSE,
    locale = locale(encoding = "UTF-8")
  )

  # Identify country and version from the manifest
  current_iso <- toupper(manifest$iso3[1])
  current_ver <- manifest$lookup_version[1]

  message(paste(">>> Weaving Node:", current_iso, "Version:", current_ver))

  # A. EXTRACTION: Process all quadrants in the manifest
  # Uses the ENV variable via pipeline_engine to find raw Excels
  facts <- manifest %>%
    split(1:nrow(.)) %>%
    map_df(~ extract_sut_quadrant(.x))

  # B. LOAD LOOKUPS: Load the Excel sheets for this version
  rows_lookup <- load_dimension_table(current_iso, current_ver, type = "rows")
  cols_lookup <- load_dimension_table(
    current_iso,
    current_ver,
    type = "columns"
  )

  # C. THE NATIONAL JOIN: Create the wide "Flat Table"
  # This merges original codes with CPC/ISIC/Bio labels
  national_flat <- facts %>%
    left_join(rows_lookup, by = c("row_id" = "stable_id")) %>%
    left_join(cols_lookup, by = c("col_id" = "stable_id"))

  # D. EXPORT NATIONAL EXCEL: The "Intermediate Product"
  # Saved in the country's external output folder
  output_dir <- file.path(base_data_path, tolower(current_iso), "output")
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  write_xlsx(
    national_flat,
    path = file.path(
      output_dir,
      paste0(current_iso, "_", current_ver, "_flat_table.xlsx")
    )
  )

  # E. APPEND TO MASTER DUCKDB: The "Deduplication" logic
  # We delete existing entries for this country to ensure a clean refresh
  dbExecute(
    con,
    sprintf("DELETE FROM global_flat_table WHERE iso3 = '%s'", current_iso)
  )

  dbWriteTable(con, "global_flat_table", national_flat, append = TRUE)

  message(paste("<<< Finished Node:", current_iso))
}

# 5. CLOSE CONNECTION
dbDisconnect(con, shutdown = TRUE)
message("All nodes successfully integrated into the Global Flat Table.")
