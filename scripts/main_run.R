# 1. SETUP
source("scripts/pipeline_engine.R")
library(duckdb)
library(purrr)
library(dplyr)
library(readr)
library(writexl)

# Get path from .Renviron
base_path <- get_base_path()
db_path <- file.path(base_path, "bioeconomy_latam.duckdb")

# 2. CRAWL FOR MANIFESTS
# This finds every file named 'manifest_*.csv' in any country folder
manifest_files <- list.files(
  path = base_path,
  pattern = "manifest_.*\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

if (length(manifest_files) == 0) {
  stop("No manifests found!")
}

# 3. INITIALIZE DATABASE
con <- dbConnect(duckdb(), db_path)

# 4. GLOBAL PROCESSING LOOP
for (m_path in manifest_files) {
  # Load specific manifest
  manifest <- read_csv(m_path, show_col_types = FALSE)
  current_iso <- toupper(manifest$iso3[1])
  current_ver <- manifest$lookup_version[1]

  message(paste(">>> Weaving Node:", current_iso, "Version:", current_ver))

  # A. Extract Raw Facts (The long-format body)
  facts <- manifest %>%
    split(1:nrow(.)) %>%
    map_df(~ extract_sut_quadrant(.x, base_path = base_path))

  # B. Load National Lookups (The Artisan Excel sheets)
  rows_lookup <- load_dimension_table(current_iso, current_ver, type = "Rows")
  cols_lookup <- load_dimension_table(current_iso, current_ver, type = "Cols")

  # C. CREATE NATIONAL FLAT TABLE (The "Join-then-Append" Strategy)
  # This joins the original codes with your CPC/ISIC/Bio labels
  national_flat <- facts %>%
    left_join(rows_lookup, by = c("row_id" = "stable_id")) %>%
    left_join(cols_lookup, by = c("col_id" = "stable_id"))

  # D. EXPORT NATIONAL EXCEL (The "Intermediate Product")
  output_dir <- file.path(base_path, tolower(current_iso), "output")
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

  # E. APPEND TO MASTER DUCKDB
  # Clean up existing data for this country/version to prevent duplicates
  dbExecute(
    con,
    sprintf(
      "DELETE FROM global_flat_table WHERE iso3 = '%s' AND version = '%s'",
      current_iso,
      current_ver
    )
  )

  dbWriteTable(con, "global_flat_table", national_flat, append = TRUE)

  message(paste("<<< Finished Node:", current_iso))
}

# 5. CLOSE & CLEANUP
dbDisconnect(con, shutdown = TRUE)
message("All nodes successfully integrated into the Global Flat Table.")
