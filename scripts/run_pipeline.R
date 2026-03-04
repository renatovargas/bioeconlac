# SUT Pipeline — Orchestration
# Renato Vargas
#
# Run this script to process all country configs, build the DuckDB master
# store, and export per-country flat Excel files.

# ── 1. SETUP ──────────────────────────────────────────────────────────────────

source("scripts/sut_functions.R")

library(tidyverse)
library(readxl)
library(writexl)
library(duckdb)

base_data_path <- get_base_path()

# ── 2. DISCOVER ───────────────────────────────────────────────────────────────

config_files <- list.files(
  "inputs/configs",
  pattern = "\\.csv$",
  full.names = TRUE
)
if (length(config_files) == 0) {
  stop("No config CSVs found in configs/")
}

# ── 3. DATABASE ───────────────────────────────────────────────────────────────

db_path <- file.path(base_data_path, "bioeconomy_latam.duckdb")
con <- dbConnect(duckdb(), db_path)

# ── 4. LOOP ───────────────────────────────────────────────────────────────────

for (m_path in config_files) {
  config <- read_csv(
    m_path,
    show_col_types = FALSE,
    locale = locale(encoding = "UTF-8")
  )

  current_iso <- unique(config$iso3)
  current_ver <- unique(config$lookup_version)

  message("── Processing: ", current_iso, " (lookup ", current_ver, ")")

  # A. Load quadrant metadata and join to config on quadrant_code
  quadrant_meta <- load_quadrant_meta(current_iso, current_ver)

  unmatched <- setdiff(config$quadrant_code, quadrant_meta$quadrant_code)
  if (length(unmatched) > 0) {
    stop(
      current_iso,
      ": quadrant_code(s) not found in lookup metadata: ",
      paste(unmatched, collapse = ", ")
    )
  }

  config_full <- config |>
    left_join(
      quadrant_meta,
      by = "quadrant_code",
      suffix = c("_config", "_meta")
    )

  # Sanity check: quadrant label in config should match metadata
  label_mismatches <- config_full |>
    filter(quadrant_config != quadrant_meta)
  if (nrow(label_mismatches) > 0) {
    warning(
      current_iso,
      ": quadrant label mismatch between config and lookup metadata ",
      "for code(s): ",
      paste(label_mismatches$quadrant_code, collapse = ", "),
      " — using lookup metadata value"
    )
  }

  # Drop config quadrant label, keep authoritative metadata label
  config_full <- config_full |>
    select(-quadrant_config) |>
    rename(quadrant = quadrant_meta)

  # B. Extract all quadrants -> long fact table
  facts <- lapply(
    split(config_full, seq_len(nrow(config_full))),
    extract_sut_quadrant
  ) |>
    bind_rows()

  # C. Load row and column lookups
  rows_lookup <- load_dimension_table(current_iso, current_ver, type = "rows")
  cols_lookup <- load_dimension_table(
    current_iso,
    current_ver,
    type = "columns"
  )

  # D. Join facts + lookups
  national_flat <- facts |>
    left_join(rows_lookup, by = "row_id") |>
    left_join(cols_lookup, by = "col_id", suffix = c("_row", "_col"))

  # Sanity check: target_table from metadata must match lookup table column
  mismatches <- national_flat |>
    filter(target_table != table_row | target_table != table_col)
  if (nrow(mismatches) > 0) {
    warning(
      current_iso,
      ": ",
      nrow(mismatches),
      " rows have target_table mismatch between quadrant metadata and lookup"
    )
  }

  # Drop redundant lookup table columns — metadata-derived target_table is authoritative
  national_flat <- national_flat |>
    select(-table_row, -table_col)

  # E. Upsert into DuckDB: delete existing rows for this iso3 + year, then append
  years_in_batch <- paste(unique(national_flat$year), collapse = ", ")
  dbExecute(
    con,
    sprintf(
      "DELETE FROM sut_flat WHERE iso3 = '%s' AND year IN (%s)",
      current_iso,
      years_in_batch
    )
  )
  dbWriteTable(con, "sut_flat", national_flat, append = TRUE)

  message("  v Upserted ", nrow(national_flat), " rows for ", current_iso)

  # F. Export per-country Excel from DuckDB (authoritative export)
  output_dir <- file.path(base_data_path, tolower(current_iso), "output")
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  output_path <- file.path(output_dir, paste0(current_iso, "_flat.xlsx"))

  country_data <- dbGetQuery(
    con,
    sprintf("SELECT * FROM sut_flat WHERE iso3 = '%s'", current_iso)
  )
  write_xlsx(list(flat = country_data), output_path)
  message("  ✓ Exported Excel: ", output_path)
}

# ── 5. CLOSE ──────────────────────────────────────────────────────────────────
message("  v Exported Excel: ", output_path)
dbDisconnect(con)
message("── Done. DuckDB: ", db_path)
