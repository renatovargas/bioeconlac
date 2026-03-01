# SUT Pipeline — Orchestration
# Renato Vargas
#
# Run this script to process all country manifests, build the DuckDB master
# store, and export per-country flat Excel files.

# ── 1. SETUP ──────────────────────────────────────────────────────────────────

source("scripts/sut_functions.R")

library(tidyverse)
library(readxl)
library(writexl)
library(duckdb)

base_data_path <- get_base_path()

# ── 2. DISCOVER ───────────────────────────────────────────────────────────────

manifest_files <- list.files("manifests", pattern = "\.csv$", full.names = TRUE)
if (length(manifest_files) == 0) {
  stop("No manifest CSVs found in manifests/")
}

# ── 3. DATABASE ───────────────────────────────────────────────────────────────

db_path <- file.path(base_data_path, "bioeconomy_latam.duckdb")
con <- dbConnect(duckdb(), db_path)

# ── 4. LOOP ───────────────────────────────────────────────────────────────────

for (m_path in manifest_files) {
  manifest <- read_csv(
    m_path,
    show_col_types = FALSE,
    locale = locale(encoding = "UTF-8")
  )

  current_iso <- unique(manifest$iso3)
  current_ver <- unique(manifest$lookup_version)

  message("── Processing: ", current_iso, " (lookup ", current_ver, ")")

  # A. Extract all quadrants → long fact table
  facts <- lapply(
    split(manifest, seq_len(nrow(manifest))),
    extract_sut_quadrant
  ) |>
    bind_rows()

  # B. Load row and column lookups
  rows_lookup <- load_dimension_table(current_iso, current_ver, type = "rows")
  cols_lookup <- load_dimension_table(
    current_iso,
    current_ver,
    type = "columns"
  )

  # C. Join facts + lookups
  national_flat <- facts |>
    left_join(rows_lookup, by = c("row_id" = "stable_id")) |>
    left_join(
      cols_lookup,
      by = c("col_id" = "stable_id"),
      suffix = c("_row", "_col")
    )

  # D. Sanity check: target_table from manifest must match lookup's table column
  mismatches <- national_flat |>
    filter(target_table != table_row | target_table != table_col)
  if (nrow(mismatches) > 0) {
    warning(
      current_iso,
      ": ",
      nrow(mismatches),
      " rows have target_table mismatch between manifest and lookup"
    )
  }

  # Drop redundant lookup table columns — manifest-derived target_table is authoritative
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

  message("  ✓ Upserted ", nrow(national_flat), " rows for ", current_iso)

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

dbDisconnect(con)
message("── Done. DuckDB: ", db_path)
