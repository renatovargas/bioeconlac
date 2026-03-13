# Rapid Stitching of Bioeconomy databases
# Renato Vargas
# Libraries

library(tidyverse)
library(openxlsx)
library(readxl)

rm(list = ls())


data_path <- "~/Dropbox/work/2025-06-CEPAL/data/old_databases"

# Data ingestion
db_files <- list.files(
  data_path,
  pattern = "*.xlsx",
  full.names = TRUE,
  recursive = TRUE
)

# For the list of Excel files in db_files
# We need to extract the database in the sheet
# With the pattern that ends in *SCN_DB and
# Stitch them together into one single flat database.

# Extract reference column names from the first valid file
ref_names <- {
  ref_sheets <- excel_sheets(db_files[[1]])
  ref_sheet <- ref_sheets[grepl("_SCN_BD$", ref_sheets)]
  names(read_excel(db_files[[1]], sheet = ref_sheet, n_max = 0))
}

# Find the matching sheet in each file and read it
read_scn_bd <- function(path) {
  sheets <- excel_sheets(path)
  db_sheet <- sheets[grepl("_SCN_BD$", sheets)]
  if (length(db_sheet) == 0) {
    warning("No *_SCN_BD sheet found in: ", path, " — skipping")
    return(NULL)
  }
  df <- read_excel(
    path,
    sheet = db_sheet,
    col_types = "text" # read everything as text to avoid type conflicts
  )
  # Overwrite column names with reference names (structure is consistent)
  names(df) <- ref_names
  df
}

stitched <- lapply(db_files, read_scn_bd) |>
  bind_rows() |>
  mutate(
    ISO3 = substr(Filas, 1, 3),
    .before = everything(),
    Año = as.integer(Año),
    Valor = as.numeric(Valor)
    # Codigo Transacción Filas intentionally left as character
  )

# Export to RDS and DuckDB
saveRDS(stitched, file.path(data_path, "SCN_BD_stitched.rds"))

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  file.path(data_path, "SCN_BD_stitched.duckdb")
)
duckdb::dbWriteTable(con, "scn_bd", stitched, overwrite = TRUE)
duckdb::dbDisconnect(con)

message(
  "Done. ",
  nrow(stitched),
  " rows stitched from ",
  length(db_files),
  " files."
)
