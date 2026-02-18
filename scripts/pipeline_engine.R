library(tidyverse)
library(readxl)

# Helper to get the base path from environment
get_base_path <- function() {
  path <- Sys.getenv("BIO_DATA_PATH")
  if (path == "") {
    stop("Error: BIO_DATA_PATH environment variable not found.")
  }
  return(path)
}

parse_indices <- function(x) {
  if (is.na(x) || x == "" || x == "0" || tolower(x) == "none") {
    return(integer(0))
  }
  clean_x <- gsub(" ", "", as.character(x))
  as.integer(unlist(strsplit(clean_x, ",")))
}

extract_sut_quadrant <- function(row) {
  # Build path using the ENV variable
  base_path <- get_base_path()
  full_path <- file.path(base_path, tolower(row$iso3), "raw", row$file_name)

  if (!file.exists(full_path)) {
    stop(paste("File missing:", full_path))
  }

  raw_body <- read_excel(
    path = full_path,
    sheet = row$sheet_name,
    range = row$cell_range,
    col_names = FALSE,
    col_types = "numeric"
  )

  raw_body[is.na(raw_body)] <- 0

  row_prefix <- paste(
    row$iso3,
    row$lookup_version,
    toupper(row$quadrant),
    "R",
    sep = "_"
  )
  col_prefix <- paste(
    row$iso3,
    row$lookup_version,
    toupper(row$quadrant),
    "C",
    sep = "_"
  )

  row_stable_ids <- sprintf("%s%03d", row_prefix, seq_len(nrow(raw_body)))
  col_stable_ids <- sprintf("%s%03d", col_prefix, seq_len(ncol(raw_body)))

  processed <- raw_body %>%
    setNames(col_stable_ids) %>%
    mutate(row_id = row_stable_ids, .before = 1) %>%
    filter(!(row_id %in% row_stable_ids[parse_indices(row$excl_rows)])) %>%
    select(-all_of(col_stable_ids[parse_indices(row$excl_cols)])) %>%
    pivot_longer(cols = -row_id, names_to = "col_id", values_to = "value") %>%
    mutate(
      iso3 = row$iso3,
      year = as.integer(row$year),
      target_table = toupper(row$target_table),
      quadrant = tolower(row$quadrant),
      version = row$lookup_version
    )

  return(processed)
}

# Dimension loader updated for external path
load_dimension_table <- function(iso3, version, type = "Rows") {
  base_path <- get_base_path()
  file_name <- paste0(toupper(iso3), "_", version, "_Lookups.xlsx")
  lookup_path <- file.path(base_path, tolower(iso3), "lookups", file_name)
  sheet_to_read <- paste0(type, "_Final")

  read_excel(lookup_path, sheet = sheet_to_read)
}
