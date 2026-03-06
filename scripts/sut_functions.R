# SUT Pipeline Functions
# Renato Vargas
#
# Pure functions for extracting and loading SUT quadrant data.
# No library() calls here — load libraries in run_pipeline.R.

# ── Environment ───────────────────────────────────────────────────────────────

get_base_path <- function() {
  path <- Sys.getenv("BIO_DATA_PATH")
  if (nchar(path) == 0) {
    stop("BIO_DATA_PATH is not set in .Renviron")
  }
  path
}

# ── Helpers ───────────────────────────────────────────────────────────────────

# Safely parse a comma-separated exclusion string to an integer vector.
# Returns integer(0) for NA, empty string, "0", or "none".
parse_excl <- function(x) {
  if (length(x) == 0) {
    return(integer(0))
  }
  if (all(is.na(x))) {
    return(integer(0))
  }
  x <- trimws(as.character(x))
  if (nchar(x) == 0 || x == "0" || tolower(x) == "none") {
    return(integer(0))
  }
  as.integer(unlist(strsplit(x, ",")))
}

# ── Quadrant metadata loader ──────────────────────────────────────────────────

# Loads the quadrants metadata sheet from the versioned lookup file.
# Returns a data frame with one row per quadrant_code for this version.
# sheet_name is NOT here — it lives in the config, as it varies by year-file
# (e.g. countries that pack all years into one file use the year as sheet name).
load_quadrant_meta <- function(iso3, version) {
  lookup_path <- file.path(
    "inputs",
    tolower(iso3),
    paste0(tolower(iso3), "_", version, "_lookups.xlsx")
  )
  if (!file.exists(lookup_path)) {
    stop("Lookup file missing: ", lookup_path)
  }
  read_excel(lookup_path, sheet = "quadrants")
}

# ── Core extraction ───────────────────────────────────────────────────────────

# Processes one config row (already joined with quadrant metadata) into a
# long tidy tibble of fact rows.
# sheet_name comes from config (varies by year-file).
# cell_range, excl_rows, excl_cols come from quadrant metadata (fixed by version).
extract_sut_quadrant <- function(row) {
  # row: a single-row data frame with config + quadrant metadata columns

  base_path <- get_base_path()
  iso3 <- tolower(row$iso3)
  version <- row$lookup_version
  target_table <- row$target_table # from quadrant metadata (authoritative)
  year <- as.integer(row$year)

  # Resolve file path: <BIO_DATA_PATH>/<iso3>/sut/<file_name>
  full_path <- file.path(base_path, iso3, "sut", row$file_name)
  if (!file.exists(full_path)) {
    stop("File missing: ", full_path)
  }

  # Parse exclusions (from quadrant metadata)
  excl_rows <- parse_excl(row$excl_rows)
  excl_cols <- parse_excl(row$excl_cols)

  # Read the data rectangle — all numeric
  datos <- read_excel(
    full_path,
    sheet = row$sheet_name,
    range = row$cell_range,
    col_names = FALSE,
    col_types = "numeric"
  )

  # Build stable IDs (4-digit zero-padded)
  n_rows <- nrow(datos)
  n_cols <- ncol(datos)
  row_ids <- sprintf(
    "%s_%s_%s_r%04d",
    toupper(iso3),
    version,
    target_table,
    seq_len(n_rows)
  )
  col_ids <- sprintf(
    "%s_%s_%s_c%04d",
    toupper(iso3),
    version,
    target_table,
    seq_len(n_cols)
  )

  # Replace NA with 0, assign stable ID names
  datos <- datos |>
    mutate(across(everything(), ~ replace_na(.x, 0))) |>
    setNames(col_ids) |>
    mutate(row_id = row_ids, .before = 1)

  # Exclude flagged rows and columns
  if (length(excl_rows) > 0) {
    datos <- filter(datos, !row_id %in% row_ids[excl_rows])
  }
  if (length(excl_cols) > 0) {
    datos <- select(datos, -all_of(col_ids[excl_cols]))
  }

  # Pivot longer -> tidy fact rows
  datos |>
    pivot_longer(
      cols = -row_id,
      names_to = "col_id",
      values_to = "value"
    ) |>
    mutate(
      iso3 = toupper(row$iso3),
      year = year,
      lookup_version = version,
      target_table = target_table,
      quadrant_code = row$quadrant_code,
      quadrant = row$quadrant, # authoritative label from metadata
      .before = 1
    )
}

# ── Dimension table loader ────────────────────────────────────────────────────

# type: "rows" or "columns"
# rows sheet must have a column named row_id
# columns sheet must have a column named col_id
load_dimension_table <- function(iso3, version, type) {
  lookup_path <- file.path(
    "inputs",
    tolower(iso3),
    paste0(tolower(iso3), "_", version, "_lookups.xlsx")
  )
  if (!file.exists(lookup_path)) {
    stop("Lookup file missing: ", lookup_path)
  }
  read_excel(lookup_path, sheet = type)
}
