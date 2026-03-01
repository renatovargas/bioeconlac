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

# ── Core extraction ───────────────────────────────────────────────────────────

extract_sut_quadrant <- function(row) {
  # row: a single-row data frame from the manifest

  base_path <- get_base_path()
  iso3 <- tolower(row$iso3)
  version <- row$lookup_version
  target_table <- row$target_table
  year <- as.integer(row$year)

  # Resolve file path: <BIO_DATA_PATH>/<iso3>/sut/<file_name>
  full_path <- file.path(base_path, iso3, "sut", row$file_name)
  if (!file.exists(full_path)) {
    stop("File missing: ", full_path)
  }

  # Parse exclusions
  excl_rows <- parse_excl(row$exclude_rows)
  excl_cols <- parse_excl(row$exclude_cols)

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
    iso3,
    version,
    target_table,
    seq_len(n_rows)
  )
  col_ids <- sprintf(
    "%s_%s_%s_c%04d",
    iso3,
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

  # Pivot longer → tidy fact rows
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
      quadrant = row$quadrant,
      .before = 1
    )
}

# ── Lookup loader ─────────────────────────────────────────────────────────────

load_dimension_table <- function(iso3, version, type) {
  # type: "rows" or "columns"
  file_name <- paste0(toupper(iso3), "_", version, "_lookups.xlsx")
  lookup_path <- file.path("inputs", "lookups", tolower(iso3), file_name)
  if (!file.exists(lookup_path)) {
    stop("Lookup file missing: ", lookup_path)
  }
  read_excel(lookup_path, sheet = type)
}
