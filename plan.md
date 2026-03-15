# Rewrite Plan: `sut_functions.R` and `run_pipeline.R`

## Context

The current `pipeline_engine.R` and `main_run.R` were adapted by a collaborator
from Renato's original single-country workflow (`procesar_cuadrante.R`,
`procesar_cou.R`, `05-procesamiento-con-funciones.R`). The config idea and
DuckDB store are good additions, but the implementation drifted from the
original's clarity and introduced fragility. This plan describes a clean rewrite
that preserves the good ideas and discards the rest.

> **Terminology note:** What the collaborator called "manifests" are called
> **configs** throughout this project — consistent with Renato's original
> naming convention. The folder is `configs/`, the files are `config_pan.csv`,
> and the variable in code is `config` / `config_files`.

---

## What to Keep

- **Config CSVs** in `configs/` — one per country, documents every quadrant
  extraction explicitly. Better than a hardcoded list.
- **DuckDB** as the master multi-country store — the single source of truth for
  all countries and all years, used for dashboards and APIs.
- **Versioned stable IDs** for rows and columns — the core traceability idea.
- **Repo-relative lookup files** in `inputs/lookups/<iso3>/` — small, versioned,
  not stored with raw data.
- **Separation of concerns**: one file for engine functions, one for the
  orchestration loop.

## What to Fix

### 1. File naming

`pipeline_engine.R` and `main_run.R` are renamed for clarity:

```
scripts/sut_functions.R   <- pure functions only (no library() calls)
scripts/run_pipeline.R    <- orchestration script you actually execute
```

`sut_functions.R` is self-documenting — anyone opening the repo knows exactly
what's in it. `run_pipeline.R` is imperative — it's the thing you run.

### 2. Config: slim down to year-specific columns only

The config CSV should contain only what is irreducibly year-specific. Everything
fixed by the lookup version belongs in the lookup `quadrants` metadata sheet,
**with one exception**: `sheet_name` stays in the config because some countries
pack all years into one file and use the year (e.g. `"2018"`) as the sheet name.
If `sheet_name` ever changes within a version, it is a config change, not a
version change.

**Config columns:**

| Column | Description |
|---|---|
| `iso3` | Country code |
| `year` | Reference year of the source Excel file |
| `lookup_version` | Clean version token e.g. `v02` (not a filename) |
| `quadrant_code` | Short key e.g. `q01` — joins to the `quadrants` sheet in the lookup |
| `quadrant` | Human-readable label from the source file (documentation only) |
| `file_name` | Source Excel filename — changes year to year |
| `sheet_name` | Sheet name in the source Excel — may vary by year for multi-year files |

`quadrant` in the config is kept for documentation and cross-reference, but the
**authoritative value is in the lookup `quadrants` sheet**. At load time the
pipeline checks that both match and warns if they do not, then drops the config
copy.

`cell_range`, `excl_rows`, and `excl_cols` are in the lookup `quadrants` sheet —
they are properties of the classification version's table layout. If any of these
change, a new lookup version should be created.

**Example `config_pan.csv`:**

```
iso3,year,lookup_version,quadrant_code,quadrant,file_name,sheet_name
PAN,2018,v02,q01,OFERTA DE PRODUCTOS A PRECIOS DE COMPRADOR,PAN_COU_Corr_2018.xlsx,Oferta
PAN,2018,v02,q02,UTILIZACION DE PRODUCTOS A PRECIOS DE COMPRADOR,PAN_COU_Corr_2018.xlsx,Utilización
PAN,2019,v02,q01,OFERTA DE PRODUCTOS A PRECIOS DE COMPRADOR,PAN_COU_Corr_2019.xlsx,Oferta
PAN,2019,v02,q02,UTILIZACION DE PRODUCTOS A PRECIOS DE COMPRADOR,PAN_COU_Corr_2019.xlsx,Utilización
```

### 3. `sut_functions.R`: rewrite close to original style

The rewrite should:

- Mirror `procesar_cuadrante()` closely — same argument names where sensible,
  same step-by-step structure.
- Use tidyverse consistently.
- Replace `parse_indices()` with `parse_excl()` — handles `NA`, `""`, `"0"`,
  `"none"` safely.
- Keep `get_base_path()` for reading `BIO_DATA_PATH` from `.Renviron`.
- `load_dimension_table()` stays repo-relative, no default for `type`.
- No `library()` calls inside `sut_functions.R`.

**Function inventory for `sut_functions.R`:**

```
get_base_path()          - reads BIO_DATA_PATH from environment
parse_excl()             - safely parse a scalar exclusion string to integer(0)
load_quadrant_meta()     - reads the quadrants metadata sheet from the lookup file
extract_sut_quadrant()   - processes one config row + metadata -> long tidy tibble
load_dimension_table()   - reads rows/columns lookup sheet from repo
```

### 4. `run_pipeline.R`: rewrite as a clean orchestration script

Structure mirrors `05-procesamiento-con-funciones.R` in spirit:

```
1. SETUP       - source sut_functions.R, load libraries, get base_data_path
2. DISCOVER    - list config CSVs from configs/
3. DATABASE    - open DuckDB connection
4. LOOP        - for each config:
     A. Load config
     B. Load quadrant metadata from lookup, join to config on quadrant_code
        - stop() if any quadrant_code has no match in metadata
        - warn() if quadrant label in config differs from metadata
     C. Extract all quadrants -> bind_rows -> facts (includes year column)
     D. Load row and column lookups
     E. Join facts + lookups -> national_flat
     F. Upsert into DuckDB (DELETE matching iso3+year, then append)
     G. Export per-country Excel from DuckDB for human inspection
5. CLOSE       - disconnect DuckDB, message done
```

No `purrr` dependency for the main loop — use `lapply` + `bind_rows`.

### 5. Stable ID format — reference quadrant, not target table

Stable IDs use `quadrant_code` (not `target_table`) because quadrants within
the same target table do not necessarily share the same columns — each quadrant
has its own independent row and column sequences. Using `target_table` would
require manually continuing the sequence across quadrants and make it impossible
to tell which stable ID belongs to which quadrant file.

Stable IDs encode: **country + lookup version + quadrant code + dimension + sequence**:

```
<iso3>_<version>_<quadrant_code>_r0001   <- row stable ID
<iso3>_<version>_<quadrant_code>_c0001   <- column stable ID
```

All lowercase, underscore-separated, zero-padded **4 digits**.

**Examples for Panama v02:**
- `pan_v02_q01_r0001` — first row of quadrant q01
- `pan_v02_q01_c0001` — first column of quadrant q01
- `pan_v02_q02_r0001` — first row of quadrant q02 (independent sequence)

The lookup `rows` and `columns` sheets stack all quadrants on top of one another,
each with their own `row_id` / `col_id` sequences starting from `0001`.

### 6. Lookup file structure

```
inputs/lookups/<iso3>/<ISO3>_<version>_lookups.xlsx
```

Example: `inputs/lookups/pan/PAN_v02_lookups.xlsx`

**Three sheets: `quadrants`, `rows`, `columns` (all lowercase).**

#### `quadrants` sheet — authoritative version layout metadata

| Column | Description |
|---|---|
| `quadrant_code` | Short key (`q01`, `q02`) — foreign key from config |
| `quadrant` | Human-readable label from source file (authoritative copy) |
| `target_table` | `supply`, `use`, `va`, `employment` |
| `cell_range` | Cell range of the data rectangle |
| `excl_rows` | Comma-separated row indices to exclude (totals, blanks, etc.) |
| `excl_cols` | Comma-separated column indices to exclude |

Note: `sheet_name` is intentionally absent — it lives in the config because
countries that pack all years into one file use different sheet names per year.

A failed join on `quadrant_code` is a hard `stop()` — extraction is undefined
without metadata.

The `quadrant` column in the config is checked against this sheet after the join.
A mismatch triggers `warning()` but not a stop — it is documentation drift, not
a structural error. The lookup value is kept; the config value is dropped.

#### `rows` and `columns` sheets — dimension lookups

Each covers all target tables for this version, stacked.

**Minimum columns:**

| Column | Description |
|---|---|
| `table_code` | Integer for ordering (`1` = supply, `2` = use, etc.) |
| `table` | Target table name (`supply`, `use`, `va`, `employment`) |
| `stable_id` | Must match the stable ID generated by `extract_sut_quadrant()` |
| `original_code` | Code as it appears in the source Excel |
| `label_es` | Spanish label |
| `label_en` | English label |

Additional classification columns (CPC, ISIC, bioeconomy flags, etc.) can be
added freely — the join is on `stable_id` only.

`target_table` is derived from the `quadrants` metadata sheet (authoritative).
The `table` column in `rows`/`columns` is a sanity-check duplicate — compared
after joining, warned on mismatch, then dropped.

### 7. Output structure

The **DuckDB file is the master**. Per-country Excel files are derived exports
for human inspection, not intermediate products.

```
# Master store — all countries, all years
<BIO_DATA_PATH>/bioeconomy_latam.duckdb

# Per-country export — all years for that country, regenerated each run
<BIO_DATA_PATH>/pan/output/PAN_flat.xlsx
```

Both live outside the repo. The Excel is written by querying DuckDB after the
upsert, ensuring it always reflects the master store.

---

## File Structure After Rewrite

```
bioeconlac/
├── configs/
│   └── config_pan.csv             <- iso3, year, lookup_version, quadrant_code, quadrant, file_name
├── inputs/
│   └── lookups/
│       └── pan/
│           └── PAN_v02_lookups.xlsx   <- sheets: quadrants, rows, columns
├── scripts/
│   ├── sut_functions.R            <- rewritten: functions only, no library()
│   ├── run_pipeline.R             <- rewritten: orchestration loop
│   ├── procesar_cuadrante.R       <- original, keep as reference
│   ├── procesar_cou.R             <- original, keep as reference
│   └── 05-procesamiento-con-funciones.R  <- original, keep as reference
```

---

## Order of Work

1. Update `configs/config_pan.csv` — slim to new columns, add `quadrant_code`.
2. Update `inputs/lookups/pan/PAN_v02_lookups.xlsx` — add `quadrants` sheet.
3. Rewrite `scripts/sut_functions.R`.
4. Update `scripts/run_pipeline.R` to join quadrant metadata before extraction.
5. Run end-to-end for Panama, verify output Excel and DuckDB table.
