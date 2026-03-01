# Rewrite Plan: `sut_functions.R` and `run_pipeline.R`

## Context

The current `pipeline_engine.R` and `main_run.R` were adapted by a collaborator
from Renato's original single-country workflow (`procesar_cuadrante.R`,
`procesar_cou.R`, `05-procesamiento-con-funciones.R`). The manifest idea and
DuckDB store are good additions, but the implementation drifted from the
original's clarity and introduced fragility. This plan describes a clean rewrite
that preserves the good ideas and discards the rest.

---

## What to Keep

- **Manifest CSVs** in `manifests/` — one per country, documents every quadrant
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
scripts/sut_functions.R   ← pure functions only (no library() calls)
scripts/run_pipeline.R    ← orchestration script you actually execute
```

`sut_functions.R` is self-documenting — anyone opening the repo knows exactly
what's in it. `run_pipeline.R` is imperative — it's the thing you run.

### 2. Manifest: clean `lookup_version` and add `year`

Two changes to the manifest CSV:

- `lookup_version` currently holds a filename (`pan_lookup_v02.xlsx`) instead of
  a clean version string. It should be a plain token like `v02`. The lookup
  filename will be constructed in code from `iso3` + `lookup_version`.
- `year` must be an explicit column. SUT tables are always year-specific and the
  entire purpose of this pipeline is to stack multiple years into a single flat
  database. Easier to add now than retrofit later.

**Action:** Update manifest CSVs so `lookup_version` = `v02` and add a `year`
column to every row.

### 3. `sut_functions.R`: rewrite close to original style

The rewrite should:

- Mirror `procesar_cuadrante()` closely — same argument names where sensible,
  same step-by-step structure.
- Use tidyverse consistently.
- Remove `parse_indices()` — replace with a simpler `parse_excl()` helper that
  handles `NA`, `""`, `"0"`, `"none"` safely using `all(is.na(x))` + `nchar()`
  checks, not a fragile scalar `if` chain.
- Keep `get_base_path()` for reading `BIO_DATA_PATH` from `.Renviron`.
- `load_dimension_table()` stays repo-relative, no default for `type`.
- No `library()` calls inside `sut_functions.R` — libraries are loaded by
  the caller (`run_pipeline.R`).

**Function inventory for `sut_functions.R`:**

```
get_base_path()          — reads BIO_DATA_PATH from environment
parse_excl()             — safely parse a scalar exclusion string → integer(0)
extract_sut_quadrant()   — processes one manifest row → long tidy tibble
load_dimension_table()   — reads rows/columns lookup sheet from repo
```

### 4. `run_pipeline.R`: rewrite as a clean orchestration script

Structure mirrors `05-procesamiento-con-funciones.R` in spirit:

```
1. SETUP       — source sut_functions.R, load libraries, get base_data_path
2. DISCOVER    — list manifest CSVs from manifests/
3. DATABASE    — open DuckDB connection
4. LOOP        — for each manifest:
     A. Load manifest
     B. Extract all quadrants → bind_rows → facts (includes year column)
     C. Load row/column lookups
     D. Join facts + lookups → national_flat
     E. Upsert national_flat into DuckDB (DELETE matching iso3+year, then append)
     F. Export per-country Excel from DuckDB for human inspection
5. CLOSE       — disconnect DuckDB, message done
```

No `purrr` dependency for the main loop — use `lapply` + `bind_rows` as in the
original, which is more readable for this use case.

### 5. Stable ID format — reference target table, not quadrant

The quadrant (`mp`, `ot`, etc.) is a filing artifact of the source country's
statistical office, not an economic dimension. Two quadrant files that both feed
the supply table share the same set of rows and columns. Encoding the quadrant
into the stable ID would create false duplicates in the lookup and break joins.

Stable IDs encode: **country + lookup version + target table + dimension + sequence**:

```
<iso3>_<version>_<target_table>_r001   ← row stable ID
<iso3>_<version>_<target_table>_c001   ← column stable ID
```

All lowercase, underscore-separated, zero-padded 3 digits.

**Target table tokens:** `supply`, `use`, `va`, `employment`

**Examples:**
- `pan_v02_supply_r001` — first row of Panama's supply table, lookup version 02
- `pan_v02_supply_c001` — first column of Panama's supply table, lookup version 02
- `pan_v02_use_r001`    — first row of Panama's use table

The `lookup_version` in the stable ID refers to the **classification version**,
not the year. A country might use `v02` lookups for years 2018–2021 and `v03`
lookups for 2022 onward (when the statistical office revised their product list).
The flat table will contain rows with different `lookup_version` values stacked —
this is intentional and expected. Aggregation to international classifications
(CPC, ISIC) handles the reconciliation.

The manifest column `target_table` declares which table each quadrant file
feeds into. `extract_sut_quadrant()` uses `target_table` (not `quadrant`) to
construct stable IDs.

### 6. Lookup file structure

```
inputs/lookups/<iso3>/<ISO3>_<version>_lookups.xlsx
```

Example: `inputs/lookups/pan/PAN_v02_lookups.xlsx`

**Two sheets: `rows` and `columns` (lowercase, no suffix).**

Each sheet covers all target tables for this version, stacked. The `table` and
`table_code` columns are present in the lookup for human readability and correct
ordering when users open the file in Excel — they are **not** the authoritative
source of `target_table` in the pipeline.

**Minimum columns expected in each sheet:**

| Column | Description |
|---|---|
| `table_code` | Integer for ordering (e.g. `1` = supply, `2` = use, etc.) |
| `table` | Human-readable target table name (`supply`, `use`, `va`, `employment`) |
| `stable_id` | Must match the stable ID generated by `extract_sut_quadrant()` |
| `original_code` | The code as it appears in the source Excel |
| `label_es` | Spanish label |
| `label_en` | English label |

Additional classification columns (CPC, ISIC, bioeconomy flags, etc.) can be
added freely — the join is on `stable_id` only.

**`target_table` is authoritative in the manifest, not the lookup.**
`extract_sut_quadrant()` reads `target_table` from the manifest row and:

1. Uses it to construct stable IDs (`pan_v02_supply_r001`, etc.).
2. Attaches `target_table` and `year` as explicit columns on every fact row.

At join time, the lookup's `table` column is compared against the manifest-
derived `target_table` as a **sanity check**:

```r
mismatches <- national_flat |>
  filter(target_table != table.x | target_table != table.y)
if (nrow(mismatches) > 0) warning("target_table mismatch between manifest and lookup")

national_flat <- national_flat |>
  select(-table.x, -table.y)   # drop redundant lookup columns, keep manifest-derived
```

A country with a standard two-file supply table (`mp` + `ot`) lists supply rows
**once** in the lookup under `table = "supply"`. Both quadrant files generate
row IDs from that same set.

### 7. Output structure

The **DuckDB file is the master**. Per-country Excel files are derived exports
for human inspection, not intermediate products. They are named by country only —
not by lookup version, since a single country flat file will contain multiple
years that may span multiple lookup versions.

```
# Master store — all countries, all years, append across pipeline runs
<BIO_DATA_PATH>/bioeconomy_latam.duckdb

# Per-country export — all years for that country, regenerated each run
<BIO_DATA_PATH>/pan/output/PAN_flat.xlsx
```

Both live outside the repo (derived artifacts, potentially large). The Excel is
written by querying DuckDB after the upsert, ensuring it always reflects the
master store.

---

## File Structure After Rewrite

```
bioeconlac/
├── manifests/
│   └── manifest_pan.csv          ← lookup_version = "v02", year column added
├── inputs/
│   └── lookups/
│       └── pan/
│           └── PAN_v02_lookups.xlsx   ← sheets: rows, columns
├── scripts/
│   ├── sut_functions.R           ← rewritten: functions only, no library()
│   ├── run_pipeline.R            ← rewritten: orchestration loop
│   ├── procesar_cuadrante.R      ← original, keep as reference
│   ├── procesar_cou.R            ← original, keep as reference
│   └── 05-procesamiento-con-funciones.R  ← original, keep as reference
```

---

## Order of Work

1. Update `manifests/manifest_pan.csv` — set `lookup_version` = `v02`, add `year`.
2. Write `scripts/sut_functions.R`.
3. Write `scripts/run_pipeline.R`.
4. Create `inputs/lookups/pan/PAN_v02_lookups.xlsx` mockup with correct stable
   IDs to test the join end-to-end.
5. Run end-to-end for Panama, verify output Excel and DuckDB table.
