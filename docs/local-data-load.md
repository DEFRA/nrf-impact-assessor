# Local Data Load

This guide explains how to load spatial reference data into a local PostGIS database for development and integration testing.

> **WARNING — Destructive operation.** The script **deletes all existing data** for each layer being loaded before inserting new data. It will prompt for confirmation before proceeding. Always ensure you have a backup or can re-source the data files before running.

---

## Overview

`scripts/load_data.py` reads spatial files and lookup databases from a local directory and writes them into the `nrf_reference` schema in PostgreSQL/PostGIS. It is the only supported mechanism for seeding a local database with reference data.

The script handles three categories of data:

| Category | DB Table | Source Format |
|---|---|---|
| Spatial layers (catchments, boundaries, GCN) | `nrf_reference.spatial_layer` | Shapefile / GeoDatabase (GDB) |
| Coefficient polygons | `nrf_reference.coefficient_layer` | GeoPackage (GPKG) |
| EDP boundaries | `nrf_reference.edp_boundary_layer` | GeoPackage (GPKG) |
| Lookup tables | `nrf_reference.lookup_table` | SQLite |

---

## Prerequisites

Before running the script, ensure the following are in place:

1. **PostGIS database is running** — the `db` service must be up:
   ```bash
   docker compose up db
   ```

2. **Database migrations have been applied** — run Alembic to create the schema:
   ```bash
   uv run alembic upgrade head
   ```

3. **uv is installed** — the project uses `uv` as its package manager. See the README if not already set up.

4. **Source data files are available** — the reference data files are not stored in this repository. Obtain them from the shared data store and place them under a local directory (e.g. `./iat_input`). See [Source Data Files](#source-data-files) below for the full list.

5. **`scripts/.env.local` is configured** — copy the example and set your paths:
   ```bash
   cp scripts/.env.example scripts/.env.local
   # Then edit scripts/.env.local
   ```

---

## Configuration

All file paths are read from `scripts/.env.local`. This file is gitignored and must be created locally.

### `.env.local` reference

| Variable | Description | Example value |
|---|---|---|
| `BASE_PATH` | Root directory containing all source data files | `./iat_input` |
| `COEFFICIENT_GPKG` | Coefficient layer GeoPackage (relative to `BASE_PATH`) | `nutrients/NMSCoefficientLayerTEST.gpkg` |
| `COEFFICIENT_LAYER` | Layer name inside the GeoPackage | `NMSCoefficientLayer` |
| `WWTW_SHAPEFILE` | WwTW catchments shapefile (relative to `BASE_PATH`) | `nutrients/wwtw files/WwTW_all_features.shp` |
| `LPA_SHAPEFILE` | LPA boundaries shapefile (relative to `BASE_PATH`) | `nutrients/LPA/LPA_National.shp` |
| `NN_CATCHMENT_SHAPEFILE` | NN catchments shapefile (relative to `BASE_PATH`) | `nutrients/Catchments/NN_Catchments_03_2024.shp` |
| `SUBCATCHMENT_SHAPEFILE` | WFD subcatchments shapefile (relative to `BASE_PATH`) | `nutrients/Catchments/WFD_Surface_Water_Operational_Catchments_Cycle_2.shp` |
| `LOOKUP_DATABASE` | SQLite database with WwTW and rates lookups (relative to `BASE_PATH`) | `nutrients/SQL_Lookups/Interim_coeffs.sqlite` |
| `EDP_BOUNDARY_GPKG` | EDP boundary GeoPackage (relative to `BASE_PATH`) | `nutrients/EDP/norfolk_edp.gpkg` |
| `EDP_BOUNDARY_LAYER` | Layer name inside the EDP GeoPackage | `nn_boundaries_norfolk_edp` |
| `GCN_RISK_ZONES_GDB` | GCN Risk Zones GeoDatabase (relative to `BASE_PATH`) | `gcn/RZ.gdb` |
| `GCN_RISK_ZONES_LAYER` | Layer name inside the GCN Risk Zones GDB | `GCN_RZ_NRF_Final` |
| `GCN_PONDS_GDB` | GCN Ponds GeoDatabase (relative to `BASE_PATH`) | `gcn/IIAT_Layers.gdb` |
| `GCN_PONDS_LAYER` | Layer name for ponds inside the GDB | `NRF_Ponds` |
| `EDP_EDGES_GDB` | EDP Edges GeoDatabase (relative to `BASE_PATH`) | `gcn/IIAT_Layers.gdb` |
| `EDP_EDGES_LAYER` | Layer name for EDP edges inside the GDB | `EDP_Edge` |
| `TEST_SHAPEFILE` | Default test shapefile used by `test_wkt.py` | `tests/data/inputs/nutrients/BnW_small_under_1_hectare/BnW_small_under_1_hectare.shp` |
| `OUTPUT_DIR` | Output directory for script results | `./output` |

The database connection is read from the standard application environment variables (`DATABASE_URL` or equivalent — see [environment.md](environment.md)).

---

## Usage

All commands below are available as `make` targets or as direct `uv run` invocations.

### Load all data

```bash
make load-data
# equivalent:
uv run python scripts/load_data.py
```

Loads all spatial layers, the coefficient layer, EDP boundaries, and all lookup tables. You will be prompted to confirm before any data is deleted.

### Load specific layers

```bash
make load-data-layer LAYER=nn_catchments
# equivalent:
uv run python scripts/load_data.py --layer nn_catchments

# multiple layers (direct invocation only):
uv run python scripts/load_data.py --layer wwtw_catchments --layer lpa_boundaries
```

Valid `--layer` / `LAYER` values:

| Value | Description | DB Table |
|---|---|---|
| `wwtw_catchments` | Wastewater treatment works catchment polygons | `spatial_layer` |
| `lpa_boundaries` | Local planning authority boundary polygons | `spatial_layer` |
| `nn_catchments` | Nutrient neutrality catchment polygons | `spatial_layer` |
| `subcatchments` | WFD surface water operational catchments | `spatial_layer` |
| `gcn_risk_zones` | Great crested newt habitat risk zones | `spatial_layer` |
| `gcn_ponds` | GCN pond locations | `spatial_layer` |
| `edp_edges` | EDP edge geometries | `spatial_layer` |
| `coefficients` | Nutrient mitigation coefficient polygons (~5.4M features) | `coefficient_layer` |
| `edp_boundaries` | EDP boundary polygons | `edp_boundary_layer` |

### Load specific lookup tables

```bash
make load-data-lookup LOOKUP=wwtw_lookup
# equivalent:
uv run python scripts/load_data.py --lookup wwtw_lookup
```

Valid `--lookup` / `LOOKUP` values:

| Value | Description | Source SQLite table |
|---|---|---|
| `wwtw_lookup` | WwTW permits, concentrations and subcatchment assignments | `WwTw_lookup` |
| `rates_lookup` | Nutrient generation rates by NN catchment | `rates_lookup` |

> **Note:** Selective lookup loading is not yet fully implemented. Specifying `--lookup` currently loads all lookup tables regardless of the value given.

### Load sample data only

```bash
make load-data-sample
# equivalent:
uv run python scripts/load_data.py --sample
```

Loads at most 100 features per layer. Use this for quick smoke tests where full coverage is not required. The `--sample` flag can be combined with `--layer`.

---

## Source Data Files

The table below lists every source file consumed by the script and what it maps to in the database.

| Source file (relative to `BASE_PATH`) | Layer / table | Format |
|---|---|---|
| `nutrients/NMSCoefficientLayerTEST.gpkg` | `coefficient_layer` | GeoPackage |
| `nutrients/wwtw files/WwTW_all_features.shp` | `spatial_layer` — `WWTW_CATCHMENTS` | Shapefile |
| `nutrients/LPA/LPA_National.shp` | `spatial_layer` — `LPA_BOUNDARIES` | Shapefile |
| `nutrients/Catchments/NN_Catchments_03_2024.shp` | `spatial_layer` — `NN_CATCHMENTS` | Shapefile |
| `nutrients/Catchments/WFD_Surface_Water_Operational_Catchments_Cycle_2.shp` | `spatial_layer` — `SUBCATCHMENTS` | Shapefile |
| `nutrients/EDP/norfolk_edp.gpkg` | `edp_boundary_layer` | GeoPackage |
| `nutrients/SQL_Lookups/Interim_coeffs.sqlite` | `lookup_table` | SQLite |
| `gcn/RZ.gdb` | `spatial_layer` — `GCN_RISK_ZONES` | GeoDatabase |
| `gcn/IIAT_Layers.gdb` | `spatial_layer` — `GCN_PONDS` and `EDP_EDGES` | GeoDatabase |

If a source file is missing the script prints a warning and **skips that layer** rather than aborting. This allows partial loads when only a subset of files is available.

---

## What the Script Does

For each layer the script follows this sequence:

1. **Read** source file into a GeoPandas GeoDataFrame.
2. **Normalise** geometry:
   - If no CRS is present, assumes `EPSG:27700` (British National Grid).
   - If CRS differs from `EPSG:27700`, reprojects.
   - Converts any 3D geometries to 2D.
3. **Apply sample limit** if `--sample` is set (first 100 rows).
4. **Transform** columns:
   - Assigns a new UUID (`id`) and version `1` to every row.
   - Serialises all non-geometry attributes as a JSONB `attributes` blob.
   - For the coefficient layer, renames source columns to snake_case and coerces coefficient columns to numeric (non-numeric values become `NULL`).
   - For lookup tables, normalises legacy column names (e.g. `NN_Catchment` → `nn_catchment`) and replaces `NaN`/`Inf` with `NULL`.
5. **Delete** existing rows for the layer being loaded.
6. **Insert** new rows in batches (5,000 rows per chunk for spatial layers; 10,000 for coefficients).
7. **Verify** by querying the row count and printing it.

---

## Database Schema

All tables live in the `nrf_reference` PostgreSQL schema.

### `nrf_reference.spatial_layer`

Stores catchment and boundary polygons for all layer types except coefficients and EDP boundaries.

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Primary key (generated by script) |
| `version` | `INTEGER` | Data version (always `1` for script-loaded data) |
| `layer_type` | `spatial_layer_type` (enum) | Identifies which layer this row belongs to |
| `geometry` | `GEOMETRY (EPSG:27700)` | Polygon geometry with spatial index |
| `name` | `VARCHAR` | Feature name if present in source |
| `attributes` | `JSONB` | All original attributes from source file |
| `created_at` | `TIMESTAMPTZ` | Row creation time |

### `nrf_reference.coefficient_layer`

Dedicated table for the nutrient mitigation coefficient polygons (~5.4M rows).

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Primary key |
| `version` | `INTEGER` | Data version |
| `geometry` | `MULTIPOLYGON (EPSG:27700)` | Geometry with spatial index |
| `crome_id` | `VARCHAR` | CROME land use identifier |
| `land_use_cat` | `VARCHAR` | Land use category |
| `nn_catchment` | `VARCHAR` | NN catchment identifier |
| `subcatchment` | `VARCHAR` | Subcatchment identifier |
| `lu_curr_n_coeff` | `FLOAT` | Current land use nitrogen coefficient |
| `lu_curr_p_coeff` | `FLOAT` | Current land use phosphorus coefficient |
| `n_resi_coeff` | `FLOAT` | Residential nitrogen coefficient |
| `p_resi_coeff` | `FLOAT` | Residential phosphorus coefficient |
| `created_at` | `TIMESTAMPTZ` | Row creation time |

### `nrf_reference.edp_boundary_layer`

EDP boundary polygons.

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Primary key |
| `version` | `INTEGER` | Data version |
| `geometry` | `GEOMETRY (EPSG:27700)` | Polygon geometry with spatial index |
| `name` | `VARCHAR` | Boundary name |
| `attributes` | `JSONB` | All original attributes |
| `created_at` | `TIMESTAMPTZ` | Row creation time |

### `nrf_reference.lookup_table`

JSONB-based lookup tables.

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Primary key |
| `name` | `VARCHAR` | Lookup identifier (e.g. `wwtw_lookup`) |
| `version` | `INTEGER` | Data version |
| `data` | `JSONB` | Array of row objects from source SQLite table |
| `description` | `VARCHAR` | Human-readable description |
| `created_at` | `TIMESTAMPTZ` | Row creation time |

---

## Backup & Restore

> **Take a backup before running any load command.** The load script is destructive — it deletes existing rows for each layer before inserting new data.

All backup targets write compressed `.sql.gz` files to `./backups/` by default. The output directory can be overridden with `BACKUP_DIR=<path>`.

### Per-table backup (recommended before a load)

Produces one `.sql.gz` file per `nrf_reference` table, timestamped together so they can be restored as a set.

```bash
make db-backup-tables
# output:
#   backups/nrf_reference_spatial_layer_20260316_120000.sql.gz
#   backups/nrf_reference_coefficient_layer_20260316_120000.sql.gz
#   backups/nrf_reference_edp_boundary_layer_20260316_120000.sql.gz
#   backups/nrf_reference_lookup_table_20260316_120000.sql.gz
```

### Full database backup

Single `.sql.gz` containing the entire `nrf_impact` database (schema + data + custom types + grants).

```bash
make db-backup

# Custom output directory:
make db-backup BACKUP_DIR=~/my-backups
```

### Schema-only backup

DDL only — tables, enums (including `spatial_layer_type`), indexes, and grants. No row data.

```bash
make db-backup-schema
```

### Cluster-level roles and grants

Captures PostgreSQL roles and server-level grants that `pg_dump` does not include.

```bash
make db-backup-globals
```

### Restore

```bash
# Restore a single table backup:
make db-restore BACKUP_FILE=./backups/nrf_reference_spatial_layer_20260316_120000.sql.gz

# Restore a full database backup:
make db-restore BACKUP_FILE=./backups/nrf_impact_20260316_120000.sql.gz
```

Restore pipes the decompressed SQL through `psql` into the running container. Run `docker compose up postgres` first if the container is not already up.

### Backup file naming

| Target | File pattern |
|---|---|
| `db-backup-tables` | `backups/nrf_reference_<table>_<YYYYMMDD_HHMMSS>.sql.gz` |
| `db-backup` | `backups/nrf_impact_<YYYYMMDD_HHMMSS>.sql.gz` |
| `db-backup-schema` | `backups/nrf_impact_schema_<YYYYMMDD_HHMMSS>.sql.gz` |
| `db-backup-globals` | `backups/nrf_impact_globals_<YYYYMMDD_HHMMSS>.sql.gz` |

All files within a single `make` invocation share the same timestamp, making it straightforward to identify backups taken together.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ValidationError` on startup | `scripts/.env.local` is missing or incomplete | Copy `.env.example` to `.env.local` and fill in all required variables |
| `File not found` warning, layer skipped | Source file path is wrong in `.env.local` | Check `BASE_PATH` and the relative paths match your local directory layout |
| `Missing expected columns` error | Coefficient GeoPackage has unexpected column names | Verify the `COEFFICIENT_LAYER` variable matches the layer name in the GPKG and that source column names match those documented above |
| `Connection refused` / DB error | PostGIS is not running | Run `docker compose up db` and re-run the script |
| `relation "nrf_reference.spatial_layer" does not exist` | Migrations have not been applied | Run `uv run alembic upgrade head` |
| Load completes but row count is 0 | Source file is empty or CRS mismatch caused all geometries to be dropped | Open the source file in QGIS to verify it contains data; check CRS |
| Coefficient load is very slow | ~5.4M polygons is expected to take several minutes | This is normal; use `--sample` for quick tests |
| `db-backup-tables` produces empty files | Container not running or DB name wrong | Confirm `docker compose up postgres` is running and `nrf-postgis` is the container name |
| `zcat: can't stat` on restore | Wrong path passed to `BACKUP_FILE` | Use the full or relative path, e.g. `make db-restore BACKUP_FILE=./backups/foo.sql.gz` |

---

## Related Documentation

- [environment.md](environment.md) — Application and database environment variables
- [local-testing.md](local-testing.md) — Running assessments locally after data is loaded