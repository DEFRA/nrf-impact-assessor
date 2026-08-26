# Reference database ERD

Entity-relationship diagram of the impact-assessor **`nrf_impact`** Postgres
database — the spatial reference layers used for impact assessment, the JSONB
lookup tables, and the audit trail for the S3-driven data sync that loads them.

- **Source:** live `nrf_impact` Postgres (`docker compose` service `postgres`),
  schema `public`, cross-checked against the Alembic revisions under
  `alembic/versions/`.
- **Generated:** 2026-08-13, by the `generate-db-diagram` skill.
- **Scope:** application domain tables only. `alembic_version` and PostGIS
  internals (`spatial_ref_sys`, `geometry_columns`, `geography_columns`) are
  excluded.

This database has **no foreign keys to `nrf_backend`** — the two databases share
a Postgres instance but are otherwise independent. The services talk over HTTP
and SNS/SQS.

Geometry columns are **SRID 27700** (British National Grid) with GiST indexes.
Reference data is **versioned rather than replaced**: a sync loads a new
`version` alongside the old, and `data_active_version` decides which one reads
should use.

## Data management

The only foreign key in this database is `data_load_history.run_id`.

```mermaid
erDiagram
    data_sync_run ||--o{ data_load_history : "audits"

    data_sync_run {
        uuid id PK "app-generated uuid4, no DB default"
        varchar status "running / success / failed"
        varchar data_version "nullable"
        boolean forced "default false"
        timestamptz started_at "default now()"
        timestamptz finished_at "nullable"
        varchar error "nullable"
    }

    data_load_history {
        uuid id PK "app-generated uuid4, no DB default"
        uuid run_id FK "no ON DELETE action"
        varchar table_name "indexed with loaded_at"
        varchar s3_key
        varchar etag
        varchar data_version "nullable"
        integer row_version "nullable, joins the table's version column"
        varchar status "success / reconciled / failed"
        varchar status_detail "nullable"
        timestamptz loaded_at "default now()"
    }

    data_active_version {
        varchar table_name PK "names a reference table, not a FK"
        integer active_version
        timestamptz updated_at "default now()"
    }

    data_rollback_event {
        uuid id PK "app-generated uuid4, no DB default"
        varchar table_name
        integer from_version
        integer to_version
        timestamptz rolled_back_at "default now()"
    }

    lookup_table {
        uuid id PK "app-generated uuid4, no DB default"
        varchar name "unique with version, indexed"
        integer version "unique with name, indexed"
        jsonb data "array of row objects"
        jsonb schema "nullable, column types"
        varchar description "nullable"
        varchar source "nullable"
        varchar license "nullable"
        timestamptz created_at "default now()"
    }
```

Reads fall back to `MAX(version)` when `data_active_version` holds no row for a
table, so it only gains one once a reload or rollback has actually run.

## Spatial reference layers

`coefficient_layer` has its own column set — the per-parcel nutrient
coefficients the levy calculation multiplies through, and by far the largest
table (~5.4M polygons).

```mermaid
erDiagram
    coefficient_layer {
        uuid id PK "app-generated uuid4, no DB default"
        integer version "indexed, no DB default"
        geometry geometry "MultiPolygon, SRID 27700, GiST"
        varchar crome_id "nullable, indexed"
        varchar land_use_cat "nullable"
        varchar nn_catchment "nullable, indexed"
        varchar subcatchment "nullable, indexed"
        double lu_curr_n_coeff "nullable, current land use N"
        double lu_curr_p_coeff "nullable, current land use P"
        double n_resi_coeff "nullable, residential N"
        double p_resi_coeff "nullable, residential P"
        timestamptz created_at "default now()"
    }
```

The remaining **nine** layers share one identical column set. It is drawn once,
on `wwtw_catchments`:

```mermaid
erDiagram
    wwtw_catchments {
        uuid id PK "app-generated uuid4, no DB default"
        integer version "indexed, no DB default"
        geometry geometry "SRID 27700, GiST"
        varchar name "nullable, indexed"
        jsonb attributes "nullable, source attributes verbatim"
        timestamptz created_at "default now()"
    }
```

| Table | Holds |
| --- | --- |
| `wwtw_catchments` | Wastewater treatment works catchment polygons |
| `lpa_boundaries` | Local planning authority boundaries |
| `nn_catchments` | Nutrient neutrality catchment polygons |
| `subcatchments` | Sub-catchment polygons |
| `gcn_risk_zones` | Great crested newt risk zones (red/amber/green) |
| `gcn_ponds` | National ponds dataset, for GCN assessment |
| `edp_edges` | Environmental designation polygon edges, for GCN assessment |
| `edp_boundary_layer` | EDP boundary polygons |
| `edp_excluded_areas` | Buffered SSSI exclusion polygons (nutrient EDP) |

These carry no foreign keys — they are independent reference layers, joined
spatially at query time.

## Constraints and indexes worth knowing

| Name | Rule |
| --- | --- |
| `uq_data_sync_run_single_running` | Partial `UNIQUE (status) WHERE status = 'running'` — enforces a single in-flight sync. |
| `uq_lookup_name_version` | `UNIQUE (name, version)` — one row per lookup table per version. |
| `ix_public_coefficient_layer_geom_v1` | Partial GiST over `geometry WHERE version = 1`. **Add an equivalent when loading a new version**, or queries against it lose the index. |
| `ix_data_load_history_table_loaded_at` | `(table_name, loaded_at)` — the provenance lookup. |
| `ix_public_<layer>_geometry` | GiST on every spatial layer. |

## UUID primary keys have no database default

Every `uuid` primary key here is `default=uuid4` in SQLAlchemy — applied in
**Python**, not by Postgres (see `app/models/db.py`). `information_schema`
therefore reports no default, and a raw `INSERT` that omits the id will fail.
The same applies to `version`, which the models default to `1`.

## Two migration definitions

The schema is defined twice: **Alembic** (`alembic/versions/`, run locally) and a
parallel **Liquibase** changelog (`changelog/`, applied on the deployed
platform). `scripts/check_migration_parity.py` enforces that every Alembic
revision has a matching Liquibase changeset. The live database reflects whichever
ran — this diagram is the same either way.
