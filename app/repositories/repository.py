"""Repository for querying PostGIS spatial and lookup data.

This module provides a unified interface for querying spatial layers and lookup
tables stored in PostGIS using SQLAlchemy 2.x query builder patterns.
"""

import hashlib
import logging
import re
import time
from enum import Enum as PyEnum
from typing import Any

import geopandas as gpd
import pandas as pd
from cachetools import TTLCache
from sqlalchemy import Select, func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import SpatialCacheConfig
from app.models.db import Base, DataLoadHistory

_cache_cfg = SpatialCacheConfig()
_land_use_cache: TTLCache = TTLCache(
    maxsize=_cache_cfg.max_size, ttl=_cache_cfg.ttl_seconds
)
_intersection_cache: TTLCache = TTLCache(
    maxsize=_cache_cfg.max_size, ttl=_cache_cfg.ttl_seconds
)

logger = logging.getLogger(__name__)


def clear_spatial_caches() -> None:
    """Drop all cached spatial query results.

    The caches key on input geometry and (for land use) the resolved data
    version, but not on the underlying table contents. A data-sync reload can
    replace reference data without bumping the version, leaving cached results
    stale until their TTL expires. Call this after a successful reload so the
    next query re-reads from the database.
    """
    _land_use_cache.clear()
    _intersection_cache.clear()
    logger.info("Cleared spatial query caches")


def _coerce_param(value: Any) -> Any:
    """Convert Python Enum instances to their .name string for psycopg2.

    Compiled SQLAlchemy expressions store raw Python Enum objects in .params.
    When passed via text() those bypass SQLAlchemy's TypeEngine, so psycopg2
    receives the object directly and can't adapt it. The PostgreSQL ENUM type
    uses UPPERCASE names (e.g. 'WWTW_CATCHMENTS'), matching .name.
    """
    return value.name if isinstance(value, PyEnum) else value


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_QUALIFIED_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _assert_safe_identifier(value: str, label: str) -> None:
    """Raise ValueError if value is not a safe SQL identifier.

    Prevents SQL injection via caller-supplied field names interpolated
    into text() queries. Allows only alphanumeric characters and underscores,
    starting with a letter or underscore.
    """
    if not _SAFE_IDENTIFIER_RE.fullmatch(value):
        msg = f"Unsafe SQL identifier for {label!r}: {value!r}. Only letters, digits, and underscores are permitted."
        raise ValueError(msg)


def _assert_safe_qualified(value: str, label: str) -> None:
    """Raise ValueError if value is not a safe schema.table qualified name."""
    if not _SAFE_QUALIFIED_RE.fullmatch(value):
        msg = f"Unsafe qualified table name for {label!r}: {value!r}. Expected format: schema.table with only safe identifier characters."
        raise ValueError(msg)


def _sa_params(compiled_sql: str, params: dict, prefix: str = "") -> tuple[str, dict]:
    """Convert compiled %(name)s SQL to SQLAlchemy :name style with optional prefix.

    Returns the rewritten SQL and a dict of coerced parameter values.
    """
    renamed: dict[str, Any] = {}

    def repl(m: re.Match) -> str:
        new_name = f"{prefix}{m.group(1)}" if prefix else m.group(1)
        renamed[new_name] = _coerce_param(params[m.group(1)])
        return f":{new_name}"

    return re.sub(r"%\((\w+)\)s", repl, compiled_sql), renamed


def _gdf_key(gdf: gpd.GeoDataFrame, cols: list[str]) -> str:
    """Stable hash of selected GeoDataFrame columns + geometry WKT."""
    rows = tuple(
        tuple(str(row[c]) for c in cols) + (wkt,)
        for row, wkt in zip(gdf.to_dict("records"), gdf.geometry.to_wkt(), strict=False)
    )
    return hashlib.sha256(str(rows).encode()).hexdigest()


def _spatial_cache_generation(session: Session) -> str:
    """Return DB-visible generation for reference data used by spatial caches."""
    loaded_at = session.scalar(
        select(func.max(DataLoadHistory.loaded_at)).where(
            DataLoadHistory.status == "success"
        )
    )
    return loaded_at.isoformat() if loaded_at else "no-successful-data-load"


def _land_use_cache_key(
    input_gdf: gpd.GeoDataFrame,
    *,
    coeff_version: int,
    nn_version: int,
    generation: str,
) -> tuple[str, int, int, str]:
    return (
        _gdf_key(
            input_gdf,
            ["rlb_id", "dwellings", "name", "dwelling_category", "source"],
        ),
        coeff_version,
        nn_version,
        generation,
    )


def _intersection_cache_key(
    *,
    input_wkt: str,
    overlay_table: type[Base],
    filter_str: str,
    overlay_columns: list[str],
    json_extracts: dict[str, list[str]] | None,
    generation: str,
) -> str:
    return hashlib.sha256(
        "|".join(
            [
                input_wkt,
                overlay_table.__tablename__,
                filter_str,
                str(sorted(overlay_columns)),
                str(sorted(json_extracts.items()) if json_extracts else []),
                generation,
            ]
        ).encode()
    ).hexdigest()


class Repository:
    """Repository for accessing spatial reference data and lookup tables."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    def session(self) -> Session:
        """Create a new SQLAlchemy session."""
        return self._session_factory()

    def execute_query(
        self, stmt: Select, as_gdf: bool = False
    ) -> gpd.GeoDataFrame | list[Base]:
        """Execute a SQLAlchemy SELECT statement."""
        with self.session() as session:
            if as_gdf:
                return gpd.read_postgis(
                    stmt, session.connection(), geom_col="geometry", crs="EPSG:27700"
                )
            result = session.scalars(stmt).all()
            return list(result)

    def majority_overlap_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        overlay_table: type[Base],
        overlay_filter: Any,
        input_id_col: str,
        overlay_attr_col: Any,
        output_field: str,
        default_value: Any = None,
    ) -> pd.DataFrame:
        """Perform majority overlap assignment using PostGIS server-side."""
        if len(input_gdf) == 0:
            return pd.DataFrame(columns=[input_id_col, output_field])

        if isinstance(overlay_attr_col, str):
            overlay_attr = getattr(overlay_table, overlay_attr_col)
        else:
            overlay_attr = overlay_attr_col

        with self.session() as session:
            session.execute(
                text(
                    "CREATE TEMPORARY TABLE _tmp_input_geom ("
                    "  input_id integer, "
                    "  geom geometry(Geometry, 27700)"
                    ") ON COMMIT DROP"
                )
            )

            insert_values = [
                {"input_id": int(input_id), "geom_wkt": wkt}
                for input_id, wkt in zip(
                    input_gdf[input_id_col], input_gdf.geometry.to_wkt(), strict=False
                )
            ]
            session.execute(
                text(
                    "INSERT INTO _tmp_input_geom (input_id, geom) "
                    "VALUES (:input_id, ST_SetSRID(ST_GeomFromText(:geom_wkt), 27700))"
                ),
                insert_values,
            )

            session.execute(text("CREATE INDEX ON _tmp_input_geom USING GIST (geom)"))

            table = overlay_table.__table__
            schema = table.schema
            table_name = table.name
            qualified = f"{schema}.{table_name}" if schema else table_name

            compiled_filter = overlay_filter.compile(dialect=session.bind.dialect)
            compiled_attr = overlay_attr.compile(dialect=session.bind.dialect)

            filter_str, filter_params = _sa_params(
                str(compiled_filter), compiled_filter.params
            )
            attr_str, attr_params = _sa_params(str(compiled_attr), compiled_attr.params)

            sql_params = {**filter_params, **attr_params}

            _assert_safe_identifier(output_field, "output_field")
            _assert_safe_qualified(qualified, "qualified")

            filter_str = filter_str.replace(f"{qualified}.", "t.")
            attr_str = attr_str.replace(f"{qualified}.", "t.")

            raw_sql = text(f"""
                SELECT i.input_id, best.attr_val
                FROM _tmp_input_geom i
                LEFT JOIN LATERAL (
                    SELECT {attr_str} AS attr_val
                    FROM {qualified} t
                    WHERE {filter_str}
                      AND ST_Intersects(t.geometry, i.geom)
                    ORDER BY ST_Area(ST_Intersection(t.geometry, i.geom)) DESC
                    LIMIT 1
                ) best ON true
            """)

            rows = session.execute(raw_sql, sql_params).fetchall()

        df = pd.DataFrame(rows, columns=[input_id_col, output_field])

        if default_value is not None:
            df[output_field] = df[output_field].fillna(default_value)

        return df

    def batch_majority_overlap_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        input_id_col: str,
        assignments: list[dict[str, Any]],
    ) -> dict[str, pd.DataFrame]:
        """Perform multiple majority overlap assignments in a single SQL query."""
        if len(input_gdf) == 0:
            return {
                a["output_field"]: pd.DataFrame(
                    columns=[input_id_col, a["output_field"]]
                )
                for a in assignments
            }

        with self.session() as session:
            t0 = time.perf_counter()

            session.execute(
                text(
                    "CREATE TEMPORARY TABLE _tmp_input_geom ("
                    "  input_id integer, "
                    "  geom geometry(Geometry, 27700)"
                    ") ON COMMIT DROP"
                )
            )

            insert_values = [
                {"input_id": int(input_id), "geom_wkt": wkt}
                for input_id, wkt in zip(
                    input_gdf[input_id_col], input_gdf.geometry.to_wkt(), strict=False
                )
            ]
            session.execute(
                text(
                    "INSERT INTO _tmp_input_geom (input_id, geom) "
                    "VALUES (:input_id, ST_SetSRID(ST_GeomFromText(:geom_wkt), 27700))"
                ),
                insert_values,
            )

            session.execute(text("CREATE INDEX ON _tmp_input_geom USING GIST (geom)"))
            session.execute(text("ANALYZE _tmp_input_geom"))

            t_setup = time.perf_counter() - t0
            logger.info(
                f"[timing] batch_majority_overlap: temp table setup "
                f"({len(input_gdf)} features): {t_setup:.3f}s"
            )

            t_query = time.perf_counter()

            select_cols = ["i.input_id"]
            lateral_clauses = []
            all_params: dict[str, Any] = {}

            for idx, assignment in enumerate(assignments):
                overlay_table = assignment["overlay_table"]
                overlay_filter = assignment["overlay_filter"]
                overlay_attr_col = assignment["overlay_attr_col"]
                output_field = assignment["output_field"]
                alias = f"lat_{idx}"

                if isinstance(overlay_attr_col, str):
                    overlay_attr = getattr(overlay_table, overlay_attr_col)
                else:
                    overlay_attr = overlay_attr_col

                table = overlay_table.__table__
                schema = table.schema
                table_name = table.name
                qualified = f"{schema}.{table_name}" if schema else table_name

                # Compile without literal_binds — JSONB subscripts and some enum
                # types don't support it. Prefix per lateral to avoid collisions
                # (all assignments compile to the same param names).
                compiled_filter = overlay_filter.compile(dialect=session.bind.dialect)
                compiled_attr = overlay_attr.compile(dialect=session.bind.dialect)

                prefix = f"lat{idx}_"

                filter_str, filter_renamed = _sa_params(
                    str(compiled_filter), compiled_filter.params, prefix
                )
                attr_str, attr_renamed = _sa_params(
                    str(compiled_attr), compiled_attr.params, prefix
                )

                _assert_safe_identifier(output_field, "output_field")
                _assert_safe_qualified(qualified, "qualified")

                filter_str = filter_str.replace(f"{qualified}.", "t.")
                attr_str = attr_str.replace(f"{qualified}.", "t.")

                all_params.update(filter_renamed)
                all_params.update(attr_renamed)

                lateral_clauses.append(
                    f"LEFT JOIN LATERAL ("
                    f"  SELECT {attr_str} AS attr_val"
                    f"  FROM {qualified} t"
                    f"  WHERE {filter_str}"
                    f"    AND ST_Intersects(t.geometry, i.geom)"
                    f"  ORDER BY ST_Area(ST_Intersection(t.geometry, i.geom)) DESC"
                    f"  LIMIT 1"
                    f") {alias} ON true"
                )
                select_cols.append(f"{alias}.attr_val AS {output_field}")

            combined_sql = text(
                f"SELECT {', '.join(select_cols)} "
                f"FROM _tmp_input_geom i " + " ".join(lateral_clauses)
            )

            rows = session.execute(combined_sql, all_params).fetchall()

            logger.info(
                f"[timing] batch_majority_overlap: combined query "
                f"({len(assignments)} laterals): {time.perf_counter() - t_query:.3f}s"
            )

        output_fields = [a["output_field"] for a in assignments]
        all_columns = [input_id_col, *output_fields]
        combined_df = pd.DataFrame(rows, columns=all_columns)

        results = {}
        for assignment in assignments:
            output_field = assignment["output_field"]
            default_value = assignment.get("default_value")

            df = combined_df[[input_id_col, output_field]].copy()
            if default_value is not None:
                df[output_field] = df[output_field].fillna(default_value)
            results[output_field] = df

        return results

    def land_use_intersection_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        coeff_version: int,
        nn_version: int,
    ) -> pd.DataFrame:
        """Perform 3-way spatial intersection (RLB x coefficient x NN catchment) in PostGIS."""
        if len(input_gdf) == 0:
            return pd.DataFrame(
                columns=[
                    "rlb_id",
                    "dwellings",
                    "name",
                    "dwelling_category",
                    "source",
                    "crome_id",
                    "lu_curr_n_coeff",
                    "lu_curr_p_coeff",
                    "n_resi_coeff",
                    "p_resi_coeff",
                    "n2k_site_n",
                    "area_in_nn_catchment_ha",
                ]
            )

        with self.session() as session:
            generation = _spatial_cache_generation(session)
            cache_key = _land_use_cache_key(
                input_gdf,
                coeff_version=coeff_version,
                nn_version=nn_version,
                generation=generation,
            )
            if cache_key in _land_use_cache:
                logger.debug("land_use_intersection_postgis cache hit")
                return _land_use_cache[cache_key].copy()

            session.execute(
                text(
                    "CREATE TEMPORARY TABLE _tmp_rlb ("
                    "  rlb_id integer, "
                    "  dwellings integer, "
                    "  name text, "
                    "  dwelling_category text, "
                    "  source text, "
                    "  geom geometry(Geometry, 27700)"
                    ") ON COMMIT DROP"
                )
            )

            wkt_values = input_gdf.geometry.to_wkt().tolist()
            records = input_gdf[
                ["rlb_id", "dwellings", "name", "dwelling_category", "source"]
            ].to_dict("records")
            insert_values = [
                {
                    "rlb_id": int(rec["rlb_id"]),
                    "dwellings": int(rec["dwellings"]),
                    "name": str(rec["name"]),
                    "dwelling_category": str(rec["dwelling_category"]),
                    "source": str(rec["source"]),
                    "geom_wkt": wkt_values[i],
                }
                for i, rec in enumerate(records)
            ]
            session.execute(
                text(
                    "INSERT INTO _tmp_rlb "
                    "(rlb_id, dwellings, name, dwelling_category, source, geom) "
                    "VALUES (:rlb_id, :dwellings, :name, :dwelling_category, :source, "
                    "ST_SetSRID(ST_GeomFromText(:geom_wkt), 27700))"
                ),
                insert_values,
            )

            session.execute(text("CREATE INDEX ON _tmp_rlb USING GIST (geom)"))
            session.execute(text("ANALYZE _tmp_rlb"))

            # ST_Area is computed directly in the subquery so only a float is
            # returned up the stack — no intermediate geometry object is
            # materialised and then discarded by the outer ST_Area call.
            raw_sql = text("""
                SELECT rlb_id, dwellings, name, dwelling_category, source,
                       crome_id, lu_curr_n_coeff, lu_curr_p_coeff,
                       n_resi_coeff, p_resi_coeff, n2k_site_n, oid,
                       area_in_nn_catchment_ha
                FROM (
                    SELECT
                        r.rlb_id, r.dwellings, r.name, r.dwelling_category, r.source,
                        c.crome_id, c.lu_curr_n_coeff, c.lu_curr_p_coeff,
                        c.n_resi_coeff, c.p_resi_coeff,
                        nn.attributes->>'N2K_Site_N' AS n2k_site_n,
                        nn.attributes->>'OID' AS oid,
                        ST_Area(
                            ST_Intersection(ST_Intersection(r.geom, c.geometry), nn.geometry)
                        ) / 10000.0 AS area_in_nn_catchment_ha
                    FROM _tmp_rlb r
                    JOIN public.coefficient_layer c
                        ON c.version = :coeff_version
                        AND ST_Intersects(r.geom, c.geometry)
                    JOIN public.nn_catchments nn
                        ON nn.version = :nn_version
                        AND ST_Intersects(r.geom, nn.geometry)
                        AND ST_Intersects(c.geometry, nn.geometry)
                ) sub
                WHERE area_in_nn_catchment_ha > 0
            """)

            rows = session.execute(
                raw_sql,
                {
                    "coeff_version": coeff_version,
                    "nn_version": nn_version,
                },
            ).fetchall()

        columns = [
            "rlb_id",
            "dwellings",
            "name",
            "dwelling_category",
            "source",
            "crome_id",
            "lu_curr_n_coeff",
            "lu_curr_p_coeff",
            "n_resi_coeff",
            "p_resi_coeff",
            "n2k_site_n",
            "oid",
            "area_in_nn_catchment_ha",
        ]
        result = pd.DataFrame(rows, columns=columns)
        _land_use_cache[cache_key] = result
        return result.copy()

    def intersection_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        overlay_table: type[Base],
        overlay_filter: Any,
        overlay_columns: list[str],
        json_extracts: dict[str, list[str]] | None = None,
    ) -> gpd.GeoDataFrame:
        """Perform spatial intersection using PostGIS server-side.

        Args:
            json_extracts: Optional dict mapping a JSONB column name to a list of
                keys to extract server-side, e.g. ``{"attributes": ["RZ", "NAME"]}``.
                Each key is added as a column with the key name as alias, avoiding
                a Python-side per-row ``apply`` on the full JSONB blob.
        """
        from geoalchemy2.functions import (
            ST_GeomFromText,
            ST_Intersection,
            ST_Intersects,
            ST_SetSRID,
        )

        input_union = input_gdf.union_all()
        input_wkt = input_union.wkt

        filter_str = str(overlay_filter.compile(compile_kwargs={"literal_binds": True}))
        with self.session() as session:
            generation = _spatial_cache_generation(session)
        cache_key = _intersection_cache_key(
            input_wkt=input_wkt,
            overlay_table=overlay_table,
            filter_str=filter_str,
            overlay_columns=overlay_columns,
            json_extracts=json_extracts,
            generation=generation,
        )
        if cache_key in _intersection_cache:
            logger.debug("intersection_postgis cache hit")
            return _intersection_cache[cache_key].copy()

        overlay_cols = [getattr(overlay_table, col) for col in overlay_columns]

        if json_extracts:
            for col_name, keys in json_extracts.items():
                col = getattr(overlay_table, col_name)
                for key in keys:
                    overlay_cols.append(col[key].astext.label(key))

        stmt = select(
            *overlay_cols,
            ST_Intersection(
                overlay_table.geometry,
                ST_SetSRID(ST_GeomFromText(input_wkt), 27700),
            ).label("geometry"),
        ).where(
            overlay_filter,
            ST_Intersects(
                overlay_table.geometry,
                ST_SetSRID(ST_GeomFromText(input_wkt), 27700),
            ),
        )

        result = gpd.read_postgis(
            stmt, self.engine, geom_col="geometry", crs="EPSG:27700"
        )
        _intersection_cache[cache_key] = result
        return result.copy()

    def close(self) -> None:
        """Close the repository and dispose of the engine."""
        self.engine.dispose()

    def __enter__(self) -> Repository:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
