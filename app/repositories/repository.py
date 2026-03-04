"""Repository for querying PostGIS spatial and lookup data.

This module provides a unified interface for querying spatial layers and lookup
tables stored in PostGIS using SQLAlchemy 2.x query builder patterns.
"""

import logging
import time
from typing import Any

import geopandas as gpd
import pandas as pd
from sqlalchemy import Select, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.models.db import Base
from app.models.enums import SpatialLayerType

logger = logging.getLogger(__name__)


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

            filter_str = str(
                overlay_filter.compile(
                    dialect=session.bind.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
            attr_str = str(
                overlay_attr.compile(
                    dialect=session.bind.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )

            filter_sql = filter_str.replace(f"{qualified}.", "t.")
            attr_sql = attr_str.replace(f"{qualified}.", "t.")

            raw_sql = text(f"""
                SELECT i.input_id, best.attr_val
                FROM _tmp_input_geom i
                LEFT JOIN LATERAL (
                    SELECT {attr_sql} AS attr_val
                    FROM {qualified} t
                    WHERE {filter_sql}
                      AND ST_Intersects(t.geometry, i.geom)
                    ORDER BY ST_Area(ST_Intersection(t.geometry, i.geom)) DESC
                    LIMIT 1
                ) best ON true
            """)

            rows = session.execute(raw_sql).fetchall()

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

                filter_str = str(
                    overlay_filter.compile(
                        dialect=session.bind.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
                attr_str = str(
                    overlay_attr.compile(
                        dialect=session.bind.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )

                filter_sql = filter_str.replace(f"{qualified}.", "t.")
                attr_sql = attr_str.replace(f"{qualified}.", "t.")

                lateral_clauses.append(
                    f"LEFT JOIN LATERAL ("
                    f"  SELECT {attr_sql} AS attr_val"
                    f"  FROM {qualified} t"
                    f"  WHERE {filter_sql}"
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

            rows = session.execute(combined_sql).fetchall()

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

            raw_sql = text("""
                SELECT rlb_id, dwellings, name, dwelling_category, source,
                       crome_id, lu_curr_n_coeff, lu_curr_p_coeff,
                       n_resi_coeff, p_resi_coeff, n2k_site_n,
                       ST_Area(isect_geom) / 10000.0 AS area_in_nn_catchment_ha
                FROM (
                    SELECT
                        r.rlb_id, r.dwellings, r.name, r.dwelling_category, r.source,
                        c.crome_id, c.lu_curr_n_coeff, c.lu_curr_p_coeff,
                        c.n_resi_coeff, c.p_resi_coeff,
                        nn.attributes->>'N2K_Site_N' AS n2k_site_n,
                        ST_Intersection(ST_Intersection(r.geom, c.geometry), nn.geometry)
                            AS isect_geom
                    FROM _tmp_rlb r
                    JOIN nrf_reference.coefficient_layer c
                        ON c.version = :coeff_version
                        AND ST_Intersects(r.geom, c.geometry)
                    JOIN nrf_reference.spatial_layer nn
                        ON nn.layer_type = CAST(:nn_layer_type AS nrf_reference.spatial_layer_type)
                        AND nn.version = :nn_version
                        AND ST_Intersects(r.geom, nn.geometry)
                        AND ST_Intersects(c.geometry, nn.geometry)
                ) sub
                WHERE ST_Area(isect_geom) > 0
            """)

            rows = session.execute(
                raw_sql,
                {
                    "coeff_version": coeff_version,
                    "nn_version": nn_version,
                    "nn_layer_type": SpatialLayerType.NN_CATCHMENTS.name,
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
            "area_in_nn_catchment_ha",
        ]
        return pd.DataFrame(rows, columns=columns)

    def intersection_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        overlay_table: type[Base],
        overlay_filter: Any,
        overlay_columns: list[str],
    ) -> gpd.GeoDataFrame:
        """Perform spatial intersection using PostGIS server-side."""
        from geoalchemy2.functions import (
            ST_GeomFromText,
            ST_Intersection,
            ST_Intersects,
            ST_SetSRID,
        )

        input_union = input_gdf.union_all()
        input_wkt = input_union.wkt

        overlay_cols = [getattr(overlay_table, col) for col in overlay_columns]

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

        return gpd.read_postgis(
            stmt, self.engine, geom_col="geometry", crs="EPSG:27700"
        )

    def close(self) -> None:
        """Close the repository and dispose of the engine."""
        self.engine.dispose()

    def __enter__(self) -> "Repository":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
