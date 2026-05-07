# Geospatial (PostGIS, GeoAlchemy, GeoPandas)

These rules extend the Python & FastAPI standards with geospatial-specific constraints.

## Database and SQLAlchemy

- Do not build SQL using unsafe string interpolation. Use SQLAlchemy expressions, parameters, or prepared statements.
- Always consider query cardinality and indexes when adding spatial filters.
- Do not load large result sets into memory unnecessarily. Use pagination, bounding boxes, limits, or streaming.
- Avoid `SELECT *` in raw SQL. Be explicit about selected columns.

```python
# Prefer
stmt = select(Site.id, Site.name, Site.geom).where(
    func.ST_Intersects(Site.geom, search_geom)
)
```

- Avoid committing inside low-level repository functions unless the project pattern explicitly does this.
- Transaction boundaries should usually live in the service layer or unit-of-work layer.

## GeoAlchemy and PostGIS

- Use GeoAlchemy types for geometry/geography columns.
- Always define SRID explicitly on geometry columns.

```python
geometry: Mapped[Any] = mapped_column(
    Geometry(geometry_type="MULTIPOLYGON", srid=27700, spatial_index=True)
)
```

- Use the correct geometry type: `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOLYGON`. Use `GEOMETRY` only when mixed types are genuinely required.
- Prefer `geometry` over `geography` unless distance/area calculations on spheroidal coordinates are required.
- Do not assume all geometries are valid. Use `ST_IsValid`, `ST_MakeValid`, or validation logic where needed.
- Be explicit about CRS transformations.

```python
func.ST_Transform(Model.geom, 4326)
```

- Do not compare geometries in different SRIDs.
- Do not silently transform geometries without documenting the source and target CRS.
- Use `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_DWithin`, and bounding-box operators appropriately.
- For performance, combine bounding-box prefilters with exact spatial predicates where helpful.

```sql
WITH query_bbox AS (
    SELECT ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 27700) AS geom
)
SELECT ...
FROM sites, query_bbox
WHERE sites.geom && query_bbox.geom
AND ST_Intersects(sites.geom, query_bbox.geom)
```

- Ensure spatial indexes exist for geometry columns used in spatial filters.

```sql
CREATE INDEX idx_sites_geom ON sites USING GIST (geom);
```

- Avoid applying `ST_Transform` to indexed columns inside high-volume WHERE clauses unless a matching functional index exists.
- Prefer transforming the query geometry to the table SRID before filtering.

## CRS rules

- Always know the CRS of every geometry. Use EPSG codes explicitly.
- Internal assessment and reference geometries in this project use EPSG:27700 unless a module explicitly documents another CRS.
- WKT/file inputs should default to EPSG:27700. Accept EPSG:4326 only when the API contract explicitly allows it, and transform immediately at the boundary before analysis.
- API GeoJSON output may use EPSG:4326 for client interoperability, but the output CRS decision must be explicit.
- For UK analysis, use EPSG:27700 where accurate area/distance calculations are required.
- For web maps and tiles, use EPSG:3857 where required.
- Do not calculate area, length, or distance in EPSG:4326 geometry coordinates. Transform to a projected CRS first.

```python
# Avoid area in degrees
gdf["area"] = gdf.geometry.area

# Prefer projected CRS
gdf = gdf.to_crs(27700)
gdf["area_m2"] = gdf.geometry.area
```

## GeoPandas rules

- Use GeoPandas for batch geospatial processing, not for high-volume request-time database work unless the result size is controlled.
- Do not load entire large tables into GeoPandas inside API requests.
- Always check CRS before spatial operations. Always set CRS when constructing a `GeoDataFrame`.
- Use `to_crs()` only when the current CRS is correctly set.
- Use vectorised operations instead of row-by-row loops. Avoid `iterrows()` for geospatial processing unless the dataset is very small.
- Use spatial indexes and `sjoin` for spatial joins.

```python
joined = geopandas.sjoin(points, polygons, predicate="within")
```

- Use Parquet/GeoParquet for intermediate geospatial data where practical. Avoid Shapefile unless required for interoperability.
- Keep heavy GeoPandas processing out of request handlers. Put it in services, background jobs, batch commands, or pipelines.

## Geometry serialization

- Be explicit when converting between WKB, WKT, GeoJSON, Shapely geometry, GeoAlchemy geometry, and PostGIS geometry.
- Do not hand-roll GeoJSON where library support is available.
- Ensure API geometry responses are valid GeoJSON. Keep coordinate order consistent.
- For database output as GeoJSON, prefer `ST_AsGeoJSON` when appropriate. Explicitly choose and document the output SRID before serializing; for public/client GeoJSON this is often EPSG:4326.

```python
ST_AsGeoJSON(ST_Transform(Model.geom, 4326))
```

- For Python-side serialization, use Shapely and Pydantic-compatible structures.

```python
mapping(shapely_geometry)
```

- Avoid returning huge GeoJSON payloads from APIs. Use limits, simplification, tiling, or pagination.

## Pydantic geometry schemas

- For geometry payloads, be explicit about the expected format: GeoJSON geometry, GeoJSON Feature, WKT, bounding box, tile coordinates, or lat/lon pair.

```python
class BoundingBox(BaseModel):
    minx: float
    miny: float
    maxx: float
    maxy: float
    srid: int = 4326
```

- Validate coordinate order clearly. For WGS84 coordinates, use longitude, latitude order.

```txt
Correct: [longitude, latitude]
Avoid: [latitude, longitude]
```

- Validate bounding boxes: `minx < maxx`, `miny < maxy`, coordinates are within expected range, SRID is supported.

```python
if bbox.minx >= bbox.maxx:
    raise ValueError("bbox.minx must be less than bbox.maxx")
```

## API performance for geospatial endpoints

- Always apply limits to endpoints that return features. Avoid returning unbounded collections.
- Use bounding boxes, tile coordinates, or filters for map endpoints.
- Consider geometry simplification for low zoom levels.
- Use database-side spatial filtering before loading results into Python. Avoid N+1 database queries.
- Add indexes for common filters: spatial indexes on geometry, B-tree indexes on IDs/status/category/date fields, composite indexes where justified by query patterns.

## Alembic migrations

- Every database schema change must include an Alembic migration. Migrations must be deterministic and reviewable.
- Review autogenerated migrations before committing. Ensure geometry columns, SRIDs, indexes, and constraints are correct.
- Include spatial indexes in migrations. Avoid destructive migrations unless explicitly required.
- For large tables, consider migration runtime and locking behaviour.

```python
op.create_index(
    "idx_sites_geom",
    "sites",
    ["geom"],
    postgresql_using="gist",
)
```

## Testing

### Database integration tests

- Use a real PostGIS database for spatial query tests. Do not rely on SQLite for PostGIS-specific behaviour.
- Test important spatial predicates against known geometries, SRID handling, and invalid geometry handling.

```txt
Example cases:
- point within polygon
- polygon intersects bbox
- geometry outside bbox is excluded
- mismatched SRID is handled correctly
- invalid geometry is rejected or repaired
```

### Geospatial test fixtures

- Keep geometry fixtures small and readable. Prefer WKT or simple GeoJSON for test geometries.
- Always include CRS/SRID in fixtures. Store reusable fixtures in a shared test utility module.

```python
SIMPLE_POLYGON_WKT = (
    "POLYGON((-1 51, -1 52, 0 52, 0 51, -1 51))"
)
```

### Mocking

- Use real PostGIS for spatial database behaviour. Avoid mocking SQLAlchemy query internals.
- Prefer repository integration tests for complex SQL/spatial queries.
- Mock external HTTP APIs and file/object storage only when the test is not about that behaviour.

## Security

- Validate and constrain user-provided table names, layer names, column names, CQL/filter expressions, file paths, CRS values, and bbox values.
- Do not allow arbitrary SQL from API input. Do not allow arbitrary file reads from user-provided paths.
- Enforce maximum request sizes for geometry uploads and maximum result sizes for feature APIs.

## File handling

- Use temporary directories safely. Clean up temporary files.
- Do not assume uploaded files are valid. Validate file type and size.
- Avoid loading very large geospatial files entirely into memory. Prefer streaming or chunked processing.
- Use GeoPackage, GeoParquet, or FlatGeobuf where appropriate. Avoid Shapefile for internal processing unless required.

## Background jobs and long-running tasks

- Do not run heavy geospatial processing directly inside HTTP request handlers.
- Use a background job system or batch command for: large spatial joins, raster/vector conversion, file imports, tile generation, large GeoPandas workflows, multi-step enrichment pipelines.
- Return job IDs or processing status for long-running operations.

## Logging

Include geospatial context in log entries where relevant:

- SRID
- bounding box or tile coordinate (where safe)
- dataset/layer name
- feature count
- processing duration for spatial joins, CRS transformations, and large serialization steps

Do not log large geometry payloads or full GeoJSON responses.

## Documentation and comments

Use docstrings for complex geospatial helpers and non-obvious CRS transformations. Document assumptions: CRS, geometry type, units, coordinate order, simplification tolerance, maximum expected dataset size.

```python
def calculate_area_m2(gdf: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """
    Calculates polygon area in square metres.

    Input geometries are expected to be in EPSG:4326 and are transformed to
    EPSG:27700 before area calculation.
    """
```

## AI-generated code rules

- When generating SQL or migrations, include spatial indexes and SRID definitions where needed.
- When generating geospatial code, explicitly state CRS assumptions.
- Do not silently change coordinate order or geometry type.
- Do not introduce request-time GeoPandas processing for large datasets.
- Prefer database-side spatial operations for API filtering.

## Common mistakes to avoid

- Calculating area or distance in EPSG:4326.
- Forgetting SRID on geometry columns.
- Mixing latitude/longitude and longitude/latitude order.
- Loading an entire PostGIS table into memory.
- Forgetting GIST indexes on spatial columns.
- Applying `ST_Transform` to indexed columns in high-volume queries.
- Returning huge GeoJSON responses without limits.
- Over-mocking database/spatial behaviour in tests.
- Trusting uploaded geospatial files without validation.
