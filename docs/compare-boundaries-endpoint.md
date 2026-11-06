# Compare boundaries endpoint

`POST /compare-boundaries` reports whether two red line boundaries cover substantially the same land. It is read-only and makes no database calls, so it needs no EDP data. Code: `app/boundary/router.py`, with boundary reading and validation shared with `/check-boundary` in `app/boundary/loading.py`.

## Request

```
POST /compare-boundaries
x-api-key: <IMPACT_ASSESSOR_API_KEY>
Content-Type: application/json

{
  "boundaryOriginal": <GeoJSON>,
  "boundaryNew": <GeoJSON>
}
```

Both fields are required and unknown keys are rejected, though not named in the error, as the name is logged and could hold anything. Each boundary can be a bare geometry, a Feature or a FeatureCollection holding one polygon, so a 2D geometry returned by `/check-boundary` can be passed straight in.

Coordinates are 2D, and WGS84 unless the GeoJSON declares EPSG:27700 in a `crs` member. The two boundaries can use different coordinate systems.

The body is limited to twice `max_upload_bytes` (4 MB by default).

## Response

```json
{ "boundariesMatch": true }
```

Boundaries match when at least 95% of their combined area is common to both (intersection ÷ union, `>= 0.95`, measured in EPSG:27700). The result is the same whichever boundary is passed first. Only the verdict is returned; the ratio is in the logs.

What that allows on a 1 ha square site:

| Change | Ratio | Match |
| --- | --- | --- |
| Grows by 5% | 95.2% | Yes |
| Shrinks by 5% | 95.0% | Yes |
| Moves 2 m sideways (2%) | 96.1% | Yes |
| Moves 3 m sideways (3%) | 94.2% | No |
| Grows by 6% | 94.3% | No |

The tolerance is a share of the site, so a larger site can gain more land and still match.

## Errors

Errors use the standard FastAPI `detail` member:

```json
{
  "detail": {
    "error": "geometry_has_holes",
    "boundary": "boundaryNew",
    "message": "boundaryNew must not contain holes"
  }
}
```

Branch on `error`; `message` is for logs and developers and may be reworded. `boundary` names the boundary at fault, or is `null` when the fault is with the request as a whole. `boundaryOriginal` is checked first, and only the first problem is reported. Codes shared with `/check-boundary` mean the same thing there.

| Status | `error` | Meaning |
| --- | --- | --- |
| 400 | `invalid_request` | Body is not JSON, or a field is missing, unknown or not an object |
| 400 | `unreadable_geometry_file` | Boundary cannot be read as GeoJSON |
| 400 | `no_polygon_found` | No features, or an empty polygon |
| 400 | `multiple_features` | More than one feature; comparing only the first could report a false match |
| 400 | `invalid_geometry` | Feature with a missing geometry |
| 400 | `unsupported_geometry_type` | Anything but a single Polygon, including MultiPolygon |
| 400 | `unclosed_ring` | Outer ring does not end on its first point |
| 400 | `self_intersecting_geometry` | Edges cross, or the polygon has collapsed to a line |
| 400 | `geometry_has_holes` | Polygon has interior rings |
| 400 | `duplicate_vertices` | The same point appears twice in a row |
| 400 | `coordinates_out_of_range` | Coordinates fall outside Great Britain |
| 413 | `request_too_large` | Body is over the limit |
| 422 | `unsupported_crs` | Boundary declares a CRS other than EPSG:4326 or EPSG:27700, or its coordinates carry altitude |

A missing or wrong `x-api-key` returns 401 from the shared auth dependency.

## Logging

No log line carries coordinates or GeoJSON.

| Event | Level | Logger | Content |
| --- | --- | --- | --- |
| Compared | INFO | `app.boundary.router` | `match`, `overlap_ratio`, `threshold`, `original_area_ha`, `new_area_ha` |
| Rejected, 4xx | WARNING | `app.common.tracing` | Status, method, path and `detail` |

Each rejection is logged once, by `TraceIdMiddleware`, so the handler adds no line of its own. An unhandled exception bypasses that middleware: it returns a 500 and is logged by the ASGI server.

`ExtraFieldsFilter` adds the `x-cdp-request-id` trace id to every line, so a request can be followed from the calling service.
