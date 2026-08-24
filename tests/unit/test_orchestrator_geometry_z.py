"""Inline boundary GeoJSON may carry a Z ordinate; the pipeline must be 2D.

PostGIS temp tables declare `geometry(Geometry, 27700)` (2D typmod), so a
`POLYGON Z` reaching the insert fails with "Geometry has Z dimension but column
does not". Reference data is already flattened on load (`scripts/load_data.py`),
so client input must be flattened the same way.
"""

from unittest.mock import MagicMock, patch

from app.models.enums import AssessmentType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob
from app.orchestrator import JobOrchestrator

_RING_3D = [
    [582793.260942, 328151.166652, 0],
    [582847.156996, 328153.199841, 0],
    [582843.800585, 328242.165813, 0],
    [582789.905522, 328240.132642, 0],
    [582793.260942, 328151.166652, 0],
]


def _job() -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={
                "type": "Polygon",
                "coordinates": [_RING_3D],
            },
            intersecting_edps=[],
        ),
    )


def test_inline_geometry_with_z_is_flattened_to_2d():
    orch = JobOrchestrator.__new__(JobOrchestrator)
    orch.repository = MagicMock()

    with patch("app.orchestrator.run_assessment", return_value={}) as run:
        orch._process_inline_geometry(_job(), AssessmentType.NUTRIENT)

    gdf = run.call_args.kwargs["rlb_gdf"]
    assert not gdf.geometry.has_z.any()
    assert "Z" not in gdf.geometry.iloc[0].wkt
