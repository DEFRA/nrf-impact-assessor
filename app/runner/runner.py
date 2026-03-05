"""Assessment execution

This module provides a runner for pluggable assessments.
"""

import logging

import geopandas as gpd
import pandas as pd

from app.assessments.gcn import GcnAssessment
from app.assessments.nutrient import NutrientAssessment
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)


ASSESSMENT_TYPES: dict[str, type] = {
    "gcn": GcnAssessment,
    "nutrient": NutrientAssessment,
}


def run_assessment(
    assessment_type: str,
    rlb_gdf: gpd.GeoDataFrame,
    metadata: dict,
    repository: Repository,
) -> dict[str, pd.DataFrame | gpd.GeoDataFrame]:
    """Run an impact assessment and return results as DataFrames.

    Args:
        assessment_type: Assessment identifier (e.g., "gcn", "nutrient")
        rlb_gdf: Red Line Boundary GeoDataFrame
        metadata: Assessment metadata (unique_ref, optional parameters, etc.)
        repository: Data repository instance for loading reference data

    Returns:
        Dictionary of DataFrames/GeoDataFrames from the assessment.

    Raises:
        KeyError: If assessment type is not registered
        ValueError: If assessment.run() fails or returns invalid data
    """
    logger.info(f"Running assessment: {assessment_type}")

    assessment_class = ASSESSMENT_TYPES.get(assessment_type)
    if assessment_class is None:
        msg = f"Assessment type {assessment_type} not supported"
        raise KeyError(msg)

    logger.info(f"Instantiating {assessment_class.__name__}")
    try:
        assessment = assessment_class(rlb_gdf, metadata, repository)
    except Exception as e:
        logger.error(f"Assessment instantiation failed: {e}")
        msg = f"Failed to instantiate assessment '{assessment_type}'"
        raise ValueError(msg) from e

    logger.info(f"Executing {assessment_type}.run()")
    try:
        dataframes = assessment.run()
    except Exception as e:
        logger.error(f"Assessment execution failed: {e}")
        msg = f"Assessment '{assessment_type}' execution failed"
        raise ValueError(msg) from e

    if not isinstance(dataframes, dict):
        msg = (
            f"Assessment '{assessment_type}'.run() must return a dict, "
            f"got {type(dataframes).__name__}"
        )
        raise ValueError(msg)

    for key, value in dataframes.items():
        if not isinstance(value, pd.DataFrame | gpd.GeoDataFrame):
            msg = (
                f"Assessment '{assessment_type}'.run() returned invalid value for key '{key}': "
                f"expected DataFrame or GeoDataFrame, got {type(value).__name__}"
            )
            raise ValueError(msg)

    logger.info(
        f"Assessment returned {len(dataframes)} result set(s): {list(dataframes.keys())}"
    )

    return dataframes
