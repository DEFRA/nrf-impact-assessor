"""Base output strategy interface for impact assessment results."""

from pathlib import Path
from typing import Protocol

from app.models.domain import ImpactAssessmentResult


class OutputStrategy(Protocol):
    """Protocol for output strategies that serialize impact assessment results.

    Output strategies handle the transformation of domain models into various
    file formats (CSV, GeoJSON, etc.) for different use cases.

    Design note: Output strategies are separate from assessments. Assessments
    return DataFrames, adapters convert to List[ImpactAssessmentResult] domain
    models, and the caller decides when/where to write output using an appropriate
    strategy.
    """

    def write(self, results: list[ImpactAssessmentResult], output_path: Path) -> Path:
        """Write impact assessment results to a file.

        Args:
            results: List of impact assessment results (domain models)
            output_path: Path where output file should be written

        Returns:
            Path to the written output file (may differ from input if strategy
            modifies extension or creates additional files)

        Raises:
            IOError: If writing fails
            ValueError: If results cannot be serialized
        """
        ...
