"""Pre-flight reference-data guard.

An assessment reads spatial and lookup reference tables. If one of those tables
is empty the assessment does not fail loudly — it produces empty or meaningless
results (or, for the lookup tables, an IndexError that used to be swallowed).
Either way the job would be marked "done" and its SQS message deleted, silently
dropping work that should have been retried once the data is reloaded.

`assert_reference_data_present` is called before the assessment runs so an empty
required table raises instead, leaving the message on the queue for redelivery.
"""

import logging

from sqlalchemy import func, select

from app.models.db import (
    CoefficientLayer,
    LookupTable,
    LpaBoundaries,
    NnCatchments,
    Subcatchments,
    WwtwCatchments,
)
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)


class EmptyReferenceDataError(RuntimeError):
    """Raised when a reference table required by an assessment is empty."""


# The reference tables each assessment type reads. An assessment type missing
# from this map is not guarded (e.g. "gcn" until its dependencies are listed).
_REQUIRED_TABLES: dict[str, list[tuple[type, str]]] = {
    "nutrient": [
        (CoefficientLayer, "coefficient_layer"),
        (LookupTable, "lookup_table"),
        (WwtwCatchments, "wwtw_catchments"),
        (LpaBoundaries, "lpa_boundaries"),
        (NnCatchments, "nn_catchments"),
        (Subcatchments, "subcatchments"),
    ],
}


def assert_reference_data_present(repository: Repository, assessment_type: str) -> None:
    """Raise EmptyReferenceDataError if any reference table the assessment needs
    is empty. Unknown assessment types are not guarded (no-op).
    """
    required = _REQUIRED_TABLES.get(assessment_type)
    if not required:
        return

    empty: list[str] = []
    with repository.session() as session:
        for model, label in required:
            count = session.scalar(select(func.count()).select_from(model))
            if not count:
                empty.append(label)

    if empty:
        msg = (
            f"{assessment_type} assessment requires reference data, but these "
            f"tables are empty: {', '.join(empty)}. Reload reference data before "
            "processing."
        )
        raise EmptyReferenceDataError(msg)
