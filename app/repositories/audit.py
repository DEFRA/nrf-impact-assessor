"""The audit row for one assessment run: the model's output on success, the
error on failure."""

import json
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from app.models.db import ModelResultRecord
from app.version.router import GIT_HASH

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


def serialise_model_output(dataframes: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Each result frame as a list of records, geometry dropped as /assess does.

    Round-tripped through to_json so NaN becomes null and numpy scalars and
    timestamps become plain JSON values JSONB will accept.
    """
    return {
        key: json.loads(
            pd.DataFrame(df.drop(columns=["geometry"], errors="ignore")).to_json(
                orient="records", date_format="iso", default_handler=str
            )
        )
        for key, df in dataframes.items()
    }


def record_model_result(
    session: Session,
    nrl_reference: str,
    assessment_type: str,
    dataframes: dict[str, pd.DataFrame] | None,
    error: BaseException | None = None,
) -> ModelResultRecord:
    """Add the audit row for one run. The caller commits."""
    row = ModelResultRecord(
        status=STATUS_FAILED if error else STATUS_SUCCESS,
        nrl_reference=nrl_reference,
        assessment_type=assessment_type,
        model_version=GIT_HASH,
        model_output=serialise_model_output(dataframes) if dataframes else None,
        error_details=f"{type(error).__name__}: {error}" if error else None,
    )
    session.add(row)
    return row
