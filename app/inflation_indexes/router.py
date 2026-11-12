"""Inflation index endpoints: list the history, add, change and delete.

One row per charging year, in force for that whole year. The admin frontend
sends the published RICS CIL Index number; the factor applied to a levy is
derived from the fixed 2026 base row, never accepted from the caller.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, NoReturn, Self
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel

from app.common.api_fields import (
    CamelModel as _CamelModel,
)
from app.common.api_fields import Comment, StaffEmail, StaffFullName, StaffId
from app.models.db import LevyInflationIndex
from app.repositories import inflation_indexes as store
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()

CilIndex = Annotated[
    Decimal,
    Field(
        gt=0,
        max_digits=10,
        decimal_places=4,
        description="Published RICS CIL Index for the charging year, up to 4 dp",
    ),
]
ChargingYear = Annotated[
    int,
    Field(
        ge=2021,
        le=2100,
        description="The charging year the index applies to, from 1 January",
    ),
]


class InflationIndexCreate(_CamelModel):
    """Add an inflation index. Staff fields identify who is saving it."""

    charging_year: ChargingYear
    cil_index: CilIndex
    comment: Comment
    staff_id: StaffId
    staff_full_name: StaffFullName
    staff_email: StaffEmail


class InflationIndexUpdate(_CamelModel):
    """Change an index whose charging year has not begun. chargingYear cannot
    change: it is the row's identity, so a different year is a different row."""

    cil_index: CilIndex | None = None
    comment: Comment | None = None
    staff_id: StaffId | None = None
    staff_full_name: StaffFullName | None = None
    staff_email: StaffEmail | None = None

    @model_validator(mode="after")
    def _require_a_change(self) -> Self:
        if not self.model_fields_set:
            msg = "at least one field must be provided"
            raise ValueError(msg)
        if any(getattr(self, name) is None for name in self.model_fields_set):
            msg = "provided fields cannot be null"
            raise ValueError(msg)
        return self


class InflationIndexResponse(_CamelModel):
    """A saved inflation index. effectiveFrom is derived from the year."""

    model_config = ConfigDict(
        frozen=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    id: UUID
    charging_year: int
    cil_index: float
    index_factor: float
    effective_from: datetime
    created_at: datetime
    comment: str
    staff_id: str
    staff_full_name: str
    staff_email: str


def _to_response(row: LevyInflationIndex) -> InflationIndexResponse:
    return InflationIndexResponse(
        id=row.id,
        charging_year=row.charging_year,
        cil_index=row.cil_index,
        index_factor=row.index_factor,
        effective_from=store.effective_from(row.charging_year),
        created_at=row.created_at,
        comment=row.comment,
        staff_id=row.staff_id,
        staff_full_name=row.staff_full_name,
        staff_email=row.staff_email,
    )


def _get_repository() -> Repository:
    """Return the process-wide shared Repository."""
    return get_shared_repository()


def _raise_conflict(detail: str) -> NoReturn:
    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def _raise_not_found(detail: str) -> NoReturn:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


def _raise_year_not_future() -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="chargingYear must be a charging year that has not begun yet",
    )


def _raise_base_index_missing() -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=(
            f"the base charging year {store.BASE_CHARGING_YEAR} index row is "
            "missing, so no index factor can be derived"
        ),
    )


@router.get(
    "/inflation-indexes",
    response_model_by_alias=True,
    summary="List every inflation index, newest charging year first",
)
def list_inflation_indexes() -> list[InflationIndexResponse]:
    """Return the full index history, newest charging year first."""
    with _get_repository().session() as session:
        return [_to_response(row) for row in store.list_inflation_indexes(session)]


@router.post(
    "/inflation-indexes",
    status_code=status.HTTP_201_CREATED,
    response_model_by_alias=True,
    responses={
        409: {"description": "An index already exists for the charging year"},
        422: {"description": "Invalid request"},
        500: {"description": "The base charging year's index row is missing"},
    },
)
def add_inflation_index(body: InflationIndexCreate) -> InflationIndexResponse:
    """Add the index for a charging year that has not begun yet."""
    with _get_repository().session() as session:
        try:
            row = store.add_inflation_index(
                session,
                **body.model_dump(),
                now=datetime.now(UTC),
            )
        except store.ChargingYearNotFutureError:
            _raise_year_not_future()
        except store.YearAlreadyExistsError:
            _raise_conflict(
                f"An inflation index for {body.charging_year} already exists"
            )
        except store.BaseIndexMissingError:
            _raise_base_index_missing()
        session.commit()
        logger.info(
            f"Inflation index added: id={row.id} charging_year={row.charging_year} "
            f"index_factor={row.index_factor} staff_id={row.staff_id}"
        )
        return _to_response(row)


@router.patch(
    "/inflation-indexes/{index_id}",
    response_model_by_alias=True,
    responses={
        404: {"description": "Inflation index not found"},
        409: {
            "description": "The charging year has begun, or the index has priced a quote"
        },
        422: {"description": "Invalid request"},
        500: {"description": "The base charging year's index row is missing"},
    },
)
def update_inflation_index(
    index_id: UUID, body: InflationIndexUpdate
) -> InflationIndexResponse:
    """Change an index whose charging year has not begun yet."""
    with _get_repository().session() as session:
        try:
            row = store.update_inflation_index(
                session,
                index_id,
                body.model_dump(exclude_unset=True),
                now=datetime.now(UTC),
            )
        except store.InflationIndexNotFoundError:
            _raise_not_found(f"No inflation index with id {index_id}")
        except store.InflationIndexTakenEffectError:
            _raise_conflict(
                f"Inflation index {index_id} has taken effect and cannot be changed"
            )
        except store.BaseIndexMissingError:
            _raise_base_index_missing()
        session.commit()
        logger.info(
            f"Inflation index updated: id={row.id} "
            f"fields={sorted(body.model_fields_set)}"
        )
        return _to_response(row)


@router.delete(
    "/inflation-indexes/{index_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"description": "Inflation index not found"},
        409: {
            "description": "The charging year has begun, or the index has priced a quote"
        },
    },
)
def delete_inflation_index(index_id: UUID) -> None:
    """Delete an index whose charging year has not begun yet."""
    with _get_repository().session() as session:
        try:
            store.delete_inflation_index(session, index_id, now=datetime.now(UTC))
        except store.InflationIndexNotFoundError:
            _raise_not_found(f"No inflation index with id {index_id}")
        except store.InflationIndexTakenEffectError:
            _raise_conflict(
                f"Inflation index {index_id} has taken effect and cannot be deleted"
            )
        except store.InflationIndexInUseError:
            _raise_conflict(
                f"Inflation index {index_id} has been used to price a quote "
                "and cannot be deleted"
            )
        session.commit()
        logger.info(f"Inflation index deleted: id={index_id}")
