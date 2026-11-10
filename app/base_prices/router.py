"""EDP base price endpoints: list the history, add, change and delete."""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, NoReturn, Self

from fastapi import APIRouter, HTTPException, status
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)
from pydantic.alias_generators import to_camel

from app.repositories import base_prices as store
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()


def _require_utc(value: datetime) -> datetime:
    """Reject any offset but UTC, so every stored moment is unambiguous."""
    if value.utcoffset() != timedelta(0):
        msg = "must be in UTC"
        raise ValueError(msg)
    return value


UtcDatetime = Annotated[AwareDatetime, AfterValidator(_require_utc)]
BasePriceAmount = Annotated[
    Decimal,
    Field(
        gt=0,
        max_digits=10,
        decimal_places=2,
        description="Base price per unit in pounds, under 100,000,000, 2dp",
    ),
]
Comment = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)
]
StaffId = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=36)
]
StaffFullName = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
]
StaffEmail = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=254)
]


class _CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, extra="forbid"
    )


class BasePriceCreate(_CamelModel):
    """Add a base price. Staff fields identify who is saving it."""

    edp_id: int = Field(gt=0)
    base_price: BasePriceAmount
    effective_from: UtcDatetime = Field(
        description="UTC date-time the price takes effect, in the future"
    )
    comment: Comment
    staff_id: StaffId
    staff_full_name: StaffFullName
    staff_email: StaffEmail


class BasePriceUpdate(_CamelModel):
    """Change a base price that has not taken effect. edpId cannot change."""

    base_price: BasePriceAmount | None = None
    effective_from: UtcDatetime | None = None
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


class BasePriceResponse(_CamelModel):
    """A saved base price."""

    model_config = ConfigDict(
        frozen=True,
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    id: int
    edp_id: int
    base_price: float
    created_at: datetime
    effective_from: datetime
    comment: str
    staff_id: str
    staff_full_name: str
    staff_email: str


class EdpBasePricesResponse(_CamelModel):
    """An EDP with its base price history, newest effective_from first."""

    model_config = ConfigDict(frozen=True, alias_generator=to_camel)

    edp_id: int
    edp_name: str
    base_prices: list[BasePriceResponse]


def _get_repository() -> Repository:
    """Return the process-wide shared Repository."""
    return get_shared_repository()


def _raise_conflict(detail: str) -> NoReturn:
    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)


def _raise_not_found(detail: str) -> NoReturn:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


def _raise_effective_from_in_past() -> NoReturn:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="effectiveFrom must be in the future",
    )


@router.get(
    "/base-prices",
    response_model_by_alias=True,
    summary="List every EDP with its full base price history",
)
def list_base_prices() -> list[EdpBasePricesResponse]:
    """Return one object per EDP, each with all its base prices."""
    with _get_repository().session() as session:
        return [
            EdpBasePricesResponse(
                edp_id=edp.id,
                edp_name=edp.name,
                base_prices=[BasePriceResponse.model_validate(p) for p in prices],
            )
            for edp, prices in store.list_edps_with_base_prices(session)
        ]


@router.post(
    "/base-prices",
    status_code=status.HTTP_201_CREATED,
    response_model_by_alias=True,
    responses={
        404: {"description": "EDP not found"},
        409: {"description": "The EDP already has a base price yet to take effect"},
        422: {"description": "Invalid request"},
    },
)
def add_base_price(body: BasePriceCreate) -> BasePriceResponse:
    """Add a base price for an EDP."""
    with _get_repository().session() as session:
        try:
            row = store.add_base_price(
                session,
                **body.model_dump(),
                now=datetime.now(UTC),
            )
        except store.EdpNotFoundError:
            _raise_not_found(f"No EDP with id {body.edp_id}")
        except store.EffectiveFromInPastError:
            _raise_effective_from_in_past()
        except store.FutureBasePriceExistsError:
            _raise_conflict(
                f"EDP {body.edp_id} already has a base price yet to take effect"
            )
        except store.DuplicateEffectiveFromError:
            _raise_conflict(
                f"EDP {body.edp_id} already has a base price effective from "
                f"{body.effective_from.isoformat()}"
            )
        session.commit()
        logger.info(
            f"Base price added: id={row.id} edp_id={row.edp_id} "
            f"effective_from={row.effective_from.isoformat()} "
            f"staff_id={row.staff_id}"
        )
        return BasePriceResponse.model_validate(row)


@router.patch(
    "/base-prices/{base_price_id}",
    response_model_by_alias=True,
    responses={
        404: {"description": "Base price not found"},
        409: {"description": "The base price has taken effect, or the date clashes"},
        422: {"description": "Invalid request"},
    },
)
def update_base_price(base_price_id: int, body: BasePriceUpdate) -> BasePriceResponse:
    """Change a base price that has not taken effect yet."""
    with _get_repository().session() as session:
        try:
            row = store.update_base_price(
                session,
                base_price_id,
                body.model_dump(exclude_unset=True),
                now=datetime.now(UTC),
            )
        except store.BasePriceNotFoundError:
            _raise_not_found(f"No base price with id {base_price_id}")
        except store.BasePriceTakenEffectError:
            _raise_conflict(
                f"Base price {base_price_id} has taken effect and cannot be changed"
            )
        except store.EffectiveFromInPastError:
            _raise_effective_from_in_past()
        except store.DuplicateEffectiveFromError:
            _raise_conflict("The EDP already has a base price with that effectiveFrom")
        session.commit()
        logger.info(
            f"Base price updated: id={row.id} fields={sorted(body.model_fields_set)}"
        )
        return BasePriceResponse.model_validate(row)


@router.delete(
    "/base-prices/{base_price_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"description": "Base price not found"},
        409: {"description": "The base price has taken effect"},
    },
)
def delete_base_price(base_price_id: int) -> None:
    """Delete a base price that has not taken effect yet."""
    with _get_repository().session() as session:
        try:
            store.delete_base_price(session, base_price_id, now=datetime.now(UTC))
        except store.BasePriceNotFoundError:
            _raise_not_found(f"No base price with id {base_price_id}")
        except store.BasePriceTakenEffectError:
            _raise_conflict(
                f"Base price {base_price_id} has taken effect and cannot be deleted"
            )
        session.commit()
        logger.info(f"Base price deleted: id={base_price_id}")
