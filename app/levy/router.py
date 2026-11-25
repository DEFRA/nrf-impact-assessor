"""Levy recalculation endpoint."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)
from pydantic.alias_generators import to_camel

from app.calculators.levy import (
    MAX_LEVY_AMOUNT,
    MAX_LEVY_UNITS,
    LevyAmountTooLargeError,
    LevyCalculation,
    LevyChargeUnavailableError,
    LevyUnitsTooLargeError,
)
from app.levy.service import price_levy
from app.levy.wire import LevyWire
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()

DEFAULT_EDP_ID = 1


class LevyRecalculationRequest(BaseModel):
    """Recalculate levy request."""

    model_config = ConfigDict(alias_generator=to_camel, extra="forbid", strict=True)

    edp_id: int = Field(
        default=DEFAULT_EDP_ID, gt=0, description="EDP to price, 1 when empty"
    )
    units: int = Field(gt=0, description="Number of housing units")
    request_date: AwareDatetime = Field(
        description="Moment the levy is calculated for, an ISO 8601 date-time in UTC"
    )

    @field_validator("request_date")
    @classmethod
    def _require_utc(cls, value: datetime) -> datetime:
        """Reject any offset but UTC, so the levy is priced on the UTC date."""
        if value.utcoffset() != timedelta(0):
            msg = "requestDate must be in UTC"
            raise ValueError(msg)
        return value

    @field_validator("edp_id", mode="before")
    @classmethod
    def _default_empty_edp_id(cls, value):
        """Use the default EDP when edpId is null or an empty string."""
        return DEFAULT_EDP_ID if value is None or value == "" else value


async def _parse_request(request: Request) -> LevyRecalculationRequest:
    """Validate the recalculate request, returning 400 when invalid."""
    try:
        return LevyRecalculationRequest.model_validate_json(await request.body())
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.errors(
                include_url=False, include_context=False, include_input=False
            ),
        ) from None


def _get_repository() -> Repository:
    """Return the process-wide shared Repository."""
    return get_shared_repository()


def _recalculate(body: LevyRecalculationRequest) -> LevyCalculation | None:
    """Price the levy for the requested EDP, units and moment."""
    with _get_repository().session() as session:
        return price_levy(session, body.edp_id, body.units, body.request_date)


@router.post(
    "/levy/recalculate",
    response_model_by_alias=True,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": LevyRecalculationRequest.model_json_schema(by_alias=True)
                }
            },
        }
    },
    responses={
        400: {"description": "Invalid request"},
        404: {"description": "No base price in force for the EDP at the date-time"},
        422: {"description": "Levy or units too large, or otherwise unavailable"},
    },
)
async def recalculate_levy(request: Request) -> LevyWire:
    """Recalculate the levy for an EDP."""
    body = await _parse_request(request)
    try:
        levy = await asyncio.to_thread(_recalculate, body)
    except LevyAmountTooLargeError as e:
        logger.warning(f"Levy too large for EDP {body.edp_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Levy of £{e.amount:,.2f} for {e.units:,} units exceeds the "
                f"maximum of £{MAX_LEVY_AMOUNT:,.2f}. Reduce the number of units."
            ),
        ) from None
    except LevyUnitsTooLargeError as e:
        logger.warning(f"Too many units for EDP {body.edp_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{e.units:,} units exceeds the maximum of {MAX_LEVY_UNITS:,}. "
                "Reduce the number of units."
            ),
        ) from None
    except LevyChargeUnavailableError as e:
        logger.error(  # NOSONAR - intentional: the reason is the whole story, no traceback
            f"Levy unavailable for EDP {body.edp_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Levy unavailable for the EDP at the request date-time",
        ) from None
    if levy is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No base price in force for EDP {body.edp_id} at "
                f"{body.request_date.isoformat()}"
            ),
        )

    calculated_at = datetime.now(UTC)
    logger.info(
        "Levy recalculated: "
        f"edp_id={body.edp_id} request_date={body.request_date.isoformat()} "
        f"calculation_date={calculated_at.isoformat().replace('+00:00', 'Z')} "
        f"provisional_amount={levy.provisional_amount} "
        f"inflation_adjusted_amount={levy.inflation_adjusted_amount}"
    )
    # Provisional is units x base price; the adjusted amount is it inflated to
    # the calculation year, or £0 when provisional is under £50.
    return LevyWire.from_calculation(levy, calculated_at)
