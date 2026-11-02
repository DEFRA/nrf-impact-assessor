"""Levy recalculation endpoint."""

import asyncio
import logging
from datetime import UTC, date, datetime

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
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
    request_date: date = Field(description="Date the levy is calculated for")

    @field_validator("edp_id", mode="before")
    @classmethod
    def _default_empty_edp_id(cls, value):
        """Use the default EDP when edpId is null or an empty string."""
        return DEFAULT_EDP_ID if value is None or value == "" else value


class LevyRecalculationResponse(BaseModel):
    """Recalculated levy."""

    model_config = ConfigDict(
        frozen=True, alias_generator=to_camel, populate_by_name=True
    )

    calculation_date: datetime
    provisional_amount: float
    inflation_adjusted_amount: float


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
    """Price the levy for the requested EDP, units and date."""
    with _get_repository().session() as session:
        # Inflation is out of scope here, so a missing RICS CIL index for a
        # later charging year must not make the levy unavailable.
        return price_levy(
            session,
            body.edp_id,
            body.units,
            body.request_date,
            apply_inflation=False,
        )


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
        404: {"description": "No levy charge for the EDP on the date"},
        422: {
            "description": "Levy or units too large to record, or otherwise unavailable"
        },
    },
)
async def recalculate_levy(request: Request) -> LevyRecalculationResponse:
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
            detail="Levy unavailable for the EDP on the request date",
        ) from None
    if levy is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No levy charge for EDP {body.edp_id} on {body.request_date}",
        )

    calculated_at = datetime.now(UTC)
    logger.info(
        "Levy recalculated: "
        f"edp_id={body.edp_id} request_date={levy.calculation_date} "
        f"calculation_date={calculated_at.isoformat().replace('+00:00', 'Z')} "
        f"provisional_amount={levy.provisional_amount} "
        f"inflation_adjusted_amount={levy.inflation_adjusted_amount}"
    )
    # Inflation is not applied yet: both amounts are the provisional levy.
    return LevyRecalculationResponse(
        calculation_date=calculated_at,
        provisional_amount=float(levy.provisional_amount),
        inflation_adjusted_amount=float(levy.inflation_adjusted_amount),
    )
