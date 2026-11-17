"""Shared Pydantic building blocks for admin-CRUD request bodies.

The base-prices and inflation-index endpoints accept the same staff fields
(who is saving the record) and the same mandatory comment, over the same
camelCase wire contract. Centralising the annotations keeps the lengths and
stripping rules from drifting between the two routers.
"""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    """Requests and responses alias snake_case fields to camelCase."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, extra="forbid"
    )


class UpdateModel(CamelModel):
    """A change body: every field optional, but at least one must be sent and
    none of the sent ones may be null."""

    @model_validator(mode="after")
    def _require_a_change(self) -> Self:
        if not self.model_fields_set:
            msg = "at least one field must be provided"
            raise ValueError(msg)
        if any(getattr(self, name) is None for name in self.model_fields_set):
            msg = "provided fields cannot be null"
            raise ValueError(msg)
        return self


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
Requestor = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    Field(description="Name of the person who asked for the change"),
]
JiraTicket = Annotated[
    str,
    StringConstraints(pattern=r"^NRF2-[0-9]+$", max_length=20),
    Field(description="Jira ticket recording the request, like NRF2-1234"),
]
