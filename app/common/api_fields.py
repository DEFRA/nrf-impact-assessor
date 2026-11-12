"""Shared Pydantic building blocks for admin-CRUD request bodies.

The base-prices and inflation-index endpoints accept the same staff fields
(who is saving the record) and the same mandatory comment, over the same
camelCase wire contract. Centralising the annotations keeps the lengths and
stripping rules from drifting between the two routers.
"""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints
from pydantic.alias_generators import to_camel


class CamelModel(BaseModel):
    """Requests and responses alias snake_case fields to camelCase."""

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, extra="forbid"
    )


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
