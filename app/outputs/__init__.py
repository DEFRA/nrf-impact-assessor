"""Output strategies for impact assessment results."""

from app.outputs.base import OutputStrategy
from app.outputs.csv import CSVOutputStrategy

__all__ = ["OutputStrategy", "CSVOutputStrategy"]
