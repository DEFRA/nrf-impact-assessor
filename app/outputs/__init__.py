"""Output strategies for impact assessment results."""

from app.outputs.base import OutputStrategy
from app.outputs.csv_output import CSVOutputStrategy

__all__ = ["OutputStrategy", "CSVOutputStrategy"]
