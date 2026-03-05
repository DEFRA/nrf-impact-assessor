"""Wastewater treatment nutrient load calculations.

Calculates nutrient loads from residential wastewater based on water usage,
occupancy rates, and treatment works permit concentrations.
"""

from app.config import CONSTANTS


def calculate_wastewater_load(
    dwellings: int,
    occupancy_rate: float,
    water_usage_litres_per_person_per_day: float,
    nitrogen_conc_mg_per_litre: float,
    phosphorus_conc_mg_per_litre: float,
) -> tuple[float, float, float]:
    """Calculate nutrient load from wastewater treatment.

    Formula:
        daily_water_litres = dwellings * occupancy_rate * water_usage_per_person
        annual_water_litres = daily_water_litres * days_per_year
        N_load_kg = annual_water_litres * ((N_conc_mg/L / 1,000,000) * 0.9)
        P_load_kg = annual_water_litres * ((P_conc_mg/L / 1,000,000) * 0.9)

    Args:
        dwellings: Number of residential units
        occupancy_rate: People per dwelling (e.g., 2.4)
        water_usage_litres_per_person_per_day: Water consumption per person per day
        nitrogen_conc_mg_per_litre: N concentration at WwTW permit (mg/L)
        phosphorus_conc_mg_per_litre: P concentration at WwTW permit (mg/L)

    Returns:
        Tuple of (daily_water_litres, nitrogen_kg_per_year, phosphorus_kg_per_year).
    """
    daily_water_litres = dwellings * (
        occupancy_rate * water_usage_litres_per_person_per_day
    )
    annual_water_litres = daily_water_litres * CONSTANTS.DAYS_PER_YEAR

    nitrogen_kg_per_year = annual_water_litres * (
        (nitrogen_conc_mg_per_litre / CONSTANTS.MILLIGRAMS_PER_KILOGRAM) * 0.9
    )
    phosphorus_kg_per_year = annual_water_litres * (
        (phosphorus_conc_mg_per_litre / CONSTANTS.MILLIGRAMS_PER_KILOGRAM) * 0.9
    )

    return daily_water_litres, nitrogen_kg_per_year, phosphorus_kg_per_year
