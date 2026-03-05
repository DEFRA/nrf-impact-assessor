"""Precautionary buffer application for nutrient impact assessment.

Aggregates land use change and wastewater impacts, applying a precautionary buffer.
"""


def apply_buffer(
    nitrogen_land_use_post_suds: float,
    phosphorus_land_use_post_suds: float,
    nitrogen_wastewater: float,
    phosphorus_wastewater: float,
    precautionary_buffer_percent: float,
) -> tuple[float, float]:
    """Apply precautionary buffer to combined nutrient impacts.

    Sums land use change (post-SuDS) and wastewater impacts, then applies
    a precautionary buffer to the absolute value of the combined impact.

    Formula:
        base_impact = land_use_post_suds + wastewater
        buffer_amount = abs(base_impact) * (precautionary_buffer_percent / 100)
        total_impact = base_impact + buffer_amount

    Args:
        nitrogen_land_use_post_suds: N from land use after SuDS (kg/year)
        phosphorus_land_use_post_suds: P from land use after SuDS (kg/year)
        nitrogen_wastewater: N from wastewater (kg/year)
        phosphorus_wastewater: P from wastewater (kg/year)
        precautionary_buffer_percent: Additional buffer percentage (e.g., 20 for 20%)

    Returns:
        Tuple of (nitrogen_total_kg_per_year, phosphorus_total_kg_per_year).
    """
    n_base = nitrogen_land_use_post_suds + nitrogen_wastewater
    p_base = phosphorus_land_use_post_suds + phosphorus_wastewater

    buffer_factor = precautionary_buffer_percent / 100
    n_buffer = abs(n_base) * buffer_factor
    p_buffer = abs(p_base) * buffer_factor

    nitrogen_total = n_base + n_buffer
    phosphorus_total = p_base + p_buffer

    return nitrogen_total, phosphorus_total
