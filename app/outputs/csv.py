"""CSV output strategy for impact assessment results.

This strategy converts domain models to the legacy 32-column CSV format,
maintaining backward compatibility for tactical use. The CSV format matches
the original script output for regression testing and existing workflows.
"""

from pathlib import Path

import pandas as pd

from app.models.domain import ImpactAssessmentResult


class CSVOutputStrategy:
    """Writes impact assessment results to CSV in legacy format.

    This strategy produces the exact 32-column CSV format from the legacy script,
    ensuring compatibility with existing data consumers and regression tests.

    The CSV columns represent:
    - Development metadata (ID, name, category, source, dwellings, area)
    - Spatial assignments (WwTW, LPA, NN catchment, subcatchments)
    - Land use impacts (uplift, post-SuDS)
    - Wastewater impacts (occupancy, usage, concentrations, loads)
    - Total nutrient impacts (with precautionary buffer)

    Note: Filename derivation from input shapefile is a tactical approach.
    In future phases, this will use an assessment run reference ID from the
    job orchestration system.
    """

    def write(self, results: list[ImpactAssessmentResult], output_path: Path) -> Path:
        """Write impact assessment results to CSV file.

        Args:
            results: List of impact assessment results (domain models)
            output_path: Path where CSV file should be written

        Returns:
            Path to the written CSV file

        Raises:
            IOError: If writing fails
            ValueError: If results cannot be serialized
        """
        if not results:
            msg = "Cannot write CSV: results list is empty"
            raise ValueError(msg)

        # Convert domain models to DataFrame rows
        rows = []
        for result in results:
            row = self._result_to_row(result)
            rows.append(row)

        # Create DataFrame with explicit column order (matches legacy)
        df = pd.DataFrame(rows)
        df = df[self._get_column_order()]

        # Write to CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

        return output_path

    def _result_to_row(self, result: ImpactAssessmentResult) -> dict:
        """Convert a single ImpactAssessmentResult to a CSV row dictionary.

        Args:
            result: Impact assessment result domain model

        Returns:
            Dictionary with keys matching legacy CSV column names
        """
        row = {
            # Development metadata
            "RLB_ID": result.rlb_id,
            "id": result.development.id,
            "Name": result.development.name,
            "Dwel_Cat": result.development.dwelling_category,
            "Source": result.development.source,
            "Dwellings": result.development.dwellings,
            "Dev_Area_Ha": result.development.area_ha,
            # Spatial assignments
            "AreaInNNCatchment": result.spatial.area_in_nn_catchment_ha,
            "NN_Catchment": result.spatial.nn_catchment,
            "Dev_SubCatchment": result.spatial.dev_subcatchment,
            "Majority_LPA": result.spatial.lpa_name,
            "Majority_WwTw_ID": result.spatial.wwtw_id,
            "WwTW_name": result.spatial.wwtw_name,
            "WwTw_SubCatchment": result.spatial.wwtw_subcatchment,
            # Land use impacts
            "N_LU_Uplift": result.land_use.nitrogen_kg_yr,
            "P_LU_Uplift": result.land_use.phosphorus_kg_yr,
            "N_LU_postSuDS": result.land_use.nitrogen_post_suds_kg_yr,
            "P_LU_postSuDS": result.land_use.phosphorus_post_suds_kg_yr,
        }

        # Wastewater impacts (may be None if outside WwTW catchments)
        if result.wastewater:
            row.update(
                {
                    "Occ_Rate": result.wastewater.occupancy_rate,
                    "Water_Usage_L_Day": result.wastewater.water_usage_L_per_person_day,
                    "Litres_used": result.wastewater.daily_water_usage_L,
                    "Nitrogen_2025_2030": result.wastewater.nitrogen_conc_2025_2030_mg_L,
                    "Nitrogen_2030_onwards": result.wastewater.nitrogen_conc_2030_onwards_mg_L,
                    "Phosphorus_2025_2030": result.wastewater.phosphorus_conc_2025_2030_mg_L,
                    "Phosphorus_2030_onwards": result.wastewater.phosphorus_conc_2030_onwards_mg_L,
                    "N_WwTW_Temp": result.wastewater.nitrogen_temp_kg_yr,
                    "P_WwTW_Temp": result.wastewater.phosphorus_temp_kg_yr,
                    "N_WwTW_Perm": result.wastewater.nitrogen_perm_kg_yr,
                    "P_WwTW_Perm": result.wastewater.phosphorus_perm_kg_yr,
                }
            )
        else:
            # Fill with None for developments outside WwTW catchments
            row.update(
                {
                    "Occ_Rate": None,
                    "Water_Usage_L_Day": None,
                    "Litres_used": None,
                    "Nitrogen_2025_2030": None,
                    "Nitrogen_2030_onwards": None,
                    "Phosphorus_2025_2030": None,
                    "Phosphorus_2030_onwards": None,
                    "N_WwTW_Temp": None,
                    "P_WwTW_Temp": None,
                    "N_WwTW_Perm": None,
                    "P_WwTW_Perm": None,
                }
            )

        # Total nutrient impacts
        row.update(
            {
                "N_Total": result.total.nitrogen_total_kg_yr,
                "P_Total": result.total.phosphorus_total_kg_yr,
            }
        )

        return row

    def _get_column_order(self) -> list[str]:
        """Get the CSV column order matching legacy output.

        Returns:
            List of column names in the correct order
        """
        return [
            # Development metadata (7 columns)
            "RLB_ID",
            "id",
            "Name",
            "Dwel_Cat",
            "Source",
            "Dwellings",
            "Dev_Area_Ha",
            # Spatial assignments (7 columns)
            "AreaInNNCatchment",
            "NN_Catchment",
            "Dev_SubCatchment",
            "Majority_LPA",
            "Majority_WwTw_ID",
            "WwTW_name",
            "WwTw_SubCatchment",
            # Land use impacts (4 columns)
            "N_LU_Uplift",
            "P_LU_Uplift",
            "N_LU_postSuDS",
            "P_LU_postSuDS",
            # Wastewater impacts (12 columns)
            "Occ_Rate",
            "Water_Usage_L_Day",
            "Litres_used",
            "Nitrogen_2025_2030",
            "Nitrogen_2030_onwards",
            "Phosphorus_2025_2030",
            "Phosphorus_2030_onwards",
            "N_WwTW_Temp",
            "P_WwTW_Temp",
            "N_WwTW_Perm",
            "P_WwTW_Perm",
            # Total impacts (2 columns)
            "N_Total",
            "P_Total",
        ]
