"""Core domain models for environmental impact assessments.

These models represent the business domain entities and calculations
as immutable value objects, separate from spatial data structures.

Includes models for:
- Nutrient mitigation impact assessment
- GCN (Great Crested Newt) impact assessment
"""

from pydantic import BaseModel, ConfigDict, Field


class Development(BaseModel):
    """A proposed residential development (Red Line Boundary).

    Attributes:
        id: ID from input data
        name: Development name
        dwelling_category: Category of dwelling type
        source: Data source identifier
        dwellings: Number of residential units
        area_m2: Total development area in square metres
        area_ha: Total development area in hectares
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(description="Development ID")
    name: str = Field(description="Development name")
    dwelling_category: str = Field(description="Category of dwelling type")
    source: str = Field(description="Data source identifier")
    dwellings: int = Field(ge=0, description="Number of residential units")
    area_m2: float = Field(ge=0, description="Total area in square metres")
    area_ha: float = Field(ge=0, description="Total area in hectares")


class SpatialAssignment(BaseModel):
    """Spatial feature assignments for a development.

    Determined by majority overlap with spatial layers.

    Attributes:
        wwtw_id: Wastewater Treatment Works ID
        wwtw_name: Wastewater Treatment Works name (None if outside modeled
            catchments)
        wwtw_subcatchment: WwTW operational subcatchment
        lpa_name: Local Planning Authority name
        nn_catchment: Nutrient Neutrality catchment name(s)
        dev_subcatchment: Development's operational subcatchment
        area_in_nn_catchment_ha: Area overlapping NN catchment in hectares
            (None if outside NN catchment)
    """

    model_config = ConfigDict(frozen=True)

    wwtw_id: int = Field(description="WwTW ID (141 if outside modelled catchments)")
    wwtw_name: str | None = Field(default=None, description="WwTW facility name")
    wwtw_subcatchment: str | None = Field(
        default=None, description="WwTW operational subcatchment"
    )
    lpa_name: str = Field(description="Local Planning Authority name")
    nn_catchment: str | None = Field(
        default=None, description="Nutrient Neutrality catchment(s)"
    )
    dev_subcatchment: str | None = Field(
        default=None, description="Development operational subcatchment"
    )
    area_in_nn_catchment_ha: float | None = Field(
        default=None,
        ge=0,
        description="Area within NN catchment (hectares), None if outside NN catchment",
    )


class LandUseImpact(BaseModel):
    """Nutrient impacts from land use change.

    Calculated from difference between current land use coefficients
    and residential land use coefficients.

    Attributes:
        nitrogen_kg_yr: Nitrogen uplift in kg/year
            (None if outside NN catchment)
        phosphorus_kg_yr: Phosphorus uplift in kg/year
            (None if outside NN catchment)
        nitrogen_post_suds_kg_yr: Nitrogen after SuDS mitigation
            (None if outside NN catchment)
        phosphorus_post_suds_kg_yr: Phosphorus after SuDS mitigation
            (None if outside NN catchment)
    """

    model_config = ConfigDict(frozen=True)

    nitrogen_kg_yr: float | None = Field(
        default=None,
        description="Nitrogen uplift (kg/year), None if outside NN catchment",
    )
    phosphorus_kg_yr: float | None = Field(
        default=None,
        description="Phosphorus uplift (kg/year), None if outside NN catchment",
    )
    nitrogen_post_suds_kg_yr: float | None = Field(
        default=None,
        description="Nitrogen after SuDS (kg/year), None if outside NN catchment",
    )
    phosphorus_post_suds_kg_yr: float | None = Field(
        default=None,
        description="Phosphorus after SuDS (kg/year), None if outside NN catchment",
    )


class WastewaterImpact(BaseModel):
    """Nutrient impacts from wastewater treatment.

    Calculated from water usage, occupancy rates, and WwTW permit concentrations.

    Note: occupancy_rate, water_usage, and daily_water_usage may be None when
    a development is within a WwTW catchment but outside NN catchment (no rates
    lookup match). In such cases, only permit concentrations are available.

    Attributes:
        occupancy_rate: People per dwelling (None if rates unavailable)
        water_usage_L_per_person_day: Litres per person per day (None if rates unavailable)
        daily_water_usage_L: Total daily water usage (None if rates unavailable)
        nitrogen_conc_2025_2030_mg_L: N concentration 2025-2030 (mg/L)
        phosphorus_conc_2025_2030_mg_L: P concentration 2025-2030 (mg/L)
        nitrogen_conc_2030_onwards_mg_L: N concentration 2030+ (mg/L)
        phosphorus_conc_2030_onwards_mg_L: P concentration 2030+ (mg/L)
        nitrogen_temp_kg_yr: Temporary N load (2025-2030) in kg/year
        phosphorus_temp_kg_yr: Temporary P load (2025-2030) in kg/year
        nitrogen_perm_kg_yr: Permanent N load (2030+) in kg/year
        phosphorus_perm_kg_yr: Permanent P load (2030+) in kg/year
    """

    model_config = ConfigDict(frozen=True)

    occupancy_rate: float | None = Field(
        default=None,
        gt=0,
        description="People per dwelling (None if rates unavailable)",
    )
    water_usage_L_per_person_day: float | None = Field(  # noqa: N815
        default=None,
        gt=0,
        description="Litres per person per day (None if rates unavailable)",
    )
    daily_water_usage_L: float | None = Field(  # noqa: N815
        default=None,
        ge=0,
        description="Total daily water usage (None if rates unavailable)",
    )
    nitrogen_conc_2025_2030_mg_L: float | None = Field(  # noqa: N815
        default=None, description="N concentration 2025-2030 (mg/L)"
    )
    phosphorus_conc_2025_2030_mg_L: float | None = Field(  # noqa: N815
        default=None, description="P concentration 2025-2030 (mg/L)"
    )
    nitrogen_conc_2030_onwards_mg_L: float | None = Field(  # noqa: N815
        default=None, description="N concentration 2030+ (mg/L)"
    )
    phosphorus_conc_2030_onwards_mg_L: float | None = Field(  # noqa: N815
        default=None, description="P concentration 2030+ (mg/L)"
    )
    nitrogen_temp_kg_yr: float | None = Field(
        default=None, description="Temporary N load 2025-2030 (kg/year)"
    )
    phosphorus_temp_kg_yr: float | None = Field(
        default=None, description="Temporary P load 2025-2030 (kg/year)"
    )
    nitrogen_perm_kg_yr: float | None = Field(
        default=None, description="Permanent N load 2030+ (kg/year)"
    )
    phosphorus_perm_kg_yr: float | None = Field(
        default=None, description="Permanent P load 2030+ (kg/year)"
    )


class NutrientImpact(BaseModel):
    """Total nutrient impacts including precautionary buffer.

    Final nutrient totals combining land use change and wastewater impacts.

    Attributes:
        nitrogen_total_kg_yr Total nitrogen impact with buffer (kg/year)
        phosphorus_total_kg_yr Total phosphorus impact with buffer (kg/year)
    """

    model_config = ConfigDict(frozen=True)

    nitrogen_total_kg_yr: float = Field(
        description="Total nitrogen with precautionary buffer (kg/year)"
    )
    phosphorus_total_kg_yr: float = Field(
        description="Total phosphorus with precautionary buffer (kg/year)"
    )


class ImpactAssessmentResult(BaseModel):
    """Complete impact assessment result for a development.

    Aggregates all domain models into a single result object.

    Attributes:
        rlb_id: Internal Red Line Boundary ID (assigned during processing)
        development: Core development information
        spatial: Spatial feature assignments
        land_use: Land use change impacts
        wastewater: Wastewater treatment impacts (None if outside WwTW catchments)
        total: Total nutrient impacts with buffer
    """

    model_config = ConfigDict(frozen=True)

    rlb_id: int = Field(ge=1, description="Internal RLB ID")
    development: Development
    spatial: SpatialAssignment
    land_use: LandUseImpact
    wastewater: WastewaterImpact | None = Field(
        default=None, description="None if outside WwTW catchments"
    )
    total: NutrientImpact

    def is_within_nn_catchment(self) -> bool:
        """Check if development is within a Nutrient Neutrality catchment.

        Returns:
            True if development overlaps any NN catchment
        """
        return self.spatial.nn_catchment is not None

    def is_within_wwtw_catchment(self) -> bool:
        """Check if development is within a modeled WwTW catchment.

        Returns:
            True if development is within a modeled WwTW catchment
        """
        return self.wastewater is not None and self.spatial.wwtw_name is not None

    def requires_assessment(self) -> bool:
        """Check if development requires nutrient impact assessment.

        Returns:
            True if development is within scope (NN catchment OR WwTW catchment)
        """
        return self.is_within_nn_catchment() or self.is_within_wwtw_catchment()


# ======================================================================================
# GCN (Great Crested Newt) Assessment Models
# ======================================================================================


class GcnDevelopment(BaseModel):
    """Development site information for GCN assessment.

    Attributes:
        id: Development ID from input data
        name: Optional development name
        unique_ref: Unique run reference (timestamp)
        unique_site: UniqueSite identifier (UniqueRef_SiteNNNNN)
        unique_buffer_site: UniqueBufferSite identifier (UniqueRef_BufferNNNNN), None for RLB
        area: Area type - "RLB" (Red Line Boundary) or "Buffer"
        orig_fid: Original feature ID from input RLB GeoDataFrame
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(description="Development ID")
    name: str | None = Field(default=None, description="Development name")
    unique_ref: str = Field(description="Unique run reference (timestamp)")
    unique_site: str = Field(description="UniqueSite identifier (UniqueRef_SiteNNNNN)")
    unique_buffer_site: str | None = Field(
        default=None, description="UniqueBufferSite (None for RLB)"
    )
    area: str = Field(description="Area type: 'RLB' or 'Buffer'")
    orig_fid: int = Field(
        ge=0, description="Original feature ID from input RLB GeoDataFrame"
    )


class GcnPondInfo(BaseModel):
    """Pond information for GCN assessment.

    Attributes:
        pond_id: Unique pond identifier
        pans: Presence/absence status - P (present), A (absent), NS (not surveyed)
        tmp_imp: Temporary impact flag - T (temporary impact), F (no impact)
        area: Location relative to development - "RLB" or "Buffer"
        concatenate_rz: Concatenated risk zones (e.g., "Red:Amber")
        max_zone: Highest priority zone (Red > Amber > Green)
    """

    model_config = ConfigDict(frozen=True)

    pond_id: str = Field(description="Unique pond identifier")
    pans: str = Field(description="P (present), A (absent), NS (not surveyed)")
    tmp_imp: str = Field(description="T (temporary impact), F (no impact)")
    area: str = Field(description="'RLB' or 'Buffer'")
    concatenate_rz: str = Field(description="Concatenated risk zones")
    max_zone: str = Field(description="Highest priority zone (Red/Amber/Green)")


class GcnHabitatImpact(BaseModel):
    """Habitat impact within GCN risk zones.

    Represents habitat area impacts by risk zone, split between RLB and buffer areas.
    Corresponds to one row in Habitat_Impact_{UniqueRef}.psv output.

    Attributes:
        unique_site: UniqueSite identifier
        unique_buffer_site: UniqueBufferSite identifier (None for RLB rows)
        area: Area type - "RLB" or "Buffer"
        risk_zone: GCN risk zone - "Red", "Amber", or "Green"
        shape_area: Habitat area in square metres
        orig_fid: Original feature ID from input RLB shapefile
        fid_rlb_merge_with_buffer: FID from RLB merge operation
        fid_rzs_clipped: FID from risk zones clipping operation
    """

    model_config = ConfigDict(frozen=True)

    unique_site: str = Field(description="UniqueSite identifier")
    unique_buffer_site: str | None = Field(
        default=None, description="UniqueBufferSite (None for RLB)"
    )
    area: str = Field(description="'RLB' or 'Buffer'")
    risk_zone: str = Field(description="'Red', 'Amber', or 'Green'")
    shape_area: float = Field(ge=0, description="Area in square metres")

    # Metadata for traceability
    orig_fid: int = Field(ge=0, description="Original feature ID from input RLB")
    fid_rlb_merge_with_buffer: int = Field(description="FID from RLB merge")
    fid_rzs_clipped: int = Field(description="FID from risk zones clipping")


class GcnPondFrequency(BaseModel):
    """Pond frequency by zone, presence/absence, and area.

    Aggregated pond counts by status and zone, split between RLB and buffer areas.
    Corresponds to one row in Ponds_Impact_Frequency_{UniqueRef}.psv output.

    Attributes:
        pans: Presence, absence or not surveyed status - P, A, or NS
        area: Area type - "RLB" or "Buffer"
        max_zone: Highest priority zone - "Red", "Amber", or "Green"
        tmp_imp: Temporary impact flag - T or F
        frequency: Count of ponds matching these criteria
    """

    model_config = ConfigDict(frozen=True)

    pans: str = Field(description="P, A, or NS")
    area: str = Field(description="'RLB' or 'Buffer'")
    max_zone: str = Field(description="'Red', 'Amber', or 'Green'")
    tmp_imp: str = Field(description="T or F")
    frequency: int = Field(ge=0, description="Count of ponds")


class GcnAssessmentResult(BaseModel):
    """Complete GCN impact assessment result for a development site.

    Contains all habitat and pond impacts, along with detailed pond information
    for validation and debugging.

    Attributes:
        unique_ref: Unique run reference (timestamp)
        development: Development site information
        habitat_impacts: List of habitat impacts by risk zone and area
        pond_frequencies: Aggregated pond counts by zone and status
        ponds_in_rlb: Detailed pond info for ponds within RLB
        ponds_in_buffer: Detailed pond info for ponds in buffer area
    """

    model_config = ConfigDict(frozen=True)

    unique_ref: str = Field(description="Unique run reference")
    development: GcnDevelopment = Field(description="Development information")

    # Primary outputs
    habitat_impacts: list[GcnHabitatImpact] = Field(
        description="Habitat impacts by risk zone"
    )
    pond_frequencies: list[GcnPondFrequency] = Field(
        description="Pond frequencies by zone and status"
    )

    # Detailed pond data for validation
    ponds_in_rlb: list[GcnPondInfo] = Field(description="Ponds within RLB")
    ponds_in_buffer: list[GcnPondInfo] = Field(description="Ponds in buffer area")
