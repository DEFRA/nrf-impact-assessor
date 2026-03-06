"""Unit tests for business logic calculators.

Tests all calculator functions with known inputs/outputs from IATScript.
"""

import pytest

from app.calculators import (
    apply_buffer,
    apply_suds_mitigation,
    calculate_land_use_uplift,
    calculate_wastewater_load,
)
from app.config import GreenspaceConfig, SuDsConfig


class TestLandUseCalculator:
    """Tests for land use change uplift calculations with greenspace adjustment."""

    @pytest.fixture
    def default_gs_config(self):
        return GreenspaceConfig()  # threshold=2.5ha, 20%, N=3.0, P=0.2

    def test_positive_uplift_below_greenspace_threshold(self, default_gs_config):
        """Test land use change below greenspace threshold (no greenspace adjustment)."""
        n_uplift, p_uplift, *_ = calculate_land_use_uplift(
            area_hectares=1.5,
            dev_area_ha=1.0,  # Below 2.5ha threshold
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == 22.5  # (25 - 10) * 1.5
        assert p_uplift == 4.5  # (5 - 2) * 1.5

    def test_positive_uplift_above_greenspace_threshold(self, default_gs_config):
        """Test greenspace adjustment for development >= 2.5ha."""
        n_uplift, p_uplift, resi_n, gs_n, resi_p, gs_p = calculate_land_use_uplift(
            area_hectares=1.5,
            dev_area_ha=3.0,  # Above 2.5ha threshold
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        # Residential component: 25 * (1 - 0.20) = 20.0
        assert resi_n == pytest.approx(20.0)
        # Greenspace component: 0.20 * 3.0 = 0.6
        assert gs_n == pytest.approx(0.6)
        # Adjusted coeff: 20.0 + 0.6 = 20.6
        # Uplift: (20.6 - 10.0) * 1.5 = 15.9
        assert n_uplift == pytest.approx(15.9)

        # P: resi = 5.0 * 0.80 = 4.0, gs = 0.20 * 0.2 = 0.04
        assert resi_p == pytest.approx(4.0)
        assert gs_p == pytest.approx(0.04)
        # Adjusted: 4.04, uplift: (4.04 - 2.0) * 1.5 = 3.06
        assert p_uplift == pytest.approx(3.06)

    def test_negative_uplift(self, default_gs_config):
        """Test land use change with negative nutrient uplift (improvement)."""
        n_uplift, p_uplift, *_ = calculate_land_use_uplift(
            area_hectares=2.0,
            dev_area_ha=1.0,  # Below threshold
            current_nitrogen_coeff=30.0,
            residential_nitrogen_coeff=15.0,
            current_phosphorus_coeff=8.0,
            residential_phosphorus_coeff=3.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == -30.0  # (15 - 30) * 2.0
        assert p_uplift == -10.0  # (3 - 8) * 2.0

    def test_zero_area(self, default_gs_config):
        """Test with zero development area."""
        n_uplift, p_uplift, *_ = calculate_land_use_uplift(
            area_hectares=0.0,
            dev_area_ha=0.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == 0.0
        assert p_uplift == 0.0

    def test_rounding(self, default_gs_config):
        """Test that results are rounded to 2 decimal places."""
        n_uplift, p_uplift, *_ = calculate_land_use_uplift(
            area_hectares=1.333,
            dev_area_ha=1.0,  # Below threshold
            current_nitrogen_coeff=10.777,
            residential_nitrogen_coeff=25.888,
            current_phosphorus_coeff=2.111,
            residential_phosphorus_coeff=5.999,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == round((25.888 - 10.777) * 1.333, 2)
        assert p_uplift == round((5.999 - 2.111) * 1.333, 2)

    def test_returns_six_values(self, default_gs_config):
        """Test that function returns 6-tuple with component breakdown."""
        result = calculate_land_use_uplift(
            area_hectares=1.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert len(result) == 6


class TestSuDsMitigationCalculator:
    """Tests for SuDS mitigation at coefficient level."""

    @pytest.fixture
    def default_suds_config(self):
        """Default SuDS configuration matching IATScript."""
        return SuDsConfig(
            threshold_area_ha=2.5,
            flow_capture_percent=100.0,
            removal_rate_percent=40.0,
        )

    def test_suds_above_threshold(self, default_suds_config):
        """Test SuDS applied to development >= 2.5ha."""
        # Simulate a large site with greenspace: resi_n=20.0, gs_n=0.6
        n_post, p_post = apply_suds_mitigation(
            area_hectares=1.5,
            dev_area_ha=3.0,  # Above 2.5ha threshold
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=20.0,  # 25 * 0.80
            gs_n_component=0.6,  # 0.20 * 3.0
            resi_p_component=4.0,  # 5 * 0.80
            gs_p_component=0.04,  # 0.20 * 0.2
            suds_config=default_suds_config,
        )

        # SuDS adj N: (20.0 * (1 - 0.40)) + 0.6 = 12.0 + 0.6 = 12.6
        # Post-SuDS uplift: (12.6 - 10.0) * 1.5 = 3.9
        assert n_post == 3.9

        # SuDS adj P: (4.0 * (1 - 0.40)) + 0.04 = 2.4 + 0.04 = 2.44
        # Post-SuDS uplift: (2.44 - 2.0) * 1.5 = 0.66
        assert p_post == 0.66

    def test_suds_below_threshold(self, default_suds_config):
        """Test SuDS NOT applied below threshold — returns greenspace-adjusted uplift."""
        n_post, p_post = apply_suds_mitigation(
            area_hectares=1.5,
            dev_area_ha=1.0,  # Below 2.5ha threshold
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=25.0,  # No greenspace adjustment below threshold
            gs_n_component=0.0,
            resi_p_component=5.0,
            gs_p_component=0.0,
            suds_config=default_suds_config,
        )

        # No SuDS: (25 - 10) * 1.5 = 22.5
        assert n_post == 22.5
        # No SuDS: (5 - 2) * 1.5 = 4.5
        assert p_post == 4.5

    def test_suds_zero_area(self, default_suds_config):
        """Test SuDS with zero intersection area."""
        n_post, p_post = apply_suds_mitigation(
            area_hectares=0.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=20.0,
            gs_n_component=0.6,
            resi_p_component=4.0,
            gs_p_component=0.04,
            suds_config=default_suds_config,
        )

        assert n_post == 0.0
        assert p_post == 0.0

    def test_suds_only_reduces_residential(self, default_suds_config):
        """Test that SuDS only reduces residential component, not greenspace."""
        # With greenspace component = 0 (small site that somehow got SuDS)
        config = SuDsConfig(
            threshold_area_ha=0.0,  # Always apply
            flow_capture_percent=100.0,
            removal_rate_percent=40.0,
        )

        n_post_no_gs, _ = apply_suds_mitigation(
            area_hectares=1.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=25.0,
            gs_n_component=0.0,
            resi_p_component=5.0,
            gs_p_component=0.0,
            suds_config=config,
        )

        n_post_with_gs, _ = apply_suds_mitigation(
            area_hectares=1.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=20.0,
            gs_n_component=5.0,  # Same total but split differently
            resi_p_component=5.0,
            gs_p_component=0.0,
            suds_config=config,
        )

        # Without GS: (25 * 0.6 + 0 - 10) * 1 = 5.0
        assert n_post_no_gs == 5.0
        # With GS: (20 * 0.6 + 5.0 - 10) * 1 = 7.0
        # GS component passes through unreduced
        assert n_post_with_gs == 7.0

    def test_custom_suds_config(self):
        """Test with custom SuDS configuration."""
        custom_config = SuDsConfig(
            threshold_area_ha=1.0,
            flow_capture_percent=75.0,
            removal_rate_percent=50.0,
        )

        n_post, p_post = apply_suds_mitigation(
            area_hectares=1.0,
            dev_area_ha=2.0,  # Above 1.0ha threshold
            current_nitrogen_coeff=10.0,
            current_phosphorus_coeff=2.0,
            resi_n_component=25.0,
            gs_n_component=0.0,
            resi_p_component=5.0,
            gs_p_component=0.0,
            suds_config=custom_config,
        )

        # Reduction = 0.75 * 0.50 = 0.375
        # SuDS adj N: 25 * (1 - 0.375) + 0 = 15.625
        # Uplift: (15.625 - 10) * 1 = 5.625 -> 5.62
        assert n_post == 5.62
        # SuDS adj P: 5 * 0.625 + 0 = 3.125
        # Uplift: (3.125 - 2) * 1 = 1.125 -> 1.12
        assert p_post == 1.12


class TestWastewaterLoadCalculator:
    """Tests for wastewater nutrient load calculations."""

    def test_basic_wastewater_load(self):
        """Test basic wastewater load calculation."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=100,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=10.0,
            phosphorus_conc_mg_per_litre=1.0,
        )

        # Daily water: 100 * (2.4 * 110) = 26,400 L
        assert daily_water == 26400.0

        # Annual water: 26,400 * 365.25 = 9,642,600 L
        # N load: 9,642,600 * ((10 / 1,000,000) * 0.9) = 86.7834 kg
        assert n_load == 86.7834

        # P load: 9,642,600 * ((1 / 1,000,000) * 0.9) = 8.67834 kg
        assert p_load == 8.67834

    def test_single_dwelling(self):
        """Test with single dwelling."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=1,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=10.0,
            phosphorus_conc_mg_per_litre=1.0,
        )

        assert daily_water == 264.0
        assert n_load == pytest.approx(0.867834)
        assert p_load == pytest.approx(0.0867834)

    def test_zero_concentration(self):
        """Test with zero WwTW permit concentration."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=50,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=0.0,
            phosphorus_conc_mg_per_litre=0.0,
        )

        assert daily_water == 13200.0
        assert n_load == 0.0
        assert p_load == 0.0

    def test_high_concentration(self):
        """Test with high nutrient concentrations."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=10,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=50.0,
            phosphorus_conc_mg_per_litre=10.0,
        )

        assert daily_water == 2640.0
        assert n_load == pytest.approx(43.3917)
        assert p_load == pytest.approx(8.67834)


class TestTotalImpactCalculator:
    """Tests for total nutrient impact with precautionary buffer."""

    def test_both_components_positive(self):
        """Test with positive land use and wastewater impacts."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=16.88,
            phosphorus_land_use_post_suds=3.38,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == pytest.approx(136.092)
        assert p_total == pytest.approx(15.636)

    def test_negative_land_use_positive_wastewater(self):
        """Test with negative land use (improvement) and positive wastewater."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=-37.5,
            phosphorus_land_use_post_suds=-12.5,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == 70.836
        assert p_total == -2.28

    def test_land_use_only(self):
        """Test with only land use impact (no wastewater)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=16.88,
            phosphorus_land_use_post_suds=3.38,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == 20.256
        assert p_total == 4.056

    def test_wastewater_only(self):
        """Test with only wastewater impact (no land use in NN catchment)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == 115.836
        assert p_total == 11.58

    def test_all_zero(self):
        """Test with zero impacts."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == 0.0
        assert p_total == 0.0

    def test_different_buffer_percent(self):
        """Test with different precautionary buffer percentage."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=10.0,
            phosphorus_land_use_post_suds=2.0,
            nitrogen_wastewater=90.0,
            phosphorus_wastewater=8.0,
            precautionary_buffer_percent=10.0,
        )

        assert n_total == 110.0
        assert p_total == 11.0
