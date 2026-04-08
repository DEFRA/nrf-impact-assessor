"""Unit tests for business logic calculators.

Tests all calculator functions with known inputs/outputs from IATScript.
"""

import numpy as np
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
        return GreenspaceConfig()  # threshold=1.0ha, 20%, N=3.0, P=0.2

    def test_positive_uplift_below_greenspace_threshold(self, default_gs_config):
        """Test land use change below greenspace threshold (no greenspace adjustment)."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=1.5,
            dev_area_ha=0.5,  # Below 1.0ha threshold
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == pytest.approx(22.5)  # (25 - 10) * 1.5
        assert p_uplift == pytest.approx(4.5)  # (5 - 2) * 1.5

    def test_positive_uplift_above_greenspace_threshold(self, default_gs_config):
        """Test greenspace adjustment for development >= 1.0ha."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=1.5,
            dev_area_ha=3.0,  # Above 1.0ha threshold
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        # Residential component: 25 * (1 - 0.20) = 20.0
        # Greenspace component: 0.20 * 3.0 = 0.6
        # Adjusted coeff: 20.0 + 0.6 = 20.6
        # Uplift: (20.6 - 10.0) * 1.5 = 15.9
        assert n_uplift == pytest.approx(15.9)

        # P: resi = 5.0 * 0.80 = 4.0, gs = 0.20 * 0.2 = 0.04
        # Adjusted: 4.04, uplift: (4.04 - 2.0) * 1.5 = 3.06
        assert p_uplift == pytest.approx(3.06)

    def test_negative_uplift(self, default_gs_config):
        """Test land use change with negative nutrient uplift (improvement)."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=2.0,
            dev_area_ha=0.5,  # Below threshold
            current_nitrogen_coeff=30.0,
            residential_nitrogen_coeff=15.0,
            current_phosphorus_coeff=8.0,
            residential_phosphorus_coeff=3.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == pytest.approx(-30.0)  # (15 - 30) * 2.0
        assert p_uplift == pytest.approx(-10.0)  # (3 - 8) * 2.0

    def test_zero_area(self, default_gs_config):
        """Test with zero development area."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=0.0,
            dev_area_ha=0.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == pytest.approx(0.0)
        assert p_uplift == pytest.approx(0.0)

    def test_rounding(self, default_gs_config):
        """Test that results are rounded to 2 decimal places."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=1.333,
            dev_area_ha=0.5,  # Below threshold
            current_nitrogen_coeff=10.777,
            residential_nitrogen_coeff=25.888,
            current_phosphorus_coeff=2.111,
            residential_phosphorus_coeff=5.999,
            greenspace_config=default_gs_config,
        )

        assert n_uplift == round((25.888 - 10.777) * 1.333, 2)
        assert p_uplift == round((5.999 - 2.111) * 1.333, 2)

    def test_returns_two_values(self, default_gs_config):
        """Test that function returns 2-tuple (n_uplift, p_uplift)."""
        result = calculate_land_use_uplift(
            area_hectares=1.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
        )

        assert len(result) == 2


class TestSuDsMitigationCalculator:
    """Tests for SuDS mitigation on aggregated uplift totals."""

    @pytest.fixture
    def default_suds_config(self):
        """Default SuDS configuration matching IATScript."""
        return SuDsConfig()

    def test_suds_above_threshold(self, default_suds_config):
        """Test SuDS applied to development >= 50 dwellings."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=15.9,
            p_lu_uplift=3.06,
            dwellings=60,  # Above 50 threshold
            suds_config=default_suds_config,
        )

        # n: 15.9 - abs(15.9) * 0.25 = 15.9 - 3.975 = 11.925 -> 11.92 (np.round half-even)
        assert n_post == pytest.approx(11.92)
        # p: 3.06 - abs(3.06) * 0.25 = 3.06 - 0.765 = 2.295 -> 2.3 (np.round half-even)
        assert p_post == pytest.approx(2.3)

    def test_suds_below_threshold(self, default_suds_config):
        """Test SuDS NOT applied below threshold — uplift unchanged."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=22.5,
            p_lu_uplift=4.5,
            dwellings=30,  # Below 50 threshold
            suds_config=default_suds_config,
        )

        assert n_post == pytest.approx(22.5)
        assert p_post == pytest.approx(4.5)

    def test_suds_at_threshold(self, default_suds_config):
        """Test SuDS applied at exactly 50 dwellings."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=20.0,
            p_lu_uplift=4.0,
            dwellings=50,  # Exactly at threshold
            suds_config=default_suds_config,
        )

        # n: 20 - abs(20) * 0.25 = 20 - 5 = 15.0
        assert n_post == pytest.approx(15.0)
        # p: 4 - abs(4) * 0.25 = 4 - 1 = 3.0
        assert p_post == pytest.approx(3.0)

    def test_suds_negative_uplift_amplified(self, default_suds_config):
        """Test that SuDS amplifies negative uplift (improvement) via abs()."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=-20.0,
            p_lu_uplift=-4.0,
            dwellings=60,
            suds_config=default_suds_config,
        )

        # n: -20 - abs(-20) * 0.25 = -20 - 5 = -25.0
        assert n_post == pytest.approx(-25.0)
        # p: -4 - abs(-4) * 0.25 = -4 - 1 = -5.0
        assert p_post == pytest.approx(-5.0)

    def test_suds_zero_uplift(self, default_suds_config):
        """Test SuDS with zero uplift."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=0.0,
            p_lu_uplift=0.0,
            dwellings=60,
            suds_config=default_suds_config,
        )

        assert n_post == pytest.approx(0.0)
        assert p_post == pytest.approx(0.0)

    def test_suds_vectorized(self, default_suds_config):
        """Test SuDS works with numpy arrays (vectorized)."""
        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=np.array([20.0, 10.0, -5.0]),
            p_lu_uplift=np.array([4.0, 2.0, -1.0]),
            dwellings=np.array([60, 30, 100]),
            suds_config=default_suds_config,
        )

        # dwellings=60 >= 50: 20 - 5 = 15, dwellings=30 < 50: 10, dwellings=100 >= 50: -5 - 1.25 = -6.25
        np.testing.assert_array_almost_equal(n_post, [15.0, 10.0, -6.25])
        np.testing.assert_array_almost_equal(p_post, [3.0, 2.0, -1.25])

    def test_custom_suds_config(self):
        """Test with custom SuDS configuration."""
        custom_config = SuDsConfig(
            threshold_dwellings=10,
            removal_rate_percent=40.0,
        )

        n_post, p_post = apply_suds_mitigation(
            n_lu_uplift=100.0,
            p_lu_uplift=20.0,
            dwellings=15,  # Above 10 threshold
            suds_config=custom_config,
        )

        # n: 100 - abs(100) * 0.40 = 100 - 40 = 60.0
        assert n_post == pytest.approx(60.0)
        # p: 20 - abs(20) * 0.40 = 20 - 8 = 12.0
        assert p_post == pytest.approx(12.0)


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
        assert daily_water == pytest.approx(26400.0)

        # Annual water: 26,400 * 365.25 = 9,642,600 L
        # N load: 9,642,600 * ((10 / 1,000,000) * 0.9) = 86.7834 kg
        assert n_load == pytest.approx(86.7834)

        # P load: 9,642,600 * ((1 / 1,000,000) * 0.9) = 8.67834 kg
        assert p_load == pytest.approx(8.67834)

    def test_single_dwelling(self):
        """Test with single dwelling."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=1,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=10.0,
            phosphorus_conc_mg_per_litre=1.0,
        )

        assert daily_water == pytest.approx(264.0)
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

        assert daily_water == pytest.approx(13200.0)
        assert n_load == pytest.approx(0.0)
        assert p_load == pytest.approx(0.0)

    def test_high_concentration(self):
        """Test with high nutrient concentrations."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=10,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=50.0,
            phosphorus_conc_mg_per_litre=10.0,
        )

        assert daily_water == pytest.approx(2640.0)
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

        assert n_total == pytest.approx(70.836)
        assert p_total == pytest.approx(-2.28)

    def test_land_use_only(self):
        """Test with only land use impact (no wastewater)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=16.88,
            phosphorus_land_use_post_suds=3.38,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == pytest.approx(20.256)
        assert p_total == pytest.approx(4.056)

    def test_wastewater_only(self):
        """Test with only wastewater impact (no land use in NN catchment)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == pytest.approx(115.836)
        assert p_total == pytest.approx(11.58)

    def test_all_zero(self):
        """Test with zero impacts."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == pytest.approx(0.0)
        assert p_total == pytest.approx(0.0)

    def test_different_buffer_percent(self):
        """Test with different precautionary buffer percentage."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=10.0,
            phosphorus_land_use_post_suds=2.0,
            nitrogen_wastewater=90.0,
            phosphorus_wastewater=8.0,
            precautionary_buffer_percent=10.0,
        )

        assert n_total == pytest.approx(110.0)
        assert p_total == pytest.approx(11.0)
