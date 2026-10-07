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
    """Tests for land use change uplift with greenspace adjustment and SuDS."""

    @pytest.fixture
    def default_gs_config(self):
        return GreenspaceConfig()  # threshold=2.5ha, 10%, N=3.0, P=0.2

    @pytest.fixture
    def default_suds_config(self):
        return SuDsConfig()  # threshold=2.5ha, 100% capture, 15% removal

    def test_positive_uplift_below_thresholds(
        self, default_gs_config, default_suds_config
    ):
        """Below 2.5ha neither greenspace nor SuDS adjusts the uplift."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=1.5,
            dev_area_ha=0.5,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        assert n_uplift == pytest.approx(22.5)  # (25 - 10) * 1.5
        assert p_uplift == pytest.approx(4.5)  # (5 - 2) * 1.5
        assert n_post == pytest.approx(22.5)
        assert p_post == pytest.approx(4.5)

    def test_positive_uplift_above_thresholds(
        self, default_gs_config, default_suds_config
    ):
        """At or above 2.5ha greenspace is split out and SuDS reduces the resi part."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=2.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        # N: resi = 25 * 0.90 = 22.5, gs = 0.10 * 3.0 = 0.3
        # pre-SuDS: (22.8 - 10) * 2 = 25.6
        # post-SuDS: (22.5 * 0.85 + 0.3 - 10) * 2 = 18.85
        assert n_uplift == pytest.approx(25.6)
        assert n_post == pytest.approx(18.85)

        # P: resi = 5 * 0.90 = 4.5, gs = 0.10 * 0.2 = 0.02
        # pre-SuDS: (4.52 - 2) * 2 = 5.04
        # post-SuDS: (4.5 * 0.85 + 0.02 - 2) * 2 = 3.69
        assert p_uplift == pytest.approx(5.04)
        assert p_post == pytest.approx(3.69)

    def test_thresholds_are_inclusive(self, default_gs_config, default_suds_config):
        """A development of exactly 2.5ha gets greenspace and SuDS."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=1.0,
            dev_area_ha=2.5,
            current_nitrogen_coeff=0.0,
            residential_nitrogen_coeff=10.0,
            current_phosphorus_coeff=0.0,
            residential_phosphorus_coeff=2.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        assert n_uplift == pytest.approx(9.3)  # 9.0 + 0.3
        assert n_post == pytest.approx(7.95)  # 9.0 * 0.85 + 0.3
        assert p_uplift == pytest.approx(1.82)  # 1.8 + 0.02
        assert p_post == pytest.approx(1.55)  # 1.8 * 0.85 + 0.02

    def test_suds_does_not_reduce_greenspace_component(
        self, default_gs_config, default_suds_config
    ):
        """With no residential loading, SuDS leaves the greenspace uplift unchanged."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=1.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=0.0,
            residential_nitrogen_coeff=0.0,
            current_phosphorus_coeff=0.0,
            residential_phosphorus_coeff=0.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        assert n_post == pytest.approx(n_uplift) == pytest.approx(0.3)
        assert p_post == pytest.approx(p_uplift) == pytest.approx(0.02)

    def test_negative_uplift(self, default_gs_config, default_suds_config):
        """SuDS reduces residential loading only, leaving current land use untouched."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=2.0,
            dev_area_ha=3.0,
            current_nitrogen_coeff=30.0,
            residential_nitrogen_coeff=15.0,
            current_phosphorus_coeff=8.0,
            residential_phosphorus_coeff=3.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        # N: (13.5 + 0.3 - 30) * 2 = -32.4; (13.5 * 0.85 + 0.3 - 30) * 2 = -36.45
        assert n_uplift == pytest.approx(-32.4)
        assert n_post == pytest.approx(-36.45)
        # P: (2.7 + 0.02 - 8) * 2 = -10.56; (2.7 * 0.85 + 0.02 - 8) * 2 = -11.37
        assert p_uplift == pytest.approx(-10.56)
        assert p_post == pytest.approx(-11.37)

    def test_zero_area(self, default_gs_config, default_suds_config):
        """Test with zero development area."""
        result = calculate_land_use_uplift(
            area_hectares=0.0,
            dev_area_ha=0.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        np.testing.assert_array_almost_equal(result, [0.0, 0.0, 0.0, 0.0])

    def test_rounding(self, default_gs_config, default_suds_config):
        """Test that results are rounded to 2 decimal places."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=1.333,
            dev_area_ha=0.5,  # Below threshold
            current_nitrogen_coeff=10.777,
            residential_nitrogen_coeff=25.888,
            current_phosphorus_coeff=2.111,
            residential_phosphorus_coeff=5.999,
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        assert n_uplift == n_post == round((25.888 - 10.777) * 1.333, 2)
        assert p_uplift == p_post == round((5.999 - 2.111) * 1.333, 2)

    def test_vectorized(self, default_gs_config, default_suds_config):
        """Test thresholds are applied per row on numpy arrays."""
        n_uplift, _, n_post, _ = calculate_land_use_uplift(
            area_hectares=np.array([1.5, 2.0]),
            dev_area_ha=np.array([0.5, 3.0]),
            current_nitrogen_coeff=np.array([10.0, 10.0]),
            residential_nitrogen_coeff=np.array([25.0, 25.0]),
            current_phosphorus_coeff=np.array([2.0, 2.0]),
            residential_phosphorus_coeff=np.array([5.0, 5.0]),
            greenspace_config=default_gs_config,
            suds_config=default_suds_config,
        )

        np.testing.assert_array_almost_equal(n_uplift, [22.5, 25.6])
        np.testing.assert_array_almost_equal(n_post, [22.5, 18.85])

    def test_custom_config(self):
        """Test with custom greenspace and SuDS configuration."""
        n_uplift, p_uplift, n_post, p_post = calculate_land_use_uplift(
            area_hectares=1.0,
            dev_area_ha=1.5,
            current_nitrogen_coeff=0.0,
            residential_nitrogen_coeff=10.0,
            current_phosphorus_coeff=0.0,
            residential_phosphorus_coeff=1.0,
            greenspace_config=GreenspaceConfig(
                threshold_area_ha=1.0, greenspace_percent=20.0
            ),
            suds_config=SuDsConfig(
                threshold_area_ha=1.0, capture_percent=50.0, removal_rate_percent=40.0
            ),
        )

        # N: resi 8.0, gs 0.6; SuDS reduction = 0.5 * 0.4 = 0.2 -> 6.4 + 0.6
        assert n_uplift == pytest.approx(8.6)
        assert n_post == pytest.approx(7.0)
        # P: resi 0.8, gs 0.04 -> 0.64 + 0.04
        assert p_uplift == pytest.approx(0.84)
        assert p_post == pytest.approx(0.68)


class TestSuDsMitigationCalculator:
    """Tests for SuDS removal on the residential coefficient."""

    @pytest.mark.parametrize(
        ("dev_area_ha", "expected"),
        [(2.49, 20.0), (2.5, 17.0), (10.0, 17.0)],
    )
    def test_applies_removal_at_or_above_area_threshold(self, dev_area_ha, expected):
        result = apply_suds_mitigation(
            residential_coeff=20.0, dev_area_ha=dev_area_ha, suds_config=SuDsConfig()
        )

        assert result == pytest.approx(expected)


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
