"""Unit tests for configuration."""


def test_default_config_values():
    """Test default configuration values."""
    from app.config import DEFAULT_CONFIG

    assert DEFAULT_CONFIG.precautionary_buffer_percent == 20.0
    assert DEFAULT_CONFIG.greenspace.threshold_area_ha == 2.5
    assert DEFAULT_CONFIG.greenspace.greenspace_percent == 20.0
    assert DEFAULT_CONFIG.greenspace.nitrogen_coeff == 3.0
    assert DEFAULT_CONFIG.greenspace.phosphorus_coeff == 0.2
    assert DEFAULT_CONFIG.suds.threshold_area_ha == 2.5
    assert DEFAULT_CONFIG.suds.removal_rate_percent == 40.0
    assert DEFAULT_CONFIG.suds.flow_capture_percent == 100.0
    assert DEFAULT_CONFIG.fallback_wwtw_id == 141


def test_suds_reduction_calculation():
    """Test SuDS total reduction factor calculation."""
    from app.config import SuDsConfig

    suds = SuDsConfig(
        threshold_area_ha=2.5,
        flow_capture_percent=100.0,
        removal_rate_percent=40.0,
    )

    # 100% capture * 40% removal = 0.40 total reduction
    assert suds.total_reduction_factor == 0.40


def test_precautionary_buffer_calculation():
    """Test precautionary buffer factor calculation."""
    from app.config import AssessmentConfig

    config = AssessmentConfig(precautionary_buffer_percent=20.0)

    # 20% = 0.20 factor
    assert config.precautionary_buffer_factor == 0.20
