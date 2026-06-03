"""Unit tests for configuration."""

import pytest


def test_default_config_values():
    """Test default configuration values."""
    from app.config import DEFAULT_CONFIG

    assert DEFAULT_CONFIG.precautionary_buffer_percent == pytest.approx(20.0)
    assert DEFAULT_CONFIG.greenspace.threshold_area_ha == pytest.approx(1.0)
    assert DEFAULT_CONFIG.greenspace.greenspace_percent == pytest.approx(20.0)
    assert DEFAULT_CONFIG.greenspace.nitrogen_coeff == pytest.approx(3.0)
    assert DEFAULT_CONFIG.greenspace.phosphorus_coeff == pytest.approx(0.2)
    assert DEFAULT_CONFIG.suds.threshold_dwellings == 50
    assert DEFAULT_CONFIG.suds.removal_rate_percent == pytest.approx(25.0)
    assert DEFAULT_CONFIG.fallback_wwtw_id == 141


def test_suds_reduction_calculation():
    """Test SuDS total reduction factor calculation."""
    from app.config import SuDsConfig

    suds = SuDsConfig(
        threshold_dwellings=50,
        removal_rate_percent=25.0,
    )

    # 25% removal = 0.25 total reduction
    assert suds.total_reduction_factor == pytest.approx(0.25)


def test_precautionary_buffer_calculation():
    """Test precautionary buffer factor calculation."""
    from app.config import AssessmentConfig

    config = AssessmentConfig(precautionary_buffer_percent=20.0)

    # 20% = 0.20 factor
    assert config.precautionary_buffer_factor == pytest.approx(0.20)


def test_data_sync_config_defaults(monkeypatch):
    for var in ("DATA_SYNC_ENABLED", "DATA_SYNC_S3_BUCKET", "DATA_SYNC_AUTH_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    from app.config import DataSyncConfig

    cfg = DataSyncConfig()
    assert cfg.enabled is False
    assert cfg.manifest_key == "manifest.json"
    assert cfg.lock_key == 728191
    assert "coefficient_layer" in cfg.tables


def test_data_sync_config_from_env(monkeypatch):
    monkeypatch.setenv("DATA_SYNC_ENABLED", "true")
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", "ref-data")
    monkeypatch.setenv("DATA_SYNC_S3_PREFIX", "dumps")
    monkeypatch.setenv("DATA_SYNC_AUTH_TOKEN", "secret")
    from app.config import DataSyncConfig

    cfg = DataSyncConfig()
    assert cfg.enabled is True
    assert cfg.s3_bucket == "ref-data"
    assert cfg.s3_prefix == "dumps"
    assert cfg.auth_token == "secret"  # noqa: S105
