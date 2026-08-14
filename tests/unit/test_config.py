"""Unit tests for configuration."""

import pytest
from pydantic import ValidationError


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


def test_keepalive_interval_defaults_under_pool_recycle():
    """The keepalive must tick inside pool_recycle."""
    from app.config import DatabaseSettings
    from app.repositories.engine import IAM_TOKEN_POOL_RECYCLE_SECONDS

    settings = DatabaseSettings()

    assert settings.keepalive_interval_seconds == 240
    assert settings.keepalive_interval_seconds < IAM_TOKEN_POOL_RECYCLE_SECONDS


def test_keepalive_interval_overridable_from_environment(monkeypatch):
    from app.config import DatabaseSettings

    monkeypatch.setenv("DB_KEEPALIVE_INTERVAL_SECONDS", "0")

    assert DatabaseSettings().keepalive_interval_seconds == 0


@pytest.mark.parametrize("interval", ["600", "900"])
def test_keepalive_interval_rejected_at_or_past_pool_recycle(monkeypatch, interval):
    """Ticking slower than pool_recycle silently defeats the keepalive."""
    from app.config import DatabaseSettings

    monkeypatch.setenv("DB_KEEPALIVE_INTERVAL_SECONDS", interval)

    with pytest.raises(ValidationError, match="pool recycle window"):
        DatabaseSettings()


@pytest.mark.parametrize("slots", ["0", "-1"])
def test_keepalive_warm_slots_rejected_when_not_positive(monkeypatch, slots):
    """A non-positive cap makes every tick a no-op while logging it started."""
    from app.config import DatabaseSettings

    monkeypatch.setenv("DB_KEEPALIVE_WARM_SLOTS", slots)

    with pytest.raises(ValidationError):
        DatabaseSettings()


def test_keepalive_warm_slots_bounded_below_pool_size():
    """The per-replica connection floor must stay under the pool size."""
    from app.config import DatabaseSettings
    from app.repositories.engine import DEFAULT_SHARED_POOL_SIZE

    settings = DatabaseSettings()

    assert settings.keepalive_warm_slots == 3
    assert settings.keepalive_warm_slots < DEFAULT_SHARED_POOL_SIZE
