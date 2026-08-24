import pytest
from pydantic import ValidationError


def test_cache_max_size_zero_rejected():
    from app.config import AerialProxyConfig

    with pytest.raises(ValidationError, match="cache_max_size"):
        AerialProxyConfig(cache_max_size=0)


def test_cache_max_size_negative_rejected():
    from app.config import AerialProxyConfig

    with pytest.raises(ValidationError, match="cache_max_size"):
        AerialProxyConfig(cache_max_size=-1)


def test_cache_ttl_seconds_zero_rejected():
    from app.config import AerialProxyConfig

    with pytest.raises(ValidationError, match="cache_ttl_seconds"):
        AerialProxyConfig(cache_ttl_seconds=0)


def test_valid_config_accepted():
    from app.config import AerialProxyConfig

    cfg = AerialProxyConfig(cache_max_size=1, cache_ttl_seconds=1)
    assert cfg.cache_max_size == 1
    assert cfg.cache_ttl_seconds == 1
