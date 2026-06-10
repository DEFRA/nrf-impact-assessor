"""Unit tests for the IAM auth token cache in app.repositories.engine."""

from unittest.mock import patch

import pytest

from app.config import DatabaseSettings
from app.repositories import engine as engine_module


@pytest.fixture(autouse=True)
def clear_token_cache():
    engine_module._token_cache.clear()
    yield
    engine_module._token_cache.clear()


def _settings(**overrides) -> DatabaseSettings:
    defaults: dict = {"host": "db.example.com", "port": 5432, "user": "app_user"}
    defaults.update(overrides)
    return DatabaseSettings(**defaults)


def test_token_reused_within_ttl():
    with patch.object(
        engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
    ) as gen:
        first = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
        second = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
    assert first == second == "tok1"
    assert gen.call_count == 1


def test_token_regenerated_after_ttl():
    with (
        patch.object(
            engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
        ) as gen,
        patch.object(
            engine_module.time,
            "monotonic",
            side_effect=[0.0, engine_module.IAM_TOKEN_CACHE_SECONDS + 1.0],
        ),
    ):
        first = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
        second = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
    assert (first, second) == ("tok1", "tok2")
    assert gen.call_count == 2


def test_token_cache_keyed_per_target():
    with patch.object(
        engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
    ) as gen:
        first = engine_module._get_iam_auth_token(_settings(user="user_a"), "eu-west-2")
        second = engine_module._get_iam_auth_token(
            _settings(user="user_b"), "eu-west-2"
        )
    assert (first, second) == ("tok1", "tok2")
    assert gen.call_count == 2
