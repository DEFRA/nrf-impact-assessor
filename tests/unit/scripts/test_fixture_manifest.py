"""Tests for fixture checksum manifest generation and validation."""

import hashlib
from pathlib import Path

import pytest
from fixture_manifest import (
    FixtureManifestError,
    build_fixture_manifest,
    validate_fixture_manifest,
    write_fixture_manifest,
)
from load_data import SpatialDataLoader


def test_build_fixture_manifest_is_sorted_and_ignores_hidden_files(
    tmp_path: Path,
):
    (tmp_path / "nested").mkdir()
    (tmp_path / "z.gpkg").write_bytes(b"z")
    (tmp_path / "nested" / "a.sqlite").write_bytes(b"a")
    (tmp_path / ".DS_Store").write_bytes(b"ignored")

    manifest = build_fixture_manifest(tmp_path)

    assert manifest.splitlines() == [
        "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb  nested/a.sqlite",
        "594e519ae499312b29433b7dd8a97ff068defcba9755b6d5d00e84c524d67b06  z.gpkg",
    ]


def test_validate_fixture_manifest_accepts_generated_manifest(tmp_path: Path):
    (tmp_path / "fixture.gpkg").write_bytes(b"fixture")
    manifest_path = write_fixture_manifest(tmp_path)

    validate_fixture_manifest(tmp_path)

    manifest_digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert (tmp_path / "compose.labels").read_text(encoding="utf-8") == (
        f"uk.gov.defra.nrf.fixture-manifest={manifest_digest}\n"
    )


def test_validate_fixture_manifest_accepts_empty_fixtures_dir(tmp_path: Path):
    write_fixture_manifest(tmp_path)

    validate_fixture_manifest(tmp_path)


def test_validate_fixture_manifest_rejects_changed_fixture(tmp_path: Path):
    fixture = tmp_path / "fixture.gpkg"
    fixture.write_bytes(b"original")
    write_fixture_manifest(tmp_path)
    fixture.write_bytes(b"changed")

    with pytest.raises(FixtureManifestError, match="fixture.gpkg"):
        validate_fixture_manifest(tmp_path)


def test_validate_fixture_manifest_rejects_stale_compose_label(tmp_path: Path):
    (tmp_path / "fixture.gpkg").write_bytes(b"fixture")
    write_fixture_manifest(tmp_path)
    (tmp_path / "compose.labels").write_text(
        "uk.gov.defra.nrf.fixture-manifest=stale\n", encoding="utf-8"
    )

    with pytest.raises(FixtureManifestError, match="compose.labels"):
        validate_fixture_manifest(tmp_path)


def test_spatial_data_loader_validates_fixtures_before_loading(tmp_path: Path):
    fixture = tmp_path / "fixture.gpkg"
    fixture.write_bytes(b"original")
    write_fixture_manifest(tmp_path)
    fixture.write_bytes(b"changed")

    with pytest.raises(FixtureManifestError, match="fixture.gpkg"):
        SpatialDataLoader(object(), fixtures_dir=tmp_path)
