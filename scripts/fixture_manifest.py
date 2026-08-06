#!/usr/bin/env python

"""Generate and validate checksums for committed fixture data."""

import argparse
import hashlib
from pathlib import Path

MANIFEST_NAME = "manifest.sha256"
COMPOSE_LABELS_NAME = "compose.labels"
COMPOSE_LABEL_KEY = "nrf.fixture-manifest"


class FixtureManifestError(ValueError):
    """Raised when fixture files do not match their checksum manifest."""


def _fixture_files(fixtures_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in fixtures_dir.rglob("*")
        if path.is_file()
        and path.name not in {MANIFEST_NAME, COMPOSE_LABELS_NAME}
        and not any(
            part.startswith(".") for part in path.relative_to(fixtures_dir).parts
        )
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fixture:
        for chunk in iter(lambda: fixture.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_fixture_manifest(fixtures_dir: Path) -> str:
    """Return deterministic sha256sum-style entries for fixture files."""
    return "\n".join(
        f"{_sha256(path)}  {path.relative_to(fixtures_dir).as_posix()}"
        for path in _fixture_files(fixtures_dir)
    )


def write_fixture_manifest(fixtures_dir: Path) -> Path:
    """Write and return the fixture checksum manifest path."""
    manifest_path = fixtures_dir / MANIFEST_NAME
    manifest_path.write_text(
        f"{build_fixture_manifest(fixtures_dir)}\n", encoding="utf-8"
    )
    manifest_digest = _sha256(manifest_path)
    (fixtures_dir / COMPOSE_LABELS_NAME).write_text(
        f"{COMPOSE_LABEL_KEY}={manifest_digest}\n", encoding="utf-8"
    )
    return manifest_path


def _parse_manifest(manifest: str) -> dict[str, str]:
    entries = {}
    for line in manifest.splitlines():
        if not line.strip():
            continue
        try:
            checksum, relative_path = line.split("  ", maxsplit=1)
        except ValueError as error:
            message = f"Invalid fixture manifest entry: {line}"
            raise FixtureManifestError(message) from error
        entries[relative_path] = checksum
    return entries


def validate_fixture_manifest(fixtures_dir: Path) -> None:
    """Raise when fixture files differ from the committed manifest."""
    manifest_path = fixtures_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        message = f"Fixture manifest is missing: {manifest_path}"
        raise FixtureManifestError(message)

    expected = _parse_manifest(manifest_path.read_text(encoding="utf-8"))
    actual = _parse_manifest(build_fixture_manifest(fixtures_dir))
    changed_paths = sorted(
        path
        for path in expected.keys() | actual.keys()
        if expected.get(path) != actual.get(path)
    )
    if changed_paths:
        message = (
            f"Fixture files do not match {manifest_path}: {', '.join(changed_paths)}. "
            "Run scripts/fixture_manifest.py to regenerate it."
        )
        raise FixtureManifestError(message)

    compose_labels_path = fixtures_dir / COMPOSE_LABELS_NAME
    expected_label = f"{COMPOSE_LABEL_KEY}={_sha256(manifest_path)}\n"
    if (
        not compose_labels_path.is_file()
        or compose_labels_path.read_text(encoding="utf-8") != expected_label
    ):
        message = (
            f"Fixture Compose label does not match the manifest: "
            f"{compose_labels_path}. Run scripts/fixture_manifest.py to regenerate it."
        )
        raise FixtureManifestError(message)


def main() -> None:
    """Generate a checksum manifest for a fixture directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "fixtures_dir",
        nargs="?",
        type=Path,
        default=Path(__file__).parent.parent / "tests" / "data" / "fixtures",
    )
    args = parser.parse_args()
    manifest_path = write_fixture_manifest(args.fixtures_dir)
    print(f"Wrote {manifest_path}")


if __name__ == "__main__":
    main()
