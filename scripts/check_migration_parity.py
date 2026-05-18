#!/usr/bin/env python3
"""Check that every Alembic migration has a corresponding Liquibase changeset.

Each Liquibase changeset file must contain a comment of the form:
    <!-- Alembic revision: <revision_id> -->

Exit code 0 = all revisions covered; 1 = missing changesets; 2 = script error.
"""

import re
import sys
from pathlib import Path

VERSIONS_DIR = Path("alembic/versions")
CHANGELOG_DIR = Path("changelog/changesets")

_REVISION_RE = re.compile(
    r'^revision[^=\n]*=\s*["\']([a-f0-9]+)["\']',
    re.MULTILINE,
)
_DOWN_REVISION_RE = re.compile(
    r'^down_revision[^=\n]*=\s*(?:["\']([a-f0-9]+)["\']|None)',
    re.MULTILINE,
)
_LIQUIBASE_REF_RE = re.compile(r"<!--\s*Alembic revision:\s*([a-f0-9]+)\s*-->")


def _load_alembic_revisions(versions_dir: Path) -> dict[str, str | None]:
    revisions: dict[str, str | None] = {}
    for path in versions_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue
        text = path.read_text()
        rev_match = _REVISION_RE.search(text)
        down_match = _DOWN_REVISION_RE.search(text)
        if not rev_match:
            continue
        rev = rev_match.group(1)
        down = down_match.group(1) if down_match and down_match.group(1) else None
        revisions[rev] = down
    return revisions


def _walk_chain(revisions: dict[str, str | None]) -> list[str]:
    roots = [rev for rev, down in revisions.items() if down is None]
    if len(roots) != 1:
        print(f"ERROR: expected one root revision, found: {roots}", file=sys.stderr)
        sys.exit(2)
    children = {down: rev for rev, down in revisions.items() if down is not None}
    chain: list[str] = []
    current: str | None = roots[0]
    while current:
        chain.append(current)
        current = children.get(current)
    return chain


def _load_covered_revisions(changelog_dir: Path) -> dict[str, Path]:
    covered: dict[str, Path] = {}
    for path in sorted(changelog_dir.glob("*.xml")):
        for match in _LIQUIBASE_REF_RE.finditer(path.read_text()):
            covered[match.group(1)] = path
    return covered


def main() -> int:
    for directory in (VERSIONS_DIR, CHANGELOG_DIR):
        if not directory.exists():
            print(f"ERROR: directory not found: {directory}", file=sys.stderr)
            return 2

    revisions = _load_alembic_revisions(VERSIONS_DIR)
    if not revisions:
        print("ERROR: no Alembic migrations found", file=sys.stderr)
        return 2

    chain = _walk_chain(revisions)
    covered = _load_covered_revisions(CHANGELOG_DIR)

    missing = [rev for rev in chain if rev not in covered]

    print(f"Alembic revisions:    {len(chain)}")
    print(f"Liquibase changesets: {len(covered)}")
    print()

    if missing:
        print("FAIL: Alembic revisions with no matching Liquibase changeset:")
        for rev in missing:
            print(f"  {rev}")
        print()
        print("Add a changeset to changelog/changesets/ containing:")
        for rev in missing:
            print(f"  <!-- Alembic revision: {rev} -->")
        return 1

    print("OK: all Alembic revisions have a matching Liquibase changeset")
    for rev in chain:
        print(f"  {rev}  ←  {covered[rev].name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
