"""Request schema describing a subset of reference tables to load."""

import re

from pydantic import BaseModel, field_validator

# `split -b` names parts with a fixed-width lowercase suffix: aa, ab, ... az,
# ba, ... The base is everything before ".part-".
_PART_RE = re.compile(r"^(?P<base>.+)\.part-(?P<suffix>[a-z]+)$")


def _suffix_index(suffix: str) -> int:
    """Position of a `split` suffix in its sequence: aa=0, ab=1, ba=26, ..."""
    index = 0
    for char in suffix:
        index = index * 26 + (ord(char) - ord("a"))
    return index


def _assert_contiguous_parts(keys: list[str]) -> None:
    """Reject a part list with a gap, a repeat, or a wrong order.

    Only applies when *every* key matches `<base>.part-<suffix>` with one shared
    base — the shape `make db-backup-tables` produces. Any other naming scheme
    skips the check, so callers using their own scheme are not constrained;
    order is then taken from the list exactly as given.

    Without this guard a dropped part surfaces as an opaque
    `zlib.error: invalid block type` mid-transaction, with nothing naming the
    table or the missing object.
    """
    matches = [_PART_RE.match(k) for k in keys]
    if not all(matches):
        return
    bases = {m.group("base") for m in matches if m}
    if len(bases) != 1:
        return
    widths = {len(m.group("suffix")) for m in matches if m}
    if len(widths) != 1:
        return
    for prev, cur in zip(matches, matches[1:], strict=False):
        if prev is None or cur is None:  # pragma: no cover - guarded above
            return
        prev_s = prev.group("suffix")
        cur_s = cur.group("suffix")
        if _suffix_index(cur_s) != _suffix_index(prev_s) + 1:
            base = next(iter(bases))
            msg = (
                f"manifest part keys are not contiguous: expected "
                f"{base}.part-<next after {prev_s}> but got {base}.part-{cur_s}; "
                "list every part exactly once, in concatenation order"
            )
            raise ValueError(msg)


class TableEntry(BaseModel):
    """One table's dump plus the version label to record.

    `key` is either a single S3 object key (relative to the configured prefix)
    or, for a dump split by `split -b`, the ordered list of its part keys. The
    parts are byte slices of one gzip member, so list order IS concatenation
    order and every part must be present.
    """

    key: str | list[str]
    version: str

    @property
    def keys(self) -> list[str]:
        """Part keys in concatenation order; the single-key form as a 1-list."""
        return [self.key] if isinstance(self.key, str) else self.key

    @property
    def is_split(self) -> bool:
        """True when this entry names more than one part."""
        return len(self.keys) > 1

    @field_validator("version")
    @classmethod
    def _non_empty_version(cls, v: str) -> str:
        if not v:
            msg = "manifest table entry version must be non-empty"
            raise ValueError(msg)
        return v

    @field_validator("key")
    @classmethod
    def _non_empty_key(cls, v: str | list[str]) -> str | list[str]:
        keys = [v] if isinstance(v, str) else v
        if not keys or not all(keys):
            msg = (
                "manifest table entry key must be a non-empty object key or a "
                "non-empty list of non-empty part keys"
            )
            raise ValueError(msg)
        _assert_contiguous_parts(keys)
        return v


class Manifest(BaseModel):
    """One or more reference tables to (re)load, each at its own version.

    Supplied in the body of a POST /admin/data-sync call. `tables` maps a
    table name to its dump key (or ordered part keys) plus the version to
    record; a subset of the allow-list is valid.
    """

    tables: dict[str, TableEntry]

    @field_validator("tables")
    @classmethod
    def _non_empty_tables(cls, v: dict[str, TableEntry]) -> dict[str, TableEntry]:
        if not v:
            msg = "manifest tables map must contain at least one entry"
            raise ValueError(msg)
        return v
