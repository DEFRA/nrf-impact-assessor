from app.data_sync.manifest import Manifest
from app.data_sync.service import needs_reload


def test_needs_reload_true_when_versions_differ():
    m = Manifest(data_version="v2", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=False) is True


def test_needs_reload_false_when_versions_match():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=False) is False


def test_needs_reload_true_when_forced():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=True) is True


def test_needs_reload_true_when_nothing_applied():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version=None, force=False) is True
