"""resolve_edp_id parses attributes->>'EDP_Id' strictly: anything that is not
an integer is 'no id', so the caller fails closed (spec decision 6)."""

from unittest.mock import MagicMock, patch

import pytest

from app.repositories.levy import resolve_edp_id

LABEL = "Broads SAC EDP"


def _session_returning(value) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = value
    return session


def _session_returning_ids(*values) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = list(values)
    return session


@pytest.fixture(autouse=True)
def active_version():
    with patch("app.repositories.levy.get_active_version", return_value=3) as gav:
        yield gav


def test_parses_numeric_text_to_int():
    assert resolve_edp_id(_session_returning_ids("1"), LABEL) == 1


def test_no_matching_row_means_no_id():
    assert resolve_edp_id(_session_returning_ids(), LABEL) is None


@pytest.mark.parametrize("raw", [None, "", "  ", "abc", "1.5"])
def test_non_integer_values_mean_no_id(raw):
    assert resolve_edp_id(_session_returning_ids(raw), LABEL) is None


def test_a_label_with_two_ids_means_no_id():
    assert resolve_edp_id(_session_returning_ids("1", "2"), LABEL) is None


def test_queries_the_active_boundary_layer_version(active_version):
    session = _session_returning_ids("1")

    resolve_edp_id(session, LABEL)

    active_version.assert_called_once_with(session, "edp_boundary_layer")
