from unittest.mock import MagicMock

from app.data_sync.service import _cleanup_old_versions


def test_cleanup_runs_delete_per_table_and_commits():
    session = MagicMock()
    _cleanup_old_versions(session, ["nn_catchments", "coefficient_layer"])

    # one execute + one commit per table
    assert session.execute.call_count == 2
    assert session.commit.call_count == 2
    sql_texts = [str(call.args[0]) for call in session.execute.call_args_list]
    assert any("DELETE FROM public.nn_catchments" in s for s in sql_texts)
    assert any("DELETE FROM public.coefficient_layer" in s for s in sql_texts)


def test_cleanup_is_best_effort_and_continues_after_failure():
    session = MagicMock()
    # First table raises, second must still run.
    session.execute.side_effect = [RuntimeError("boom"), None]

    _cleanup_old_versions(session, ["nn_catchments", "coefficient_layer"])

    assert session.execute.call_count == 2
    session.rollback.assert_called_once()
    # The second table still committed.
    assert session.commit.call_count == 1
