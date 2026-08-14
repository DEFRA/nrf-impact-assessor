from fastapi.testclient import TestClient

from app.main import app


def test_lifespan(mocker):
    mock_mongo_client = mocker.AsyncMock()
    mock_get_mongo = mocker.patch(
        "app.main.get_mongo_client", return_value=mock_mongo_client
    )
    mock_init_certs = mocker.patch("app.main.init_custom_certificates")
    mock_cleanup = mocker.patch("app.main.cleanup_cert_files")

    mock_table_status = mocker.patch("app.main.log_startup_table_status")
    mock_start_keepalive = mocker.patch("app.main.start_keepalive")
    mock_stop_keepalive = mocker.patch("app.main.stop_keepalive")

    # Using TestClient as a context manager triggers lifespan startup/shutdown
    with TestClient(app):
        mock_init_certs.assert_called_once()  # Startup: certs initialized
        mock_get_mongo.assert_called_once()  # Startup: connect called
        mock_table_status.assert_called_once()  # Startup: reference tables logged
        mock_start_keepalive.assert_called_once()  # Startup: keepalive started

    mock_mongo_client.close.assert_awaited_once()  # Shutdown: close called
    mock_cleanup.assert_called_once()  # Shutdown: cert files cleaned up
    mock_stop_keepalive.assert_awaited_once_with(mock_start_keepalive.return_value)


def test_lifespan_keeps_certs_when_a_refresh_is_abandoned(mocker):
    """An abandoned refresh thread may still be opening TLS connections."""
    mocker.patch("app.main.init_custom_certificates")
    mocker.patch("app.main.get_mongo_client", return_value=mocker.AsyncMock())
    mocker.patch("app.main.log_startup_table_status")
    mocker.patch("app.main.start_keepalive")
    mocker.patch("app.main.stop_keepalive", return_value=False)
    mock_cleanup = mocker.patch("app.main.cleanup_cert_files")

    with TestClient(app):
        pass

    mock_cleanup.assert_not_called()


def test_lifespan_starts_keepalive_at_configured_interval(mocker):
    mocker.patch("app.main.init_custom_certificates")
    mocker.patch("app.main.cleanup_cert_files")
    mocker.patch("app.main.get_mongo_client", return_value=mocker.AsyncMock())
    mocker.patch("app.main.log_startup_table_status")
    mocker.patch("app.main.stop_keepalive")
    mock_start_keepalive = mocker.patch("app.main.start_keepalive")

    with TestClient(app):
        pass

    from app.config import DatabaseSettings

    settings = DatabaseSettings()
    mock_start_keepalive.assert_called_once_with(
        settings.keepalive_interval_seconds, settings.keepalive_warm_slots
    )


def test_health(mocker):
    mocker.patch("app.main.init_custom_certificates")
    mocker.patch("app.main.cleanup_cert_files")
    mocker.patch("app.main.get_mongo_client", return_value=mocker.AsyncMock())

    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_root(mocker):
    mocker.patch("app.main.init_custom_certificates")
    mocker.patch("app.main.cleanup_cert_files")
    mocker.patch("app.main.get_mongo_client", return_value=mocker.AsyncMock())

    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 404
