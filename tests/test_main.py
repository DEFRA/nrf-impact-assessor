from fastapi.testclient import TestClient

from app.main import app


def test_lifespan(mocker):
    mock_mongo_client = mocker.AsyncMock()
    mock_get_mongo = mocker.patch(
        "app.main.get_mongo_client", return_value=mock_mongo_client
    )
    mock_init_certs = mocker.patch("app.main.init_custom_certificates")
    mock_cleanup = mocker.patch("app.main.cleanup_cert_files")

    # Using TestClient as a context manager triggers lifespan startup/shutdown
    with TestClient(app):
        mock_init_certs.assert_called_once()  # Startup: certs initialized
        mock_get_mongo.assert_called_once()  # Startup: connect called

    mock_mongo_client.close.assert_awaited_once()  # Shutdown: close called
    mock_cleanup.assert_called_once()  # Shutdown: cert files cleaned up


def test_example(mocker):
    mocker.patch("app.main.init_custom_certificates")
    mocker.patch("app.main.cleanup_cert_files")
    mocker.patch("app.main.get_mongo_client", return_value=mocker.AsyncMock())

    with TestClient(app) as client:
        response = client.get("/example/test")
        assert response.status_code == 200
        assert response.json() == {"ok": True}


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
