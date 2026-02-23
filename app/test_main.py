from fastapi.testclient import TestClient

from .main import app

client = TestClient(app)


def test_lifespan(mocker):
    mock_mongo_client = mocker.AsyncMock()
    mock_get_mongo = mocker.patch("app.main.get_mongo_client", return_value=mock_mongo_client)

    # Using TestClient as a context manager triggers lifespan startup/shutdown
    with TestClient(app):
        mock_get_mongo.assert_called_once()  # Startup: connect called

    mock_mongo_client.close.assert_awaited_once()  # Shutdown: close called


def test_example():
    response = client.get("/example/test")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_root():
    response = client.get("/")
    assert response.status_code == 404
