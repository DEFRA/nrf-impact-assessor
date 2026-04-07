"""Unit tests for backend client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.clients.backend_client import BackendClient


@pytest.fixture
def mock_http_client():
    with patch("app.clients.backend_client.create_client") as mock_create:
        client = MagicMock(spec=httpx.Client)
        mock_create.return_value = client
        yield client


def _make_response(status_code, text=""):
    response = httpx.Response(
        status_code=status_code,
        request=httpx.Request("PATCH", "https://test/quotes/NRF-000001"),
        text=text,
    )
    return response


class TestPatchQuote:
    def test_successful_patch(self, mock_http_client):
        mock_http_client.patch.return_value = _make_response(200)
        client = BackendClient(base_url="https://test", max_retries=0)

        client.patch_quote("NRF-000001", {"edps": []})

        mock_http_client.patch.assert_called_once_with(
            "https://test/quotes/NRF-000001", json={"edps": []}
        )

    def test_404_not_retried(self, mock_http_client):
        mock_http_client.patch.return_value = _make_response(404, "Not Found")

        client = BackendClient(base_url="https://test", max_retries=3)

        with pytest.raises(httpx.HTTPStatusError):
            client.patch_quote("NRF-000001", {"edps": []})

        assert mock_http_client.patch.call_count == 1

    def test_400_not_retried(self, mock_http_client):
        mock_http_client.patch.return_value = _make_response(400, "Bad Request")

        client = BackendClient(base_url="https://test", max_retries=3)

        with pytest.raises(httpx.HTTPStatusError):
            client.patch_quote("NRF-000001", {"edps": []})

        assert mock_http_client.patch.call_count == 1

    @patch("app.clients.backend_client.time.sleep")
    def test_500_retried(self, mock_sleep, mock_http_client):
        mock_http_client.patch.side_effect = [
            _make_response(500, "Server Error"),
            _make_response(200),
        ]

        client = BackendClient(base_url="https://test", max_retries=3)
        client.patch_quote("NRF-000001", {"edps": []})

        assert mock_http_client.patch.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch("app.clients.backend_client.time.sleep")
    def test_transport_error_retried(self, mock_sleep, mock_http_client):
        mock_http_client.patch.side_effect = [
            httpx.ConnectError("Connection refused"),
            _make_response(200),
        ]

        client = BackendClient(base_url="https://test", max_retries=3)
        client.patch_quote("NRF-000001", {"edps": []})

        assert mock_http_client.patch.call_count == 2

    @patch("app.clients.backend_client.time.sleep")
    def test_max_retries_exceeded(self, mock_sleep, mock_http_client):
        mock_http_client.patch.return_value = _make_response(500, "Server Error")

        client = BackendClient(base_url="https://test", max_retries=2)

        with pytest.raises(httpx.HTTPStatusError):
            client.patch_quote("NRF-000001", {"edps": []})

        # 1 initial + 2 retries = 3 calls
        assert mock_http_client.patch.call_count == 3

    def test_base_url_trailing_slash_stripped(self, mock_http_client):
        mock_http_client.patch.return_value = _make_response(200)
        client = BackendClient(base_url="https://test/", max_retries=0)

        client.patch_quote("NRF-000001", {"edps": []})

        mock_http_client.patch.assert_called_once_with(
            "https://test/quotes/NRF-000001", json={"edps": []}
        )
