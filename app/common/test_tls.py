import base64
import ssl

import pytest

from app.common.tls import (
    cleanup_cert_files,
    extract_all_certs,
    init_custom_certificates,
    load_certs_into_context,
)


class TestExtractAllCerts:
    def test_extract_valid_pem_cert(self, mocker, monkeypatch, tmp_path):
        cert_content = b"-----BEGIN CERTIFICATE-----\ndata\n-----END CERTIFICATE-----"
        encoded_cert = base64.b64encode(cert_content).decode()
        monkeypatch.setenv("TRUSTSTORE_CERT1", encoded_cert)

        cert_path = tmp_path / "cert1.pem"

        mock_named_temp_file = mocker.patch(
            "app.common.tls.tempfile.NamedTemporaryFile"
        )
        mock_file_obj = mocker.MagicMock()
        mock_file_obj.name = str(cert_path)
        mock_named_temp_file.return_value.__enter__.return_value = mock_file_obj

        certs = extract_all_certs()

        assert len(certs) == 1
        assert certs["TRUSTSTORE_CERT1"] == str(cert_path)
        mock_file_obj.write.assert_called_once_with(cert_content)

    def test_extract_invalid_base64_cert(self, monkeypatch):
        monkeypatch.setenv("TRUSTSTORE_BAD", "invalid-base64!")

        certs = extract_all_certs()
        assert len(certs) == 0

    def test_extract_non_pem_content(self, monkeypatch):
        non_pem = base64.b64encode(b"not a certificate").decode()
        monkeypatch.setenv("TRUSTSTORE_BAD", non_pem)

        certs = extract_all_certs()
        assert len(certs) == 0

    def test_extract_no_truststore_vars(self, monkeypatch):
        monkeypatch.setenv("NORMAL_VAR", "value")

        certs = extract_all_certs()
        assert len(certs) == 0


class TestLoadCertsIntoContext:
    def test_load_valid_certs(self, mocker):
        mock_create_context = mocker.patch("app.common.tls.ssl.create_default_context")
        mock_ctx = mocker.MagicMock()
        mock_create_context.return_value = mock_ctx

        certs = {
            "TRUSTSTORE_1": "/path/to/cert1.pem",
            "TRUSTSTORE_2": "/path/to/cert2.pem",
        }

        ctx = load_certs_into_context(certs)

        assert ctx == mock_ctx
        assert mock_ctx.load_verify_locations.call_count == 2
        mock_ctx.load_verify_locations.assert_any_call("/path/to/cert1.pem")
        mock_ctx.load_verify_locations.assert_any_call("/path/to/cert2.pem")

    def test_load_certs_error_raises(self, mocker):
        mock_create_context = mocker.patch("app.common.tls.ssl.create_default_context")
        mock_ctx = mocker.MagicMock()
        mock_create_context.return_value = mock_ctx
        mock_ctx.load_verify_locations.side_effect = ssl.SSLError("Bad cert")

        certs = {"TRUSTSTORE_BAD": "/path/to/bad.pem"}

        with pytest.raises(ssl.SSLError):
            load_certs_into_context(certs)


class TestCleanupCertFiles:
    def test_cleanup_removes_files(self, tmp_path):
        from app.common import tls

        cert_file = tmp_path / "test.pem"
        cert_file.write_text("test")

        tls.custom_ca_certs.update({"TRUSTSTORE_TEST": str(cert_file)})

        cleanup_cert_files()

        assert not cert_file.exists()
        assert len(tls.custom_ca_certs) == 0

    def test_cleanup_handles_missing_files(self):
        from app.common import tls

        tls.custom_ca_certs.update({"TRUSTSTORE_GONE": "/nonexistent/path.pem"})

        cleanup_cert_files()
        assert len(tls.custom_ca_certs) == 0


class TestInitCustomCertificates:
    def test_init_globals(self, mocker):
        from app.common import tls

        mock_extract = mocker.patch("app.common.tls.extract_all_certs")
        mock_load = mocker.patch("app.common.tls.load_certs_into_context")

        mock_certs = {"cert": "path"}
        mock_ctx = mocker.MagicMock()

        mock_extract.return_value = mock_certs
        mock_load.return_value = mock_ctx

        init_custom_certificates()

        mock_extract.assert_called_once()
        mock_load.assert_called_once()

        assert tls.custom_ca_certs == mock_certs
        assert tls.ctx == mock_ctx

        # Clean up global state
        tls.custom_ca_certs.clear()
