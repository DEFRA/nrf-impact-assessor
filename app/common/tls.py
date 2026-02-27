import base64
import os
import ssl
import tempfile
from logging import getLogger

logger = getLogger(__name__)

custom_ca_certs: dict[str, str] = {}
ctx: ssl.SSLContext | None = None

_PEM_PREFIX = b"-----BEGIN "


# Custom CA Certificates are passed to services on deployment
# as base64 encoded environment variables with the prefix `TRUSTSTORE_`
def extract_all_certs():
    certs = {}
    for var_name, var_value in os.environ.items():
        if var_name.startswith("TRUSTSTORE_"):
            try:
                decoded_value = base64.b64decode(var_value)
            except base64.binascii.Error as err:
                logger.error("Error decoding value for %s. Skipping. %s", var_name, err)
                continue
            if not decoded_value.strip().startswith(_PEM_PREFIX):
                logger.error("Value for %s is not a valid PEM certificate. Skipping.", var_name)
                continue
            with tempfile.NamedTemporaryFile(
                mode="wb", delete=False, prefix=var_name, suffix=".pem"
            ) as tmp_file:
                tmp_file.write(decoded_value)
                certs[var_name] = tmp_file.name
                logger.debug("Wrote %s to temp file", var_name)
    logger.info("Loaded %d custom certificates", len(certs))
    return certs


def load_certs_into_context(certs):
    context = ssl.create_default_context()
    for key, path in certs.items():
        try:
            context.load_verify_locations(path)
            logger.info("Added %s to truststore", key)
        except ssl.SSLError as err:
            logger.error("Failed to load cert %s: %s", key, err)
            raise
    return context


def cleanup_cert_files():
    for var_name, path in custom_ca_certs.items():
        try:
            os.unlink(path)
            logger.debug("Removed temp cert file for %s", var_name)
        except OSError as err:
            logger.warning("Failed to remove temp cert file for %s: %s", var_name, err)
    custom_ca_certs.clear()


def init_custom_certificates():
    global ctx
    logger.info("Initializing custom certificates")
    custom_ca_certs.update(extract_all_certs())
    ctx = load_certs_into_context(custom_ca_certs)