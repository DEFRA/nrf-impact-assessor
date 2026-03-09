"""SQLAlchemy engine factory for PostGIS connection management.

Supports both local development (static password) and CDP cloud deployment
(IAM authentication with short-lived RDS tokens).
"""

import logging
import os

import boto3
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool

from app.common import tls
from app.config import AWSConfig, DatabaseSettings

logger = logging.getLogger(__name__)

# Token lifetime is 15 minutes; recycle connections at 10 minutes
# to ensure fresh tokens before expiry
IAM_TOKEN_POOL_RECYCLE_SECONDS = 600


def _get_iam_auth_token(settings: DatabaseSettings, region: str) -> str:
    """Generate a short-lived IAM authentication token for RDS."""
    logger.info(
        "Requesting IAM auth token: host=%s, port=%d, user=%s, region=%s",
        settings.host,
        settings.port,
        settings.user,
        region,
    )
    try:
        session = boto3.Session(region_name=region)
        credentials = session.get_credentials()

        if credentials:
            frozen_credentials = credentials.get_frozen_credentials()
            logger.info(
                "AWS credentials found: access_key_id=%s..., method=%s",
                frozen_credentials.access_key[:8]
                if frozen_credentials.access_key
                else "None",
                credentials.method,
            )
        else:
            logger.warning("No AWS credentials found - token generation may fail")

        client = session.client("rds")
        token = client.generate_db_auth_token(
            DBHostname=settings.host,
            Port=settings.port,
            DBUsername=settings.user,
            Region=region,
        )
        logger.info(
            "Successfully generated IAM auth token (length=%d chars)", len(token)
        )
        return token
    except Exception:
        logger.exception(
            "Failed to generate IAM auth token for host=%s, user=%s, region=%s",
            settings.host,
            settings.user,
            region,
        )
        raise


def _get_password(settings: DatabaseSettings, region: str) -> str:
    """Get the appropriate password based on authentication mode."""
    if settings.iam_authentication:
        return _get_iam_auth_token(settings, region)
    return settings.local_password


def _build_ssl_connect_args(settings: DatabaseSettings, region: str) -> dict:
    """Build SSL connect_args for IAM authentication."""
    connect_args: dict = {"sslmode": settings.ssl_mode}
    cert_path = tls.get_cert_path(settings.rds_truststore)
    if cert_path:
        connect_args["sslrootcert"] = cert_path
        logger.info(
            "SSL enabled: sslmode=%s, sslrootcert=%s (from TRUSTSTORE_%s, region=%s)",
            settings.ssl_mode,
            cert_path,
            settings.rds_truststore,
            region,
        )
    else:
        logger.info(
            "SSL enabled: sslmode=%s, no TRUSTSTORE_%s cert found (region=%s)",
            settings.ssl_mode,
            settings.rds_truststore,
            region,
        )
    return connect_args


def _create_pooled_engine(
    settings: DatabaseSettings,
    region: str,
    base_url: str,
    connect_args: dict,
    pool_size: int,
    max_overflow: int,
    echo: bool,
) -> Engine:
    """Create a QueuePool engine for IAM or local authentication."""
    pool_recycle = (
        IAM_TOKEN_POOL_RECYCLE_SECONDS if settings.iam_authentication else None
    )

    if settings.iam_authentication:
        engine = create_engine(
            base_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
            echo=echo,
            connect_args=connect_args,
        )

        @event.listens_for(engine, "do_connect")
        def provide_token(_dialect, _conn_rec, _cargs, cparams):
            """Inject fresh IAM token before each connection."""
            logger.debug("do_connect event: requesting fresh IAM token")
            cparams["password"] = _get_iam_auth_token(settings, region)
            logger.debug("do_connect event: IAM token injected into connection params")

        logger.info(
            "Created engine with IAM authentication: pool_size=%d, max_overflow=%d, pool_recycle=%ds",
            pool_size,
            max_overflow,
            pool_recycle,
        )
    else:
        password = settings.local_password
        if password:
            url_with_password = base_url.replace(
                f"{settings.user}@", f"{settings.user}:{password}@"
            )
            logger.debug("Using static password for local authentication")
        else:
            url_with_password = base_url
            logger.debug("No password configured (using trust authentication)")

        engine = create_engine(
            url_with_password,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            echo=echo,
            connect_args=connect_args,
        )
        logger.info(
            "Created engine with local authentication: pool_size=%d, max_overflow=%d",
            pool_size,
            max_overflow,
        )

    return engine


def create_db_engine(
    settings: DatabaseSettings | None = None,
    aws_config: AWSConfig | None = None,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
    use_null_pool: bool = False,
) -> Engine:
    """Create a SQLAlchemy engine from database settings.

    Supports two authentication modes:
    1. Local development: Uses static password from DB_LOCAL_PASSWORD
    2. CDP Cloud (IAM): Uses short-lived tokens from AWS RDS
    """
    if settings is None:
        settings = DatabaseSettings()

    region = (
        aws_config.region if aws_config else os.environ.get("AWS_REGION", "eu-west-2")
    )

    logger.info(
        "Configuring database connection: host=%s, port=%d, database=%s, user=%s, iam_auth=%s",
        settings.host,
        settings.port,
        settings.database,
        settings.user,
        settings.iam_authentication,
    )

    base_url = settings.connection_url
    connect_args = (
        _build_ssl_connect_args(settings, region) if settings.iam_authentication else {}
    )

    if use_null_pool:
        logger.info("Generating authentication token for connection check")
        password = _get_password(settings, region)
        url_with_password = base_url.replace(
            f"{settings.user}@", f"{settings.user}:{password}@"
        )
        engine = create_engine(
            url_with_password, poolclass=NullPool, echo=echo, connect_args=connect_args
        )
        logger.info("Created engine with NullPool for connection check")
    else:
        engine = _create_pooled_engine(
            settings, region, base_url, connect_args, pool_size, max_overflow, echo
        )

    return engine
