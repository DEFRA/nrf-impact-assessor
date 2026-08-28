"""SQLAlchemy engine factory for PostGIS connection management.

Supports both local development (static password) and CDP cloud deployment
(IAM authentication with short-lived RDS tokens).
"""

import logging
import os
import threading
import time

import boto3
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from app.common import tls
from app.config import IAM_TOKEN_POOL_RECYCLE_SECONDS, AWSConfig, DatabaseSettings

logger = logging.getLogger(__name__)

# Reuse one token across connections for 9 minutes: a token cached for up to
# 9 minutes still has 6 minutes of validity left when a connection uses it.
IAM_TOKEN_CACHE_SECONDS = 540

# libpq TCP keepalive settings.
TCP_KEEPALIVE_CONNECT_ARGS: dict = {
    "keepalives": 1,
    "keepalives_idle": 60,
    "keepalives_interval": 30,
    "keepalives_count": 5,
}

_token_cache: dict[tuple[str, int, str, str], tuple[float, str]] = {}
_token_cache_lock = threading.Lock()

_SHARED_LOCK = threading.Lock()
_SHARED_ENGINE: Engine | None = None
_SHARED_REPOSITORY = None  # type: ignore[var-annotated]

DEFAULT_SHARED_POOL_SIZE = 10
DEFAULT_SHARED_MAX_OVERFLOW = 10


def _get_iam_auth_token(settings: DatabaseSettings, region: str) -> str:
    """Return a cached IAM auth token, generating a fresh one when stale.

    Every new pooled connection asks for a token; without a cache, a burst of
    connections (pool fill, recycle expiry) generates one token each.
    Generation happens under the lock so a concurrent burst produces a single
    token rather than one per connection.
    """
    key = (settings.host, settings.port, settings.user, region)
    now = time.monotonic()
    with _token_cache_lock:
        entry = _token_cache.get(key)
        if entry is not None and now - entry[0] < IAM_TOKEN_CACHE_SECONDS:
            return entry[1]
        token = _generate_iam_auth_token(settings, region)
        _token_cache[key] = (now, token)
        return token


def _generate_iam_auth_token(settings: DatabaseSettings, region: str) -> str:
    """Generate a short-lived IAM authentication token for RDS."""
    try:
        session = boto3.Session(region_name=region)
        if session.get_credentials() is None:
            logger.warning("No AWS credentials found - token generation may fail")

        client = session.client("rds")
        token = client.generate_db_auth_token(
            DBHostname=settings.host,
            Port=settings.port,
            DBUsername=settings.user,
            Region=region,
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


def _build_ssl_connect_args(settings: DatabaseSettings, region: str) -> dict:
    """Build SSL connect_args for IAM authentication."""
    connect_args: dict = {"sslmode": settings.ssl_mode, **TCP_KEEPALIVE_CONNECT_ARGS}
    cert_path = tls.get_cert_path(settings.rds_truststore)
    if cert_path:
        connect_args["sslrootcert"] = cert_path
    else:
        logger.warning(
            "No TRUSTSTORE_%s cert found; connecting with sslmode=%s (region=%s)",
            settings.rds_truststore,
            settings.ssl_mode,
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
            """Inject a (possibly cached) IAM token before each connection."""
            cparams["password"] = _get_iam_auth_token(settings, region)

    else:
        # base_url already carries DB_LOCAL_PASSWORD (URL-encoded) when one is
        # set; with none, it is a trust-auth URL with no password at all.
        engine = create_engine(
            base_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            echo=echo,
            connect_args=connect_args,
        )

    return engine


def create_db_engine(
    settings: DatabaseSettings | None = None,
    aws_config: AWSConfig | None = None,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
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

    base_url = settings.connection_url
    connect_args = (
        _build_ssl_connect_args(settings, region)
        if settings.iam_authentication
        else dict(TCP_KEEPALIVE_CONNECT_ARGS)
    )

    return _create_pooled_engine(
        settings, region, base_url, connect_args, pool_size, max_overflow, echo
    )


def get_shared_engine(
    pool_size: int = DEFAULT_SHARED_POOL_SIZE,
    max_overflow: int = DEFAULT_SHARED_MAX_OVERFLOW,
) -> Engine:
    """Return the process-wide shared SQLAlchemy engine, creating it on first call."""
    global _SHARED_ENGINE
    if _SHARED_ENGINE is not None:
        return _SHARED_ENGINE
    with _SHARED_LOCK:
        if _SHARED_ENGINE is None:
            _SHARED_ENGINE = create_db_engine(
                DatabaseSettings(),
                pool_size=pool_size,
                max_overflow=max_overflow,
            )
        return _SHARED_ENGINE


def get_shared_repository():
    """Return the process-wide shared Repository, creating it on first call."""
    global _SHARED_REPOSITORY
    if _SHARED_REPOSITORY is not None:
        return _SHARED_REPOSITORY
    # Resolve the engine before acquiring _SHARED_LOCK — get_shared_engine
    # also takes _SHARED_LOCK and threading.Lock is non-reentrant.
    engine = get_shared_engine()
    with _SHARED_LOCK:
        if _SHARED_REPOSITORY is None:
            from app.repositories.repository import Repository

            _SHARED_REPOSITORY = Repository(engine)
        return _SHARED_REPOSITORY


def warm_shared_engine() -> None:
    """Open one connection on the shared engine to prime the IAM token cache and verify connectivity."""
    engine = get_shared_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def _discard_idle_connections(engine: Engine, count: int) -> None:
    """Drop up to ``count`` idle pooled connections, leaving their slots empty.

    ``invalidate`` closes the underlying DBAPI connection and does not open a
    replacement — the caller must re-check-out to refill the slot.
    """
    connections = []
    try:
        for _ in range(count):
            conn = engine.connect()
            connections.append(conn)
            conn.invalidate()
    finally:
        for conn in connections:
            conn.close()


def refresh_shared_engine_pool(max_slots: int | None = None) -> None:
    """Replace up to ``max_slots`` idle pooled connections with fresh ones.

    Pinging alone is not enough: ``pool_recycle`` measures age from creation, so
    a pinged connection still aged out and reconnected on some later checkout —
    a request's, if one arrived first. Replacing every tick keeps a warm slot no
    older than one interval, so it never reaches the recycle threshold.

    Connections are held simultaneously — checking one out and returning it in
    a loop would reuse the same connection and leave the rest cold. Checked-out
    slots are skipped: they are warm already, and going past ``pool_size`` would
    only create overflow connections, which are discarded on return.

    ``pool.size()`` is the configured maximum, not the live connection count, so
    running uncapped pins ``pool_size`` connections per process — a floor that
    multiplies by replica count when the service autoscales.

    Every idle connection is discarded, not just the ``max_slots`` replaced:
    cycling a burst-expanded pool a slice at a time takes 800s at the default
    10/3/240s settings, past ``pool_recycle``. Dropping the excess holds the
    pool at the warm-slot floor instead.
    """
    engine = get_shared_engine()
    pool = engine.pool
    idle_slots = pool.size() - pool.checkedout()
    if max_slots is not None:
        idle_slots = min(idle_slots, max_slots)
    if idle_slots <= 0:
        return

    # Only existing connections need discarding; empty slots are filled below.
    _discard_idle_connections(engine, pool.checkedin())

    connections = []
    try:
        for _ in range(idle_slots):
            conn = engine.connect()
            connections.append(conn)
            conn.execute(text("SELECT 1"))
    finally:
        for conn in connections:
            conn.close()
