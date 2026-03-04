"""Main SQS consumer process for job polling."""

import os

# Limit BLAS threads to avoid oversubscription with ProcessPoolExecutor.
# Each spatial worker process gets 1 BLAS thread; combined with the 80% CPU cap
# on worker processes this keeps total CPU usage at ~80%.
# Must be set before numpy is imported.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import json
import logging
import logging.config
import multiprocessing
import signal
import sys
import time
from pathlib import Path

import uvicorn
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.aws.sqs import SQSClient
from app.common.proxy_utils import configure_proxy_settings
from app.common.tls import init_custom_certificates
from app.config import ApiServerConfig, AWSConfig, DatabaseSettings
from app.orchestrator import JobOrchestrator
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository


class WorkerConfig:
    """Worker polling configuration loaded from SQS_ prefixed env vars."""

    def __init__(self):
        self.wait_time_seconds = int(os.environ.get("SQS_WAIT_TIME_SECONDS", "20"))
        self.visibility_timeout = int(os.environ.get("SQS_VISIBILITY_TIMEOUT", "300"))
        self.max_messages = int(os.environ.get("SQS_MAX_MESSAGES", "1"))


def is_running_in_ecs() -> bool:
    """Detect if running in AWS ECS (CDP environment).

    ECS automatically injects metadata URI environment variables into containers.
    These are always present in ECS and never present locally.
    """
    return bool(
        os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        or os.environ.get("ECS_CONTAINER_METADATA_URI")
    )


def configure_logging() -> None:
    """Configure logging based on environment.

    In ECS/CDP: Uses logging.json with ECS-compatible structured logging,
    trace ID injection, and health check filtering.

    Locally: Uses logging-dev.json with simple text format for readability.
    """
    config_file = "logging.json" if is_running_in_ecs() else "logging-dev.json"
    config_path = Path(__file__).parent.parent / config_file

    if config_path.exists():
        with open(config_path) as f:
            logging.config.dictConfig(json.load(f))
    else:
        # Fallback to basic config if file not found
        logging.basicConfig(
            level=logging.INFO,
            format=(
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"logger": "%(name)s", "message": "%(message)s"}'
            ),
            datefmt="%Y-%m-%dT%H:%M:%S",
        )


configure_logging()
logger = logging.getLogger(__name__)


# Configure and log proxy settings early for connectivity
configure_proxy_settings()


def run_api_server(port: int) -> None:
    """Run the API server in a separate process.

    Uses uvicorn as an ASGI server to serve the FastAPI app.
    The API server provides health check and job submission endpoints.

    Args:
        port: The port to listen on for API requests.
    """
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, log_level="warning")


class SqsConsumer:
    """Long-running SQS consumer that polls for and processes jobs."""

    def __init__(self, sqs_client: SQSClient, orchestrator: JobOrchestrator):
        self.sqs_client = sqs_client
        self.orchestrator = orchestrator
        self.running = True

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigint)

    def run(self) -> None:
        """Main polling loop."""
        logger.info("SQS consumer started, polling for jobs...")

        while self.running:
            try:
                results = self.sqs_client.receive_messages()

                if not results:
                    continue

                for job_message, receipt_handle in results:
                    logger.info(f"Processing job: {job_message.job_id}")
                    # Pass the assessment_type from the job message
                    self.orchestrator.process_job(
                        job_message, job_message.assessment_type
                    )

                    self.sqs_client.delete_message(receipt_handle)
                    logger.info(
                        f"Job {job_message.job_id} processing complete, message deleted from queue"
                    )

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in consumer loop: {e}")
                time.sleep(5)

        logger.info("SQS consumer stopped")

    def _handle_sigterm(self, _signum, _frame):
        """Handle SIGTERM for graceful ECS task shutdown."""
        logger.info("Received SIGTERM, initiating graceful shutdown...")
        self.running = False

    def _handle_sigint(self, _signum, _frame):
        """Handle SIGINT (Ctrl+C) for local testing."""
        logger.info("Received SIGINT, initiating graceful shutdown...")
        self.running = False


def check_database_connection(
    db_settings: DatabaseSettings, aws_config: AWSConfig | None = None
) -> bool:
    """Check if the database is accessible.

    Attempts to connect and execute a simple query using the same
    connection pattern as the main engine (QueuePool with do_connect event).

    Returns True if successful, False otherwise.
    Logs warnings on failure but does not raise exceptions.

    Args:
        db_settings: Database connection settings.
        aws_config: AWS configuration (needed for IAM auth region).
    """
    try:
        engine = create_db_engine(db_settings, aws_config, pool_size=1, max_overflow=0)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        logger.info("Database connection check: OK")
        return True
    except SQLAlchemyError as e:
        logger.warning(f"Database connection check failed: {e}")
        return False
    except Exception as e:
        logger.warning(f"Database connection check failed with unexpected error: {e}")
        return False


def main():
    """Main entry point for the SQS consumer worker."""
    api_server_process = None

    try:
        # Initialize TLS certificates before any connections
        init_custom_certificates()

        aws_config = AWSConfig()
        worker_config = WorkerConfig()
        api_config = ApiServerConfig()
        db_settings = DatabaseSettings()

        # Check database connectivity early
        check_database_connection(db_settings, aws_config)

        logger.info("Initializing worker components...")

        # Start API server in separate process for health checks and job submission
        api_server_process = multiprocessing.Process(
            target=run_api_server,
            args=(api_config.port,),
            daemon=True,
        )
        api_server_process.start()
        logger.info(f"API server started on port {api_config.port}")
        if api_config.testing_enabled:
            logger.info("API_TESTING_ENABLED=true: test endpoints enabled at /test/*")
        else:
            logger.info("API_TESTING_ENABLED=false: test endpoints disabled")

        # Initialize PostGIS repository (ONCE - reused across jobs)
        # Uses IAM authentication in CDP cloud, static password locally
        engine = create_db_engine(db_settings, aws_config)
        repository = Repository(engine)

        sqs_client = SQSClient(
            queue_url=aws_config.sqs_queue_url,
            region=aws_config.region,
            wait_time_seconds=worker_config.wait_time_seconds,
            visibility_timeout=worker_config.visibility_timeout,
            max_messages=worker_config.max_messages,
            endpoint_url=aws_config.endpoint_url,
        )

        orchestrator = JobOrchestrator(
            aws_config=aws_config,
            repository=repository,
        )

        consumer = SqsConsumer(sqs_client=sqs_client, orchestrator=orchestrator)
        consumer.run()

    except Exception as e:
        logger.exception(f"Worker failed to start: {e}")
        sys.exit(1)

    finally:
        # Explicit cleanup of API server process
        if api_server_process is not None and api_server_process.is_alive():
            logger.info("Terminating API server...")
            api_server_process.terminate()
            api_server_process.join(timeout=5)


if __name__ == "__main__":
    main()
