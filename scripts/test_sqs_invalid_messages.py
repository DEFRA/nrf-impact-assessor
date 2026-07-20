#!/usr/bin/env python

"""Exercise SQSClient's invalid-message handling against LocalStack.

Drives every failure path in `SQSClient.receive_messages()` end to end: sends
each crafted message to a local SQS queue, then calls the real `receive_messages()`
and asserts each message was accepted or rejected as expected. Rejections are
identified by the ERROR/EXCEPTION log records SQSClient emits (which carry the
originating `message_id`), so this tests the actual production code path rather
than re-implementing the checks.

Prerequisites:
    LocalStack running (e.g. `make up`).

Usage:
    python scripts/test_sqs_invalid_messages.py

Environment overrides:
    AWS_ENDPOINT_URL   default http://localhost:4568
    SQS_QUEUE_NAME     default nrf-impact-assessment-jobs
    AWS_REGION         default eu-west-2

Exit code is 0 only if every case behaved as expected.
"""

import contextlib
import json
import logging
import os
import sys
import uuid
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.aws.sqs import SQSClient  # noqa: E402

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4568")
QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME", "nrf-impact-assessment-jobs")
REGION = os.environ.get("AWS_REGION", "eu-west-2")

# LocalStack accepts any credentials, but boto3 requires them to be present.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

_VALID_GEOM = {
    "type": "Polygon",
    "coordinates": [
        [
            [582814.93, 328188.89],
            [582808.89, 328203.73],
            [582824.96, 328210.17],
            [582830.09, 328197.00],
            [582814.93, 328188.89],
        ]
    ],
}


def _valid_job(**overrides) -> dict:
    job = {
        "reference": "NRF-000001",
        "boundaryGeojson": {
            "boundaryGeometryOriginal": json.loads(json.dumps(_VALID_GEOM)),
            "intersectingEdps": [
                {"label": "River Wensum SAC", "n2k_site_name": "River Wensum SAC"}
            ],
        },
        "developmentTypes": ["housing"],
        "residentialBuildingCount": 25,
    }
    job.update(overrides)
    return job


def _job_with_crs(crs_name: str) -> dict:
    job = _valid_job()
    job["boundaryGeojson"]["boundaryGeometryOriginal"]["crs"] = {
        "type": "name",
        "properties": {"name": crs_name},
    }
    return job


def _bowtie_job() -> dict:
    job = _valid_job()
    job["boundaryGeojson"]["boundaryGeometryOriginal"]["coordinates"] = [
        [[0, 0], [0, 10], [10, 0], [10, 10], [0, 0]]
    ]
    return job


# name, raw body string, expected outcome at the SQSClient layer.
# "rejected" -> receive_messages() drops it (retry -> DLQ).
# "accepted" -> receive_messages() returns it (may still fail downstream).
CASES: list[tuple[str, str, str]] = [
    ("invalid-json-body", "not json {", "rejected"),
    (
        "fails-schema-validation",
        json.dumps({"reference": "BAD-REF", "residentialBuildingCount": 0}),
        "rejected",
    ),
    (
        "sns-broken-inner-message",
        json.dumps({"Type": "Notification", "Message": "not-valid-json{"}),
        "rejected",
    ),
    (
        "unsupported-crs",
        json.dumps(_job_with_crs("urn:ogc:def:crs:EPSG::123")),
        "rejected",
    ),
    (
        "supported-crs",
        json.dumps(_job_with_crs("urn:ogc:def:crs:EPSG::27700")),
        "accepted",
    ),
    # Passes the SQS layer; its self-intersection fails later in the assessment.
    ("bowtie-geometry", json.dumps(_bowtie_job()), "accepted"),
    ("valid-control", json.dumps(_valid_job()), "accepted"),
]


class _RejectionCapture(logging.Handler):
    """Collect the message_id of every WARNING+ record SQSClient emits."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.rejected_ids: set[str] = set()

    def emit(self, record: logging.LogRecord) -> None:
        message_id = getattr(record, "message_id", None)
        if message_id:
            self.rejected_ids.add(message_id)


def main() -> int:
    sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT_URL)

    # Use a throwaway queue per run so leftover messages from a previous run
    # can't skew the result. purge_queue is rate-limited (~60s) and rejected
    # messages are intentionally not deleted, so a shared queue is unreliable;
    # an ephemeral queue we delete at the end sidesteps both problems.
    queue_name = f"{QUEUE_NAME}-invtest-{uuid.uuid4().hex[:8]}"
    try:
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: cannot reach LocalStack SQS at {ENDPOINT_URL}: {exc}")
        print("Is LocalStack running? Try `make up`.")
        return 2

    # Send every case and remember which MessageId maps to which case.
    id_to_case: dict[str, str] = {}
    for name, body, _expected in CASES:
        resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
        id_to_case[resp["MessageId"]] = name

    capture = _RejectionCapture()
    logger = logging.getLogger("app.aws.sqs")
    logger.addHandler(capture)
    logger.setLevel(logging.WARNING)

    client = SQSClient(
        queue_url=queue_url,
        region=REGION,
        wait_time_seconds=1,
        visibility_timeout=2,
        max_messages=10,
        endpoint_url=ENDPOINT_URL,
    )

    # Drain the queue. Rejected messages are not deleted, so we delete accepted
    # ones here and rely on rejection logs to classify the rest. A few rounds
    # guarantee every message is delivered at least once.
    for _ in range(6):
        results = client.receive_messages()
        for _job, receipt_handle in results:
            client.delete_message(receipt_handle)

    logger.removeHandler(capture)

    print(f"\nSQSClient invalid-message check against {ENDPOINT_URL}\n")
    failures = 0
    for message_id, name in sorted(id_to_case.items(), key=lambda kv: kv[1]):
        expected = next(exp for n, _b, exp in CASES if n == name)
        actual = "rejected" if message_id in capture.rejected_ids else "accepted"
        ok = actual == expected
        failures += not ok
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name:<26} expected={expected:<9} actual={actual}")

    # Tear down the throwaway queue (removes any undeleted rejected messages).
    with contextlib.suppress(Exception):
        sqs.delete_queue(QueueUrl=queue_url)

    print()
    if failures:
        print(f"{failures} case(s) behaved unexpectedly.")
        return 1
    print("All cases behaved as expected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
