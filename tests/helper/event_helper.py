import json
import time
import uuid
from typing import Any

from config import get_logger
from tests.config import THINGS_REPORT_JOB_FILE_PATH_PREFIX, QUEUE_WAIT_SECONDS
from util.s3_util import isodate_to_timestamp
from tests.helper.helper import validate_uuid4
from things_report_archive_service.service import ThingsReportArchiveService

log = get_logger()


async def event_consumer(event_queue: Any, timeout_seconds=0) -> list[dict]:
    timeout = time.time() + timeout_seconds
    messages = []

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break

        event_messages = event_queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=QUEUE_WAIT_SECONDS,
        )

        for event_message in event_messages:
            log.info(f"TEST EVENT CONSUMER event_message: {event_message=}")

            messages.append(event_message)

            event_message.delete()

    log.info(f"TEST EVENT CONSUMER messages: {messages=}")

    return messages


def assert_event_message(actual_result: dict, expected_result: dict) -> None:
    assert validate_uuid4(actual_result["Id"])
    assert validate_uuid4(expected_result["Id"])
    assert actual_result["Id"] != expected_result["Id"]

    assert actual_result["UserId"] == expected_result["UserId"]
    assert actual_result["ReportName"] == expected_result["ReportName"]
    assert actual_result["JobPath"] == expected_result["JobPath"]
    assert actual_result["JobUploadPath"] == expected_result["JobUploadPath"]
