import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, parse_qs, ParseResult

from config import get_logger, THINGS_REPORT_JOB_BUCKET_NAME
from tests.config import QUEUE_WAIT_SECONDS
from util.s3_util import create_presigned_url
from tests.helper.helper import validate_uuid4

log = get_logger()


def event_consumer(event_queue: Any, timeout_seconds=0) -> list[dict]:
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


def create_event_message(
    s3_client: Any, name: str, event: str, message: str, job_upload_path: str
):
    message_id = str(uuid.uuid4())
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    # TODO move and pass into function
    presigned_url = create_presigned_url(
        bucket_name=THINGS_REPORT_JOB_BUCKET_NAME,
        object_name=f"{job_upload_path}.zip",
        s3_client=s3_client,
    )
    log.info(f"presigned_url {presigned_url=}")

    event_type = "notification"
    description = "Report Archive Notification"
    read = "False"

    return {
        "Id": message_id,
        "Name": name,
        "Date": timestamp,
        "Type": event_type,
        "Event": event,
        "Description": description,
        "Message": message,
        "Value": presigned_url,
        "Read": read,
    }


def get_uri(url: str) -> str:
    index = url.find("?")
    log.info(f"GET URI ? INDEX {index=}")

    return url[0:index]


def get_querystring_value(url: ParseResult, key: str) -> str:
    return parse_qs(url.query)[key][0]


def assert_querystring_value(
    actual_url: ParseResult, expected_url: ParseResult, key: str
):
    assert get_querystring_value(actual_url, key) == get_querystring_value(
        expected_url, key
    )


def assert_querystring_value_length(
    actual_url: ParseResult, expected_url: ParseResult, key: str
):
    expected_result = get_querystring_value(expected_url, key)
    actual_result = get_querystring_value(actual_url, key)

    assert len(actual_result) == len(expected_result)


def assert_url(actual_url, expected_url):
    assert get_uri(actual_url) == get_uri(expected_url)

    actual_querystring = urlparse(actual_url)
    expected_querystring = urlparse(expected_url)

    assert len(actual_querystring) == 6

    assert_querystring_value(
        actual_querystring, expected_querystring, "X-Amz-Algorithm"
    )
    assert_querystring_value_length(
        actual_querystring, expected_querystring, "X-Amz-Credential"
    )
    assert_querystring_value_length(
        actual_querystring, expected_querystring, "X-Amz-Date"
    )
    assert_querystring_value(actual_querystring, expected_querystring, "X-Amz-Expires")
    assert_querystring_value(
        actual_querystring, expected_querystring, "X-Amz-SignedHeaders"
    )
    assert_querystring_value(
        actual_querystring, expected_querystring, "X-Amz-Security-Token"
    )
    assert_querystring_value_length(
        actual_querystring, expected_querystring, "X-Amz-Signature"
    )


def assert_event_message(actual_result, expected_result):
    message_body = json.loads(actual_result.body)
    log.info(f"{expected_result=}")

    log.info(f"{message_body=}")

    assert validate_uuid4(message_body["Id"]) is True
    assert message_body["Name"] == expected_result["Name"]
    assert len(message_body["Date"]) == len(expected_result["Date"])
    assert message_body["Type"] == expected_result["Type"]
    assert message_body["Event"] == expected_result["Event"]
    assert message_body["Message"] == expected_result["Message"]
    assert message_body["Description"] == expected_result["Description"]

    assert_url(message_body["Value"], expected_result["Value"])
