import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, parse_qs, ParseResult

from config import get_logger, THINGS_REPORT_JOB_BUCKET_NAME
from tests.config import THINGS_REPORT_JOB_FILE_PATH_PREFIX, QUEUE_WAIT_SECONDS
from util.s3_util import isodate_to_timestamp, create_presigned_url
from tests.helper.helper import validate_uuid4
from things_report_archive_service.service import ThingsReportArchiveService

log = get_logger()

# [sqs.Message(queue_url='https://sqs.eu-west-2.amazonaws.com/123456789012/event-queue.fifo', receipt_handle='mmcsqmaybbqkdnlvrvqkdfnnufqfyatkhesudwwfjzkzhpuvelkpolrfwhtiqfvetctbhfabvkbpukildzzylcanzhwahdylybmmpvhkpbgwtfihtjmziyndgydhokjefqkxutnpasexhpuqcaffybiejrorwdeupcqlyurvlcjeojbhjkoyuthtw')]
# def create_event_message():
#     return [sqs.Message(
#         queue_url='https://sqs.eu-west-2.amazonaws.com/123456789012/event-queue.fifo',
#         receipt_handle='mmcsqmaybbqkdnlvrvqkdfnnufqfyatkhesudwwfjzkzhpuvelkpolrfwhtiqfvetctbhfabvkbpukildzzylcanzhwahdylybmmpvhkpbgwtfihtjmziyndgydhokjefqkxutnpasexhpuqcaffybiejrorwdeupcqlyurvlcjeojbhjkoyuthtw'
# )]


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


# def assert_event_message(actual_result: dict, expected_result: dict) -> None:
#     assert validate_uuid4(actual_result["Id"])
#     assert validate_uuid4(expected_result["Id"])
#     assert actual_result["Id"] != expected_result["Id"]
#
#     assert actual_result["UserId"] == expected_result["UserId"]
#     assert actual_result["ReportName"] == expected_result["ReportName"]
#     assert actual_result["JobPath"] == expected_result["JobPath"]
#     assert actual_result["JobUploadPath"] == expected_result["JobUploadPath"]


# def create_event_message(
#     report_name: str,
#     report_type: str,
#     report_event: str,
# ):
#     return {
#         "Id": "",
#         "Name": report_name,
#         "Type": report_type,
#         "Event": report_event,
#         "Description": "",
#         "Value": "",
#         "Read": "False",
#     }

# 'https://my-bucket.s3.amazonaws.com/a9f33d36-ad63-4129-88bf-a8818996d224/report_name_0-1592654400-1592913600.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=FOOBARKEY%2F20240527%2Feu-west-2%2Fs3%2Faws4_request&X-Amz-Date=20240527T212421Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Security-Token=testing&X-Amz-Signature=d75926a8fad9ae654a9f942e5bb15ac157a247128e75a0454e5a887fb7b155f8'


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

    # assert message_body["Value"] == expected_result.Value
    # ("https://my-bucket.s3.amazonaws.com/a9f33d36-ad63-4129-88bf-a8818996d224/report_name_0-1592654400-1592913600.zip?"
    #  "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
    #  "X-Amz-Credential=FOOBARKEY%2F20240527%2Feu-west-2%2Fs3%2Faws4_request&"
    #  "X-Amz-Date=20240527T215727Z&"
    #  "X-Amz-Expires=3600&"
    #  "X-Amz-SignedHeaders=host&"
    #  "X-Amz-Security-Token=testing&"
    #  "X-Amz-Signature=d36b2928ae6dd6c40d45b5d630b8e03e635ea9e035f4061b61c4a435009bb084"
    #  "")
    #     assert (
    #         "https://my-bucket.s3.amazonaws.com/a9f33d36-ad63-4129-88bf-a8818996d224/report_name_0-1592654400"
    #         "-1592913600.zip"
    #     ) in message_body["Value"]

    # actual_parsed_url = urlparse(message_body["Value"])
    # expected_parsed_url = urlparse(message_body["Value"])

    # assert len(actual_parsed_url) == 6
    #
    # assert get_uri(message_body["Value"]) == get_uri(expected_result["Value"])

    # assert_querystring_value(actual_parsed_url, expected_parsed_url, "X-Amz-Algorithm", 0)
    # assert_querystring_value(actual_parsed_url, expected_parsed_url, "X-Amz-Algorithm", 0)
    # assert parsed_url.scheme == "http"

    # x_amz_algorithm = parse_qs(parsed_url.query)["X-Amz-Algorithm"][0]
    # log.info(f"{x_amz_algorithm=}")
    # x_amz_credential = parse_qs(parsed_url.query)["X-Amz-Credential"][1]
    # x_amz_date = parse_qs(parsed_url.query)["X-Amz-Date"][2]
    # x_amz_expires = parse_qs(parsed_url.query)["X-Amz-Expires"][3]
    # x_amz_signedheaders = parse_qs(parsed_url.query)["X-Amz-SignedHeaders"][4]
    # x_amz_security_token = parse_qs(parsed_url.query)["X-Amz-Security-Token"][5]
    # x_amz_signature = parse_qs(parsed_url.query)["X-Amz-Signature"][6]

    # assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in message_body["Value"]
    # # assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in message_body["Value"]
    # assert "X-Amz-Credential=FOOBARKEY%2F20240527%2Feu-west-2%2Fs3%2Faws4_request" in message_body["Value"]
    # assert "X-Amz-Date=20240527T212421Z" in message_body["Value"]
    # assert "X-Amz-Expires=3600" in message_body["Value"]

    # assert message_body["Read"] == expected_result["Read"]

    # actual_result.receipt_handle = expected_result.receipt_handle
