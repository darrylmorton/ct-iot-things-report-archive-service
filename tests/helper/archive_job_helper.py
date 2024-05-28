import json
import time
import uuid

from config import get_logger
from tests.config import THINGS_REPORT_JOB_FILE_PATH_PREFIX
from util.s3_util import isodate_to_timestamp
from tests.helper.helper import validate_uuid4
from things_report_archive_service.service import ThingsReportArchiveService
# from util.service_util import create_archive_job_message

log = get_logger()


def create_archive_job_message(
    message_id: str,
    user_id: str,
    report_name: str,
    job_path: str,
    job_upload_path: str,
) -> dict:
    return dict(
        Id=message_id,
        MessageAttributes={
            "Id": {
                "DataType": "String",
                "StringValue": message_id,
            },
            "UserId": {
                "DataType": "String",
                "StringValue": user_id,
            },
            "ReportName": {
                "DataType": "String",
                "StringValue": report_name,
            },
            "JobPath": {
                "DataType": "String",
                "StringValue": job_path,
            },
            "JobUploadPath": {
                "DataType": "String",
                "StringValue": job_upload_path,
            },
        },
        MessageBody=json.dumps({
            "Id": message_id,
            "UserId": user_id,
            "ReportName": report_name,
            "JobPath": job_path,
            "JobUploadPath": job_upload_path,
        }),
        MessageDeduplicationId=message_id,
    )


def expected_archive_job_message(message: dict) -> list[dict]:
    message_body = json.loads(message["MessageBody"])

    start_timestamp = isodate_to_timestamp(message_body["StartTimestamp"])
    end_timestamp = isodate_to_timestamp(message_body["EndTimestamp"])

    message_id = uuid.uuid4()
    job_path_prefix = f"{message_body["UserId"]}/{message_body["ReportName"]}"
    job_path_suffix = f"{int(start_timestamp)}-{int(end_timestamp)}"
    job_path = f"{job_path_prefix}-{job_path_suffix}"

    job_upload_path = job_path
    job_path = f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{job_path}"

    return [
        create_archive_job_message(
            message_id=str(message_id),
            user_id=message_body["UserId"],
            report_name=message_body["ReportName"],
            job_path=job_path,
            job_upload_path=job_upload_path,
        )
    ]


def service_poll(job_service: ThingsReportArchiveService, timeout_seconds=0) -> None:
    log.debug("Polling...")

    timeout = time.time() + timeout_seconds

    while True:
        if time.time() > timeout:
            log.info(f"Task timed out after {timeout_seconds}")
            break
        else:
            job_service.consume()


# def report_archive_job_consumer(
#     report_archive_job_queue: Any, timeout_seconds=0
# ) -> list[dict]:
#     timeout = time.time() + timeout_seconds
#     messages = []
#
#     while True:
#         if time.time() > timeout:
#             log.info(f"Task timed out after {timeout_seconds}")
#             break
#
#         archive_job_messages = report_archive_job_queue.receive_messages(
#             MessageAttributeNames=["All"],
#             MaxNumberOfMessages=10,
#             WaitTimeSeconds=QUEUE_WAIT_SECONDS,
#         )
#
#         for archive_job_message in archive_job_messages:
#             log.info(f"TEST CONSUMER archive_job_message: {archive_job_message=}")
#
#             messages.append(archive_job_message)
#
#             archive_job_message.delete()
#
#     log.info(f"TEST CONSUMER message: {messages=}")
#
#     return messages


def assert_archive_job_message(actual_result: dict, expected_result: dict) -> None:
    assert validate_uuid4(actual_result["Id"])
    assert validate_uuid4(expected_result["Id"])
    assert actual_result["Id"] != expected_result["Id"]

    assert actual_result["UserId"] == expected_result["UserId"]
    assert actual_result["ReportName"] == expected_result["ReportName"]
    assert actual_result["JobPath"] == expected_result["JobPath"]
    assert actual_result["JobUploadPath"] == expected_result["JobUploadPath"]


def assert_archive_job_messages(
    actual_result: list[dict], expected_result: list[dict]
) -> None:
    assert len(actual_result) == len(expected_result)

    index = 0

    for archive_message in actual_result:
        archive_message_body = json.loads(archive_message.body)

        expected_message = expected_result[index]
        expected_result_body = json.loads(expected_message["MessageBody"])

        assert_archive_job_message(archive_message_body, expected_result_body)

        index = index + 1
