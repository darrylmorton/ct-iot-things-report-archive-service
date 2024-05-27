import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from botocore.client import BaseClient

from config import THINGS_REPORT_JOB_BUCKET_NAME
from util.s3_util import create_presigned_url

log = logging.getLogger("service_util")

EVENT_TYPE = "report_job_archive"
EVENT_SUCCESS = "archive ready"
EVENT_ERROR = "archive error"


def create_event_message(
    s3_client: Any, name: str, event: str, job_upload_path: str
) -> dict:
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

    return dict(
        Id=message_id,
        MessageAttributes={
            "Id": {
                "DataType": "String",
                "StringValue": message_id,
            },
            "Name": {
                "DataType": "String",
                "StringValue": name,
            },
            "Date": {
                "DataType": "String",
                "StringValue": timestamp,
            },
            "Type": {
                "DataType": "String",
                "StringValue": event_type,
            },
            "Event": {
                "DataType": "String",
                "StringValue": event,
            },
            "Description": {
                "DataType": "String",
                "StringValue": description,
            },
            "Value": {
                "DataType": "String",
                "StringValue": presigned_url,
            },
            "Read": {
                "DataType": "String",
                "StringValue": read,
            },
        },
        MessageBody=json.dumps({
            "Id": message_id,
            "Name": name,
            "Date": timestamp,
            "Type": event_type,
            "Event": event,
            "Description": description,
            "Value": presigned_url,
            "Read": read,
        }),
        MessageDeduplicationId=message_id,
    )
