import json
import uuid

from datetime import datetime, timezone
from botocore import client

import config
from util import s3_util

log = config.get_logger()

EVENT_TYPE = "report_job_archive"
EVENT_SUCCESS = "archive ready"
EVENT_ERROR = "archive error"


def create_event_message(
    s3_client: client.BaseClient, name: str, event: str, message: str, job_upload_path: str
) -> dict:
    message_id = str(uuid.uuid4())
    timestamp = datetime.now(tz=timezone.utc).isoformat()

    presigned_url = s3_util.create_presigned_url(
        bucket_name=config.THINGS_REPORT_JOB_BUCKET_NAME,
        object_name=f"{job_upload_path}.zip",
        s3_client=s3_client,
    )

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
            "Message": {
                "DataType": "String",
                "StringValue": message,
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
            "Message": message,
            "Value": presigned_url,
            "Read": read,
        }),
        MessageDeduplicationId=message_id,
    )
