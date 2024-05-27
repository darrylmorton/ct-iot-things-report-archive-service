import json
import uuid
from typing import Any

import boto3

from config import AWS_REGION, QUEUE_WAIT_SECONDS


def create_sqs_queue(queue_name: str, dlq_name="") -> tuple[Any, Any]:
    sqs = boto3.resource("sqs", region_name=AWS_REGION)
    queue_attributes = {
        "WaitSeconds": f"{QUEUE_WAIT_SECONDS}",
    }
    dlq = None

    if dlq_name:
        dlq = sqs.create_queue(
            QueueName=f"{dlq_name}.fifo", Attributes=queue_attributes
        )

        dlq_policy = json.dumps({
            "deadLetterTargetArn": dlq.attributes["QueueArn"],
            "maxReceiveCount": "10",
        })

        queue_attributes["RedrivePolicy"] = dlq_policy

    queue = sqs.create_queue(
        QueueName=f"{queue_name}.fifo", Attributes=queue_attributes
    )

    return queue, dlq


# def create_sns_topic(topic_name: str):
#     sns = boto3.resource("sns", region_name=AWS_REGION)
#
#     return sns.create_topic(Name=topic_name)


def validate_uuid4(uuid_string):
    """
    Validate that a UUID string is in
    fact a valid uuid4.
    Happily, the uuid module does the actual
    checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid.
    """

    try:
        val = uuid.UUID(uuid_string, version=4)

    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False

    # If the uuid_string is a valid hex code,
    # but an invalid uuid4,
    # the UUID.__init__ will convert it to a
    # valid uuid4. This is bad for validation purposes.

    return str(val) == uuid_string
