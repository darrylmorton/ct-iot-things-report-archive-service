import os
import logging

import dotenv

dotenv.load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION")

ENVIRONMENT = os.environ.get("ENVIRONMENT")
LOG_LEVEL = os.environ.get("LOG_LEVEL")
SERVICE_NAME = os.environ.get("SERVICE_NAME")

QUEUE_WAIT_SECONDS = int(os.environ.get("QUEUE_WAIT_SECONDS"))
THINGS_REPORT_ARCHIVE_QUEUE = os.environ.get("THINGS_REPORT_ARCHIVE_QUEUE")
THINGS_REPORT_ARCHIVE_DLQ = os.environ.get("THINGS_REPORT_ARCHIVE_DLQ")
THINGS_REPORT_JOB_FILE_PATH_PREFIX = os.environ.get("THINGS_REPORT_JOB_FILE_PATH_PREFIX")
THINGS_EVENT_QUEUE = os.environ.get("THINGS_EVENT_QUEUE")
THINGS_REPORT_JOB_BUCKET_NAME = os.environ.get("THINGS_REPORT_JOB_BUCKET_NAME")
THINGS_REPORT_ARCHIVE_EXPIRATION = os.environ.get("THINGS_REPORT_ARCHIVE_EXPIRATION")

EVENT_CATEGORY = "Report"
EVENT_TYPE_SUCCESS = "OK"
EVENT_TYPE_ERROR = "ERROR"


def get_logger() -> logging.Logger:
    logger = logging.getLogger("uvicorn")
    logger.setLevel(logging.getLevelName(LOG_LEVEL))

    return logger
