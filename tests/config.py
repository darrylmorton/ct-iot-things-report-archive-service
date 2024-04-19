import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.test")


AWS_REGION = os.environ.get("AWS_REGION")

ENVIRONMENT = os.environ.get("ENVIRONMENT")
LOG_LEVEL = os.environ.get("LOG_LEVEL")
SERVICE_NAME = os.environ.get("SERVICE_NAME")

QUEUE_WAIT_SECONDS = os.environ.get("QUEUE_WAIT_SECONDS")
THINGS_REPORT_ARCHIVE_QUEUE = os.environ.get("THINGS_REPORT_ARCHIVE_QUEUE")
THINGS_REPORT_ARCHIVE_DLQ = os.environ.get("THINGS_REPORT_ARCHIVE_DLQ")
THINGS_REPORT_JOB_FILE_PATH_PREFIX = os.environ.get(
    "THINGS_REPORT_JOB_FILE_PATH_PREFIX"
)
THINGS_REPORT_JOB_BUCKET_NAME = os.environ.get("THINGS_REPORT_JOB_BUCKET_NAME")