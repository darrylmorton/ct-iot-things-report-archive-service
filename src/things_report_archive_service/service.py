import json
import boto3
from botocore.exceptions import ClientError

from config import (
    AWS_REGION,
    get_logger,
    THINGS_REPORT_ARCHIVE_QUEUE,
    QUEUE_WAIT_SECONDS,
    THINGS_REPORT_ARCHIVE_DLQ,
    THINGS_EVENT_QUEUE,
)
from util.s3_util import (
    s3_list_job_files,
    s3_download_job_files,
    upload_zip_file,
)
from util.service_util import (
    EVENT_SUCCESS,
    EVENT_ERROR,
    create_event_message,
)

log = get_logger()


class ThingsReportArchiveService:
    def __init__(self):
        log.debug("initializing ThingsReportArchiveService...")

        self.sqs = boto3.resource("sqs", region_name=AWS_REGION)
        self.s3_client = boto3.client("s3", region_name=AWS_REGION)
        self.report_archive_job_queue = self.sqs.Queue(
            f"{THINGS_REPORT_ARCHIVE_QUEUE}.fifo"
        )
        self.report_archive_job_dlq = self.sqs.Queue(
            f"{THINGS_REPORT_ARCHIVE_DLQ}.fifo"
        )
        self.event_queue = self.sqs.Queue(f"{THINGS_EVENT_QUEUE}.fifo")

    #
    def _process_message(self, message_body: dict) -> bool:
        log.debug("Processing archive job message...")

        report_name = message_body["ReportName"]
        job_upload_path = message_body["JobUploadPath"]

        csv_files = s3_list_job_files(self.s3_client)

        if not csv_files:
            event_message = create_event_message(
                s3_client=self.s3_client,
                name=report_name,
                event=EVENT_ERROR,
                message="There are no csv jobs to generate an archive job file",
                job_upload_path=job_upload_path,
            )

            self.produce([event_message])

            return False

        path_prefix, archived = s3_download_job_files(self.s3_client, csv_files)

        if path_prefix and archived:
            upload_zip_file(self.s3_client, path_prefix, archived)

        event_message = create_event_message(
            s3_client=self.s3_client,
            name=report_name,
            event=EVENT_SUCCESS,
            message="Successfully uploaded archive job file",
            job_upload_path=job_upload_path,
        )

        self.produce([event_message])

        return True

    def poll(self) -> None:
        log.debug("Polling for archive job messages...")

        while True:
            self.consume()

    def consume(self) -> None:
        log.debug("Consuming archive job messages...")

        try:
            archive_job_messages = self.report_archive_job_queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=QUEUE_WAIT_SECONDS,
            )

            if len(archive_job_messages) > 0:
                for archive_job_message in archive_job_messages:
                    message_body = json.loads(archive_job_message.body)

                    archive_job_message.delete()

                    self._process_message(message_body)

        except ClientError as error:
            log.error(
                f"Couldn't receive report_archive_job_queue messages error {error}"
            )

            raise error

    def produce(self, event_messages: list[dict]) -> list[dict]:
        log.debug(f"Sending event message...")

        try:
            if len(event_messages) > 0:
                self.event_queue.send_messages(Entries=event_messages)

            return event_messages
        except ClientError as error:
            log.error(f"Couldn't receive event_queue messages error {error}")

            raise error
