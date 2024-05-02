import json

import boto3
from botocore.exceptions import ClientError

from config import (
    AWS_REGION,
    get_logger,
    THINGS_REPORT_ARCHIVE_QUEUE,
    QUEUE_WAIT_SECONDS,
    THINGS_REPORT_JOB_BUCKET_NAME, THINGS_REPORT_ARCHIVE_DLQ,
)
from util.s3_util import s3_upload_zip, create_zip_report_job_path, s3_list_job_files

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

    async def _process_message(self, message_body: dict) -> None:
        log.debug("Processing archive job message...")

        report_name = message_body["ReportName"]
        user_id = message_body["UserId"]
        job_path = message_body["JobPath"]
        job_upload_path = message_body["JobUploadPath"]

        log.info(f"{report_name=}")
        log.info(f"{user_id=}")
        log.info(f"{job_path=}")
        log.info(f"{job_upload_path=}")

        # response = s3_list_job_files(self.s3_client)
        # log.info(f"{response=}")

        # bucket_files = self.s3_client.list_objects_v2(
        #     Bucket=THINGS_REPORT_JOB_BUCKET_NAME
        # )
        # log.info(f"{bucket_files=}")
        # request bucket contents for *.csv (debug)
        # create zip file
        # upload zip file to bucket

        # await self.upload_zip_job(
        #     user_id,
        #     report_name,
        #     job_index,
        #     start_timestamp,
        #     end_timestamp,
        #     response_body,
        # )

    async def poll(self) -> None:
        log.debug("Polling for archive job messages...")

        while True:
            await self.consume()

    async def consume(self) -> None:
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

                    await self._process_message(message_body)

        except ClientError as error:
            log.error(
                f"Couldn't receive report_archive_job_queue messages error {error}"
            )

            raise error

    async def download_job_files(self) -> None:
        pass

    async def upload_zip_job(
        self,
        user_id: str,
        report_name: str,
        job_index: int,
        start_timestamp: str,
        end_timestamp: str,
        # response_body: list[ThingPayload],
    ) -> None:
        log.debug("Uploading job csv file...")

        report_job_file_path, report_job_upload_path, report_job_filename = (
            create_zip_report_job_path(
                user_id,
                report_name,
                job_index,
                start_timestamp,
                end_timestamp,
            )
        )

        try:
            s3_upload_zip(
                self.s3_client,
                f"{report_job_file_path}/{report_job_filename}",
                f"{report_job_upload_path}/{report_job_filename}",
            )
        except ClientError as error:
            log.error(f"S3 client upload error: {error}")

            raise error
