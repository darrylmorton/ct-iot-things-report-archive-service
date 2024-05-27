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
        log.info("initializing ThingsReportArchiveService...")

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

        csv_files = s3_list_job_files(self.s3_client)
        log.info(f"{csv_files=}")

        # if len(csv_files) == 0:
        #     # TODO handle...
        #     pass

        path_prefix, archived = s3_download_job_files(self.s3_client, csv_files)

        log.info(f"*** _process_message {path_prefix=} {archived=}")

        # uploaded = False
        event = EVENT_ERROR

        if path_prefix and archived:
            upload_zip_file(self.s3_client, path_prefix, archived)
            event = EVENT_SUCCESS

        # TODO create message complete structure via util...
        # event_message = json.dumps({
        #     "Id": str(message_id),
        #     "Name": report_name,
        #     "Date": timestamp,
        #     "Type": EVENT_TYPE,
        #     "Event": event,
        #     "Description": "Report Archive Job Event",
        #     "Value": f"{presigned_url}",
        #     "Read": f"{uploaded}",
        # })

        event_message = create_event_message(
            s3_client=self.s3_client,
            name=report_name,
            event=event,
            job_upload_path=job_upload_path,
        )
        log.info(f"{event_message=}")

        await self.produce([event_message])

        # TODO decide what to return, unit testing this function for consideration...
        # return [event_message]

        # TODO handling conditions???

        # bucket_files = self.s3_client.list_objects_v2(
        #     Bucket=THINGS_REPORT_JOB_BUCKET_NAME
        # )
        # log.info(f"{bucket_files=}")
        # request bucket contents for *.csv (debug)
        # create zip file
        # upload zip file to bucket
        #
        # await self.upload_zip_job(
        #     user_id,
        #     report_name,
        #     job_index,
        #     start_timestamp,
        #     end_timestamp,
        #     response_body,
        # )

    async def poll(self) -> None:
        log.info("Polling for archive job messages...")

        while True:
            await self.consume()

    async def consume(self) -> None:
        log.info("Consuming archive job messages...")

        try:
            archive_job_messages = self.report_archive_job_queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=QUEUE_WAIT_SECONDS,
            )
            log.info(f"*** CONSUME {archive_job_messages=}")

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

    # async def download_job_files(self) -> None:
    #     pass

    # async def upload_zip_job(
    #     self,
    #     user_id: str,
    #     report_name: str,
    #     job_index: int,
    #     start_timestamp: str,
    #     end_timestamp: str,
    # ) -> None:
    #     log.debug("Uploading job csv file...")
    #
    #     report_job_file_path, report_job_upload_path, report_job_filename = (
    #         create_zip_report_job_path(
    #             user_id,
    #             report_name,
    #             job_index,
    #             start_timestamp,
    #             end_timestamp,
    #         )
    #     )
    #
    #     try:
    #         s3_upload_zip(
    #             self.s3_client,
    #             f"{report_job_file_path}/{report_job_filename}",
    #             f"{report_job_upload_path}/{report_job_filename}",
    #         )
    #     except ClientError as error:
    #         log.error(f"S3 client upload error: {error}")
    #
    #         raise error

    async def produce(self, event_messages: list[dict]) -> list[dict]:
        log.info(f"Sending event message...{event_messages=}")

        try:
            if len(event_messages) > 0:
                self.event_queue.send_messages(Entries=event_messages)

            return event_messages
        except ClientError as error:
            log.error(f"Couldn't receive event_queue messages error {error}")

            raise error
