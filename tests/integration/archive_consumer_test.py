import uuid
from unittest.mock import patch

import boto3
import pytest

from config import (
    THINGS_REPORT_JOB_FILE_PATH_PREFIX,
    THINGS_REPORT_ARCHIVE_QUEUE,
    THINGS_REPORT_ARCHIVE_DLQ,
    get_logger, AWS_REGION,
)
from tests.helper.archive_job_helper import (
    create_archive_job_message,
    service_poll,
    report_archive_job_consumer,
)
from tests.helper.helper import create_sqs_queue
from util.s3_util import s3_list_job_files, s3_download_job_files, s3_filter_csv_file, upload_zip_file, \
    upload_zip_file
from util.service_util import isodate_to_timestamp

log = get_logger()


class TestArchiveConsumer:
    message_id = str(uuid.uuid4())
    user_id = "28ae898f-8a46-4bc1-a64e-c95709308315"
    report_name = "report_name_0"
    job_index = 0
    start_timestamp = "2020-06-20T12:00:00Z"
    start_epoch_timestamp = isodate_to_timestamp(start_timestamp)
    end_timestamp = "2020-06-23T12:00:00Z"
    end_epoch_timestamp = isodate_to_timestamp(end_timestamp)
    # fmt: off
    job_file_path_prefix = (
        f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{user_id}/{report_name}-{start_epoch_timestamp}-{end_epoch_timestamp}"
    )
    job_upload_path = (
        f"{user_id}/{report_name}-{start_epoch_timestamp}-{end_epoch_timestamp}"
    )
    job_path_suffix = f"{report_name}-{0}.zip"
    job_path = f"{job_file_path_prefix}-{job_path_suffix}"

    # mock_s3_upload_zip

    # uploading disabled
    # @patch("things_report_archive_service.service.s3_upload_zip")
    @pytest.mark.skip
    async def test_archive_consumer(
            self, archive_service
    ):
        # mock_s3_upload_zip.return_value = None
        log.info(f"{THINGS_REPORT_ARCHIVE_QUEUE=}")
        log.info(f"{THINGS_REPORT_ARCHIVE_DLQ=}")

        report_archive_queue, _ = create_sqs_queue(
            THINGS_REPORT_ARCHIVE_QUEUE, THINGS_REPORT_ARCHIVE_DLQ
        )

        expected_archive_message = create_archive_job_message(
            self.message_id,
            self.user_id,
            self.report_name,
            self.job_path,
            self.job_upload_path
        )

        report_archive_queue.send_messages(Entries=[expected_archive_message])
        await service_poll(archive_service, 10)

        actual_archive_messages = await report_archive_job_consumer(
            report_archive_queue, 10
        )

        log.info(f"{actual_archive_messages=}")
        # actual_archive_job_messages

    # @pytest.mark.skip(reason="requires real aws credentials")
    # def test_s3_list_job_files(self):
    #     s3_client = boto3.client("s3", region_name=AWS_REGION)
    #
    #     # response = s3_list_job_files(s3_client)
    #     # log.info(f"{response=}")
    #
    #     s3_download_job_files(s3_client)

    # @pytest.mark.skip(reason="requires real aws credentials")
    class TestArchiveConsumerWithRealAwsCredentials:
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        path_prefix = '28ae898f-8a46-4bc1-a64e-c95709308315/report_name_0-1592654400-1592913600'
        filtered_csvs = [
            {
                'path_prefix': path_prefix,
                'filename': 'report_name_0-0.csv'
            },
            {
                'path_prefix': path_prefix,
                'filename': 'report_name_0-1.csv'
            }
        ]
        archived_path_suffix = 'dist/28ae898f-8a46-4bc1-a64e-c95709308315/report_name_0-1592654400-1592913600.zip'

        def test_s3_list_job_files(self):
            expected_result = self.filtered_csvs

            actual_result = s3_list_job_files(self.s3_client)

            assert actual_result == expected_result

        # def test_s3_download_job_csv_files(self):
        #     path_prefix, archived = s3_download_job_files(self.s3_client, self.filtered_csvs)
        #
        #     assert path_prefix == self.path_prefix
        #     assert archived.endswith(self.archived_path_suffix) is True

        def test_s3_upload_zip(self):
            path_prefix, archived = s3_download_job_files(self.s3_client, self.filtered_csvs)
            log.info(f"TEST {path_prefix=}")
            log.info(f"TEST {archived=}")

            uploaded = upload_zip_file(self.s3_client, archived, path_prefix)
            log.info(f"{uploaded=}")

