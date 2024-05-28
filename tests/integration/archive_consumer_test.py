import uuid
from unittest.mock import patch

import boto3
import pytest

from config import (
    THINGS_REPORT_JOB_FILE_PATH_PREFIX,
    THINGS_REPORT_ARCHIVE_QUEUE,
    THINGS_REPORT_ARCHIVE_DLQ,
    get_logger,
    AWS_REGION,
    THINGS_EVENT_QUEUE,
)

from tests.helper import helper, archive_job_helper, event_helper
from util.s3_util import (
    s3_list_job_files,
    s3_download_job_files,
    upload_zip_file,
)
from util.service_util import EVENT_SUCCESS
from util.util import isodate_to_timestamp

log = get_logger()


class TestArchiveConsumer:
    message_id = str(uuid.uuid4())
    user_id = "a9f33d36-ad63-4129-88bf-a8818996d224"
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

    path_prefix = f"{user_id}/report_name_0-1592654400-1592913600"
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
    archived_path_suffix = f"dist/{user_id}.zip"
    # fmt: off
    s3_contents = {
        'Contents': [
            {'Key': f"{path_prefix}.zip"},
            {'Key': f"{path_prefix}/report_name_0-0.cs"},
            {'Key': f"{path_prefix}/report_name_0-0.csv"},
            {'Key': f"{path_prefix}/report_name_0-1.csv"},
            {'Key': f"{path_prefix}/report_name_0-1.sv"}
        ]
    }

    # uploading disabled
    @patch("things_report_archive_service.service.s3_list_job_files")
    @patch("things_report_archive_service.service.s3_download_job_files")
    @patch("things_report_archive_service.service.upload_zip_file")
    # @pytest.mark.skip(reason="requires real aws credentials")
    def test_archive_consumer(
            self,
            mock_s3_list_job_files,
            mock_s3_download_job_files,
            mock_upload_zip_file,
            archive_service,
    ):
        mock_s3_list_job_files.return_value = self.s3_contents
        mock_s3_download_job_files.return_value = (
            self.path_prefix,
            self.archived_path_suffix
        )
        mock_upload_zip_file.return_value = True

        report_archive_queue, _ = helper.create_sqs_queue(
            THINGS_REPORT_ARCHIVE_QUEUE,
            THINGS_REPORT_ARCHIVE_DLQ
        )
        event_queue, _ = helper.create_sqs_queue(THINGS_EVENT_QUEUE)

        expected_archive_message = archive_job_helper.create_archive_job_message(
            self.message_id,
            self.user_id,
            self.report_name,
            self.job_path,
            self.job_upload_path
        )

        report_archive_queue.send_messages(Entries=[expected_archive_message])
        archive_job_helper.service_poll(archive_service, 10)

        expected_result = event_helper.create_event_message(
            s3_client=archive_service.s3_client,
            name=self.report_name,
            event=EVENT_SUCCESS,
            message="Successfully uploaded archive job file",
            job_upload_path=self.job_upload_path
        )

        actual_event_messages = event_helper.event_consumer(
            event_queue, 10
        )

        event_helper.assert_event_message(actual_event_messages[0], expected_result)

    @pytest.mark.skip(reason="requires real aws credentials")
    class TestArchiveConsumerWithRealAwsCredentials:
        user_id = "28ae898f-8a46-4bc1-a64e-c95709308315"
        s3_client = boto3.client("s3", region_name=AWS_REGION)

        path_prefix = f"{user_id}/report_name_0-1592654400-1592913600"
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
        archived_path_suffix = f"dist/{path_prefix}.zip"

        def test_s3_list_job_files(self):
            expected_result = self.filtered_csvs

            actual_result = s3_list_job_files(self.s3_client)

            assert actual_result == expected_result

        def test_s3_download_job_csv_files(self):
            path_prefix, archived = s3_download_job_files(
                self.s3_client,
                self.filtered_csvs
            )

            assert path_prefix == self.path_prefix
            assert archived.endswith(
                self.archived_path_suffix
            ) is True

        def test_s3_upload_zip(self):
            path_prefix, archived = s3_download_job_files(
                self.s3_client,
                self.filtered_csvs
            )

            actual_result = upload_zip_file(self.s3_client, archived, path_prefix)

            assert actual_result is True
