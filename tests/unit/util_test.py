import pytest

from util import util


class TestUtil:
    start_timestamp = "2024-04-12T00:00:00Z"
    start_epoch_timestamp = util.isodate_to_timestamp(start_timestamp)

    def test_create_timestamp(self):
        expected_epoch_timestamp = self.start_epoch_timestamp
        actual_epoch_timestamp_result = util.isodate_to_timestamp(self.start_timestamp)

        assert actual_epoch_timestamp_result == expected_epoch_timestamp

    @pytest.mark.skip
    def test_s3_list_job_files(self):
        pass

    @pytest.mark.skip
    def test_s3_filter_csv_file(self):
        pass

    @pytest.mark.skip
    def test_create_zip_file(self):
        pass

    @pytest.mark.skip
    def test_s3_download_job_files(self):
        pass

    @pytest.mark.skip
    def test_create_zip_report_job_path(self):
        pass

    @pytest.mark.skip
    def test_upload_zip_file(self):
        pass

    @pytest.mark.skip
    def test_create_presigned_url(self):
        pass

    @pytest.mark.skip
    def test_create_event_message(self):
        pass
