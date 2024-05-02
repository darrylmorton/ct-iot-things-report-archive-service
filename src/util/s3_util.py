from config import THINGS_REPORT_JOB_FILE_PATH_PREFIX, THINGS_REPORT_JOB_BUCKET_NAME
from util.service_util import isodate_to_timestamp


def create_zip_report_job_path(
    user_id: str, report_name: str, job_index, start_timestamp: str, end_timestamp: str
) -> tuple[str, str, str]:
    start_timestamp = isodate_to_timestamp(start_timestamp)
    end_timestamp = isodate_to_timestamp(end_timestamp)
    # fmt: off
    report_job_file_path = (
        f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{user_id}/{report_name}-{start_timestamp}-{end_timestamp}"
    )
    report_job_upload_path = (
        f"{user_id}/{report_name}-{start_timestamp}-{end_timestamp}"
    )
    report_job_filename = f"{report_name}-{job_index}.zip"

    return report_job_file_path, report_job_upload_path, report_job_filename


def s3_upload_zip(s3_client, file_path, upload_path) -> None:
    with open(file_path, "rb") as f:
        s3_client.upload_file(file_path, THINGS_REPORT_JOB_BUCKET_NAME, upload_path)

        f.close()
