import os
import shutil

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from config import (
    THINGS_REPORT_JOB_FILE_PATH_PREFIX,
    THINGS_REPORT_JOB_BUCKET_NAME,
    get_logger,
    THINGS_REPORT_ARCHIVE_EXPIRATION,
)
from util.util import isodate_to_timestamp

log = get_logger()


def s3_list_job_files(s3_client) -> list[dict]:
    response = s3_client.list_objects_v2(Bucket=THINGS_REPORT_JOB_BUCKET_NAME)

    return s3_filter_csv_file(response["Contents"])


def s3_filter_csv_file(s3_contents: list[dict]) -> list[dict]:
    csv_files = []

    for s3_content in s3_contents:
        content = s3_content["Key"]

        if content.endswith(".csv"):
            content_split = content.rsplit("/", 1)

            file_metadata = {
                "path_prefix": content_split[0],
                "filename": content_split[1],
            }
            csv_files.append(file_metadata)

    return csv_files


def create_zip_file(path_prefix: str) -> str:
    return shutil.make_archive(
        f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}",
        "zip",
        f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}",
    )


# 'wb'
def s3_download_job_files(s3_client, csv_files):
    path_prefix = ""
    archived = None

    for item in csv_files:
        path_prefix = item["path_prefix"]
        filename = item["filename"]

        if not os.path.exists(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}"):
            os.makedirs(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}")

        write_file = open(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}/{filename}", "wb")

        s3_client.download_fileobj(
            Bucket=THINGS_REPORT_JOB_BUCKET_NAME,
            Key=f"{path_prefix}/{filename}",
            Fileobj=write_file,
        )

        write_file.close()

    # TODO behaviour for error scenarios
    try:
        archived = create_zip_file(path_prefix)

    except NotADirectoryError as error:
        log.error(f"create zip file not a directory error {error}")
    except ValueError as error:
        log.error(f"create zip file unknown archive format error {error}")

    return path_prefix, archived


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


def upload_zip_file(s3_client, file_path, upload_path) -> bool:
    try:
        s3_client.upload_file(file_path, THINGS_REPORT_JOB_BUCKET_NAME, f"{upload_path}.zip")

        return True

    except ClientError as error:
        log.error(f"S3 client upload error: {error}")

        raise error
    finally:
        shutil.rmtree(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{upload_path}")
        os.remove(file_path)

        return False


def create_presigned_url(s3_client: BaseClient, bucket_name: str, object_name: str) -> str:
    """Generate a presigned URL to share an S3 object

    :param s3_client: BaseClient for S3 service
    :param bucket_name
    :param object_name
    :return: Presigned URL as string. If error, returns None.
    """

    presigned_url = ""

    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=THINGS_REPORT_ARCHIVE_EXPIRATION,
        )

    except ClientError as error:
        log.error(f"create_presigned_url error {error}")
    finally:
        return presigned_url
