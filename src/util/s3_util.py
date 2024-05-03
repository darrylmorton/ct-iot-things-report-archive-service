import os
import shutil
from functools import reduce

import boto3
from botocore.exceptions import ClientError

from config import THINGS_REPORT_JOB_FILE_PATH_PREFIX, THINGS_REPORT_JOB_BUCKET_NAME, get_logger
from util.service_util import isodate_to_timestamp

log = get_logger()

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


def s3_filter_csv_file(s3_contents: list[dict]) -> list[dict]:
    csv_files = []
    log.info(f"s3_filter_csv_file {s3_contents=}")

    for s3_content in s3_contents:
        content = s3_content["Key"]

        if content.endswith(".csv"):
            # create dir path
            # create filename
            content_split = content.rsplit("/", 1)

            file_metadata = {
                "path_prefix": content_split[0],
                "filename": content_split[1]
            }
            csv_files.append(file_metadata)

    return csv_files


def s3_list_job_files(s3_client) -> list[dict]:
    response = s3_client.list_objects_v2(Bucket=THINGS_REPORT_JOB_BUCKET_NAME)
    log.info(f"{response=}")

    result = s3_filter_csv_file(response["Contents"])
    log.info(f"{result=}")

    return result


# 'wb'
def s3_download_job_files(s3_client): # -> list[str]:
    csv_files = s3_list_job_files(s3_client)
    path_prefix = ""
    archived = None

    for item in csv_files:
        log.info(f"*** {item['path_prefix']}/{item['filename']}")

        path_prefix = item['path_prefix']
        filename = item["filename"]

        if not os.path.exists(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}"):
            os.makedirs(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}")

        # path_prefix = f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{item['path_prefix']}"

        # s3_client.download_file(
        #     THINGS_REPORT_JOB_BUCKET_NAME,
        #     f"{path_prefix}/{filename}",
        #     f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}/{filename}"
        # )

        # obj = s3_client.get_object(THINGS_REPORT_JOB_BUCKET_NAME, f"{path_prefix}/{filename}")
        #
        # with open(f"{path_prefix}/{filename}", 'wb') as f:

        write_file = open(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}/{filename}", "wb")

        s3_client.download_fileobj(Bucket=THINGS_REPORT_JOB_BUCKET_NAME, Key=f"{path_prefix}/{filename}", Fileobj=write_file)

        write_file.close()

        archived = shutil.make_archive(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}", 'zip', f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}")

        log.info(f"{archived}")

    try:
        s3_upload_zip(
            s3_client,
            archived, #f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}.zip",
            f"{path_prefix}.zip",
        )
    except ClientError as error:
        log.error(f"S3 client upload error: {error}")

        raise error
    finally:
        shutil.rmtree(f"{THINGS_REPORT_JOB_FILE_PATH_PREFIX}/{path_prefix}")
        os.remove(archived)
