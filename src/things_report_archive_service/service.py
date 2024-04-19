import boto3

from config import AWS_REGION


class ThingsReportArchiveService:
    def __init__(self):
        self.sqs = boto3.resource("sqs", region_name=AWS_REGION)

    async def _process_message(self, message: dict) -> None:
        pass

    def poll(self) -> None:
        pass

    def consume(self) -> None:
        pass

    async def download_job_files(self) -> None:
        pass

    async def upload_archive(self) -> None:
        pass
