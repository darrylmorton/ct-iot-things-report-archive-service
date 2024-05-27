import os

import boto3
import pytest
from moto import mock_aws

from tests.config import AWS_REGION
from things_report_archive_service.service import ThingsReportArchiveService


@pytest.fixture
def aws_credentials():
    # Mocked AWS Credentials for moto
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_REGION"] = AWS_REGION


@pytest.fixture
def sqs_client(aws_credentials):
    with mock_aws():
        sqs_conn = boto3.client("sqs", region_name=AWS_REGION)
        yield sqs_conn


@pytest.fixture
def archive_service(sqs_client):
    archive_service = ThingsReportArchiveService()

    yield archive_service
