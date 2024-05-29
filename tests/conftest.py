import os
import boto3
import pytest
import moto

import tests.config as test_config
from things_report_archive_service import service


@pytest.fixture
def aws_credentials():
    # Mocked AWS Credentials for moto
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_REGION"] = test_config.AWS_REGION


@pytest.fixture
def sqs_client(aws_credentials):
    with moto.mock_aws():
        sqs_conn = boto3.client("sqs", region_name=test_config.AWS_REGION)

        yield sqs_conn


@pytest.fixture
def archive_service(sqs_client):
    archive_service = service.ThingsReportArchiveService()

    yield archive_service
