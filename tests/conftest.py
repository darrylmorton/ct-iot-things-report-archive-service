import os

import boto3
import pytest
from moto import mock_aws

from tests.config import AWS_REGION


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
        conn = boto3.client("sqs", region_name=AWS_REGION)
        yield conn
