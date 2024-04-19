# -*- coding: utf-8 -*-

import moto
from s3pathlib import context
import pynamodb_mate as pm

from ..vendor.mock_aws import BaseMockTest


class BaseTest(BaseMockTest):
    bucket = "test-bucket"

    mock_list = [
        moto.mock_sts,
        moto.mock_s3,
        moto.mock_dynamodb,
    ]

    @classmethod
    def setup_s3_and_dynamodb(cls):
        context.attach_boto_session(cls.bsm.boto_ses)
        cls.bsm.s3_client.create_bucket(Bucket=cls.bucket)
        pm.Connection()
