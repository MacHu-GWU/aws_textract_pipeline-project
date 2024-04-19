# -*- coding: utf-8 -*-

import moto
import pynamodb_mate as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from aws_textract_pipeline.doc_type import DocTypeEnum
from aws_textract_pipeline.tracker import (
    StatusEnum,
    BaseStatusAndUpdateTimeIndex,
    BaseTracker,
    Data,
    Component,
)
from aws_textract_pipeline.tests.mock_test import BaseTest


class StatusAndUpdateTimeIndex(BaseStatusAndUpdateTimeIndex):
    pass


class Tracker(BaseTracker):
    class Meta:
        table_name = "test-table"
        region = "us-east-1"
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


class TestTracker(BaseTest):
    @classmethod
    def setup_class_post_hook(cls):
        cls.setup_s3_and_dynamodb()
        Tracker.create_table(wait=True)

    def test(self):
        s3path_landing = S3Path(self.bucket, "root", "landing", "report.pdf")
        tracker = Tracker.new(
            task_id="doc-1",
            data=Data(
                landing_uri=s3path_landing.uri,
                doc_type=DocTypeEnum.pdf.value,
                components=[
                    Component(id="000001"),
                    Component(id="000002"),
                    Component(id="000003"),
                ],
            ).to_dict(),
        )

        tracker = Tracker.get_one_or_none(task_id="doc-1")

        # Test property method
        assert tracker.doc_id == "doc-1"
        assert tracker.data_obj.landing_uri == s3path_landing.uri

        # Test status transition
        assert tracker.status == StatusEnum.s01000_landing_to_raw_pending.value

        with tracker.start_landing_to_textract(debug=False):
            pass
        assert tracker.status == StatusEnum.s01060_landing_to_raw_succeeded.value

        with tracker.start_raw_to_component(debug=False):
            pass
        assert tracker.status == StatusEnum.s02060_raw_to_component_succeeded.value

        with tracker.start_component_to_textract_output(debug=False):
            pass
        assert (
            tracker.status
            == StatusEnum.s03060_component_to_textract_output_succeeded.value
        )

        with tracker.start_textract_output_to_text_and_json(debug=False):
            pass
        assert (
            tracker.status
            == StatusEnum.s05060_textract_output_to_text_and_json_succeeded.value
        )

        with tracker.start_json_to_extracted_data(debug=False):
            pass
        assert (
            tracker.status == StatusEnum.s07060_json_to_extracted_data_succeeded.value
        )

        with tracker.start_extracted_data_to_hil_output(debug=False):
            pass
        assert (
            tracker.status
            == StatusEnum.s08060_extracted_data_to_hil_output_succeeded.value
        )

        with tracker.start_hil_output_to_hil_post_process(debug=False):
            pass
        assert (
            tracker.status
            == StatusEnum.s09060_hil_output_to_hil_post_process_succeeded.value
        )


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.tracker", preview=False)
