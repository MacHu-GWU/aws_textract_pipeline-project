# -*- coding: utf-8 -*-

import pynamodb_mate as pm
from pathlib_mate import Path
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context
from rich import print as rprint
import aws_textract_pipeline.api as aws_textract_pipeline

bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
context.attach_boto_session(bsm.boto_ses)
bucket = f"{bsm.aws_account_alias}-{bsm.aws_region}-data"
s3dir_root = S3Path(f"s3://{bucket}/projects/aws_textract_pipeline/doc_store/").to_dir()
print(f"s3dir_root: {s3dir_root.console_url}")
dir_here = Path.dir_here(__file__)
path_doc = dir_here / "f1040.pdf"

ws = aws_textract_pipeline.Workspace(s3dir_uri=s3dir_root.uri)


class StatusAndUpdateTimeIndex(aws_textract_pipeline.BaseStatusAndUpdateTimeIndex):
    pass


class Tracker(aws_textract_pipeline.BaseTracker):
    class Meta:
        table_name = "aws_textract_pipeline-tracker"
        region = bsm.aws_region
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


with bsm.awscli():
    pm.Connection()
    Tracker.create_table(wait=True)
    print(f"Dynamodb Table: {Tracker.get_table_items_console_url()}")

# Reset everything
s3dir_root.delete()
Tracker.delete_all()

landing_doc = aws_textract_pipeline.LandingDocument(
    s3uri=ws.s3dir_landing.joinpath(path_doc.basename).uri,
    doc_type=aws_textract_pipeline.DocTypeEnum.pdf.value,
)
landing_doc.dump(bsm=bsm, body=path_doc.read_bytes())

# fmt: off
tracker = Tracker.new_from_landing_doc(bsm=bsm, landing_doc=landing_doc)
tracker.landing_to_raw(bsm=bsm, workspace=ws, debug=True)
components = tracker.raw_to_component(bsm=bsm, workspace=ws, clear_tmp_dir=True, debug=True)
component_to_textract_output_result = tracker.component_to_textract_output(bsm=bsm, workspace=ws, use_form_feature=True, debug=True)
rprint(component_to_textract_output_result)
component_to_textract_output_result.wait_document_analysis_job_to_succeed(bsm=bsm, timeout=300, verbose=True)

# tracker = Tracker.get_one_or_none(task_id="5355b0c5deb635c613b45246475123c2")
tracker.textract_output_to_text_and_json(bsm=bsm, workspace=ws, debug=True)
# fmt: on

