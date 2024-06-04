# -*- coding: utf-8 -*-

# import python libraries
from pathlib_mate import Path
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager
import pynamodb_mate.api as pm
from rich import print as rprint

import aws_textract_pipeline.api as aws_textract_pipeline

# Setup your AWS environment
bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
context.attach_boto_session(bsm.boto_ses)
bucket = f"{bsm.aws_account_alias}-{bsm.aws_region}-data"
s3dir_root = S3Path(f"s3://{bucket}/projects/aws_textract_pipeline/doc_store/").to_dir()
print(f"s3dir_root: {s3dir_root.console_url}")

# Setup the sample document
dir_here = Path.dir_here(__file__)
path_doc = dir_here / "f1040.pdf"

# Setup important context for aws_textract_pipeline
ws = aws_textract_pipeline.Workspace(s3dir_uri=s3dir_root.uri)


# Declare the DynamoDB table
class StatusAndUpdateTimeIndex(
    aws_textract_pipeline.trackers.BaseStatusAndUpdateTimeIndex
):
    pass


class DynamoDBTableMeta:
    table_name = "aws_textract_pipeline-tracker"
    region = bsm.aws_region
    billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE


class LandingToRawTask(aws_textract_pipeline.trackers.LandingToRawTask):
    Meta = DynamoDBTableMeta
    status_and_update_time_index = StatusAndUpdateTimeIndex()


class RawToFragmentTask(aws_textract_pipeline.trackers.RawToFragmentTask):
    # Meta = DynamoDBTableMeta

    class Meta:
        table_name = "aws_textract_pipeline-tracker"
        region = bsm.aws_region
        billing_mode = pm.constants.PAY_PER_REQUEST_BILLING_MODE

    status_and_update_time_index = StatusAndUpdateTimeIndex()


# Create the DynamoDB table
with bsm.awscli():
    pm.Connection()
    LandingToRawTask.create_table(wait=True)
    print(f"Dynamodb Table: {LandingToRawTask.get_table_items_console_url()}")

# Reset everything (delete all documents in S3 and all records in DynamoDB tracker)
s3dir_root.delete()
LandingToRawTask.delete_all()

# Start the ETL pipeline
landing_doc = aws_textract_pipeline.LandingDocument(
    s3uri=ws.s3dir_landing.joinpath(path_doc.basename).uri,
    doc_type=aws_textract_pipeline.types.DocTypeEnum.pdf.value,
    features=["FORMS"],
)
landing_doc.dump(bsm=bsm, body=path_doc.read_bytes())

landing_to_raw_task = LandingToRawTask.new_from_landing_doc(
    bsm=bsm,
    landing_doc=landing_doc,
    save=True,
)
doc_id = landing_to_raw_task.doc_id

# fmt: off
# --- method 1
LandingToRawTask.run(doc_id=doc_id, bsm=bsm, workspace=ws, detailed_error=True, debug=True)
RawToFragmentTask.run(doc_id=doc_id, bsm=bsm, workspace=ws, clear_tmp_dir=True, detailed_error=True, debug=True)


# --- method 1
# tracker.landing_to_raw(bsm=bsm, workspace=ws, debug=True)
# components = tracker.raw_to_component(bsm=bsm, workspace=ws, clear_tmp_dir=True, debug=True)
# component_to_textract_output_result = tracker.component_to_textract_output(bsm=bsm, workspace=ws, use_form_feature=True, debug=True)
# component_to_textract_output_result.wait_document_analysis_job_to_succeed(bsm=bsm, timeout=300, verbose=True)
# tracker = Tracker.get_one_or_none(task_id="5355b0c5deb635c613b45246475123c2") # for debug only
# tracker.textract_output_to_text_and_json(bsm=bsm, workspace=ws, debug=True)

# --- method 2
# res = tracker.move_to_next_stage(bsm=bsm, workspace=ws, debug=True, use_form_feature=True)
# res = tracker.move_to_next_stage(bsm=bsm, workspace=ws, debug=True, use_form_feature=True)
# res = tracker.move_to_next_stage(bsm=bsm, workspace=ws, debug=True, use_form_feature=True)
# res.component_to_textract_output_result.wait_document_analysis_job_to_succeed(bsm=bsm, timeout=300, verbose=True)
# res = tracker.move_to_next_stage(bsm=bsm, workspace=ws, debug=True, use_form_feature=True)
# rprint(res)
# fmt: on
