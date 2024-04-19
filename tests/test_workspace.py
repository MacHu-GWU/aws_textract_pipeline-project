# -*- coding: utf-8 -*-

from aws_textract_pipeline.workspace import Workspace


class TestWorkspace:
    def test(self):
        ws = Workspace(s3dir_uri="s3://bucket/root/")
        doc_id = "doc-1"
        comp_id = "000001"

        _ = ws.s3dir
        _ = ws.s3dir_landing
        _ = ws.s3dir_raw
        _ = ws.s3dir_component
        _ = ws.s3dir_image
        _ = ws.s3dir_textract_output
        _ = ws.s3dir_text
        _ = ws.s3dir_json
        _ = ws.s3dir_extracted_data
        _ = ws.s3dir_extracted_data_hil_output
        _ = ws.s3dir_extracted_data_hil_post_process

        _ = ws.get_raw_s3path(doc_id)
        _ = ws.get_component_s3path(doc_id, comp_id)
        _ = ws.get_image_s3path(doc_id, comp_id)
        _ = ws.get_textract_output_s3dir(doc_id, comp_id)
        _ = ws.get_text_s3path(doc_id, comp_id)
        _ = ws.get_json_s3path(doc_id, comp_id)
        _ = ws.get_extracted_data_s3path(doc_id, comp_id)
        _ = ws.get_extracted_data_hil_output_s3dir(doc_id, comp_id)
        _ = ws.get_extracted_data_hil_post_process_s3path(doc_id, comp_id)


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.workspace", preview=False)
