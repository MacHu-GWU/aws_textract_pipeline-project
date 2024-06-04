# -*- coding: utf-8 -*-

from aws_textract_pipeline.workspace import Workspace


class TestWorkspace:
    def test(self):
        ws = Workspace(s3dir_uri="s3://bucket/root/")
        doc_id = "doc-1"
        frag_id = "000001"

        _ = ws.s3dir
        _ = ws.s3dir_landing
        _ = ws.s3dir_raw
        _ = ws.s3dir_fragment
        _ = ws.s3dir_image
        _ = ws.s3dir_textract_text_detection_output
        _ = ws.s3dir_textract_text_detection_text
        _ = ws.s3dir_textract_text_detection_json
        _ = ws.s3dir_textract_document_analysis_output
        _ = ws.s3dir_textract_document_analysis_text
        _ = ws.s3dir_textract_document_analysis_json
        _ = ws.s3dir_textract_expense_analysis_output
        _ = ws.s3dir_textract_expense_analysis_text
        _ = ws.s3dir_textract_expense_analysis_json
        _ = ws.s3dir_textract_lending_analysis_output
        _ = ws.s3dir_textract_lending_analysis_text
        _ = ws.s3dir_textract_lending_analysis_json
        _ = ws.s3dir_extracted_data
        _ = ws.s3dir_extracted_data_hil_output
        _ = ws.s3dir_extracted_data_hil_post_process

        _ = ws.get_raw_s3path(doc_id)
        _ = ws.get_fragment_s3path(doc_id, frag_id)
        _ = ws.get_image_s3path(doc_id, frag_id)
        _ = ws.get_textract_text_detection_output_s3dir(doc_id, frag_id)
        _ = ws.get_textract_text_detection_text_s3path(doc_id, frag_id)
        _ = ws.get_textract_text_detection_json_s3path(doc_id, frag_id)
        _ = ws.get_textract_document_analysis_output_s3dir(doc_id, frag_id)
        _ = ws.get_textract_document_analysis_text_s3path(doc_id, frag_id)
        _ = ws.get_textract_document_analysis_json_s3path(doc_id, frag_id)
        _ = ws.get_textract_expense_analysis_output_s3dir(doc_id, frag_id)
        _ = ws.get_textract_expense_analysis_text_s3path(doc_id, frag_id)
        _ = ws.get_textract_expense_analysis_json_s3path(doc_id, frag_id)
        _ = ws.get_textract_lending_analysis_output_s3dir(doc_id, frag_id)
        _ = ws.get_textract_lending_analysis_text_s3path(doc_id, frag_id)
        _ = ws.get_textract_lending_analysis_json_s3path(doc_id, frag_id)
        _ = ws.get_extracted_data_s3path(doc_id, frag_id)
        _ = ws.get_extracted_data_hil_output_s3dir(doc_id, frag_id)
        _ = ws.get_extracted_data_hil_post_process_s3path(doc_id, frag_id)


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.workspace", preview=False)
