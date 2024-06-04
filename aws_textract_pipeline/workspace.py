# -*- coding: utf-8 -*-

"""
todo: add docstring
"""

import dataclasses

from s3pathlib import S3Path


@dataclasses.dataclass
class Workspace:
    """
    Workspace class to hold the S3 directory URI and provide methods to get
    S3 paths for different stages of the pipeline.

    :param s3dir_uri: the root S3 directory URI. All the S3 paths are relative to this directory.
    """

    s3dir_uri: str

    # fmt: off
    @property
    def s3dir(self) -> S3Path:
        return S3Path.from_s3_uri(self.s3dir_uri).to_dir()

    @property
    def s3dir_landing(self) -> S3Path:
        return self.s3dir.joinpath("0010-landing").to_dir()

    @property
    def s3dir_raw(self) -> S3Path:
        return self.s3dir.joinpath("0020-raw").to_dir()

    @property
    def s3dir_fragment(self) -> S3Path:
        return self.s3dir.joinpath("0030-fragments").to_dir()

    @property
    def s3dir_image(self) -> S3Path:
        return self.s3dir.joinpath("0040-image").to_dir()

    @property
    def s3dir_textract_text_detection_output(self) -> S3Path:
        return self.s3dir.joinpath("0050-text-detection-output").to_dir()

    @property
    def s3dir_textract_text_detection_text(self) -> S3Path:
        return self.s3dir.joinpath("0060-text-detection-output-text").to_dir()

    @property
    def s3dir_textract_text_detection_json(self) -> S3Path:
        return self.s3dir.joinpath("0070-text-detection-output-json").to_dir()

    @property
    def s3dir_textract_document_analysis_output(self) -> S3Path:
        return self.s3dir.joinpath("0080-document-analysis-output").to_dir()

    @property
    def s3dir_textract_document_analysis_text(self) -> S3Path:
        return self.s3dir.joinpath("0090-document-analysis-output-text").to_dir()

    @property
    def s3dir_textract_document_analysis_json(self) -> S3Path:
        return self.s3dir.joinpath("0100-document-analysis-output-json").to_dir()

    @property
    def s3dir_textract_expense_analysis_output(self) -> S3Path:
        return self.s3dir.joinpath("0110-expense-analysis-output").to_dir()

    @property
    def s3dir_textract_expense_analysis_text(self) -> S3Path:
        return self.s3dir.joinpath("0120-expense-analysis-output-text").to_dir()

    @property
    def s3dir_textract_expense_analysis_json(self) -> S3Path:
        return self.s3dir.joinpath("0130-expense-analysis-output-json").to_dir()

    @property
    def s3dir_textract_lending_analysis_output(self) -> S3Path:
        return self.s3dir.joinpath("0140-lending-analysis-output").to_dir()

    @property
    def s3dir_textract_lending_analysis_text(self) -> S3Path:
        return self.s3dir.joinpath("0150-lending-analysis-output-text").to_dir()

    @property
    def s3dir_textract_lending_analysis_json(self) -> S3Path:
        return self.s3dir.joinpath("0160-lending-analysis-output-json").to_dir()

    @property
    def s3dir_extracted_data(self) -> S3Path:
        return self.s3dir.joinpath("0170-extracted-data").to_dir()

    @property
    def s3dir_extracted_data_hil_output(self) -> S3Path:
        return self.s3dir.joinpath("0180-extracted-data-hil-output").to_dir()

    @property
    def s3dir_extracted_data_hil_post_process(self) -> S3Path:
        return self.s3dir.joinpath("0190-extracted-data-hil-post-process").to_dir()

    def get_raw_s3path(self, doc_id: str) -> S3Path:
        return self.s3dir_raw.joinpath(doc_id)

    def get_fragment_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_fragment.joinpath(doc_id, frag_id)

    def get_image_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_image.joinpath(doc_id, frag_id)

    def get_textract_text_detection_output_s3dir(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_text_detection_output.joinpath(doc_id, frag_id).to_dir()

    def get_textract_text_detection_text_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_text_detection_text.joinpath(doc_id, frag_id)

    def get_textract_text_detection_json_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_text_detection_json.joinpath(doc_id, frag_id)

    def get_textract_document_analysis_output_s3dir(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_document_analysis_output.joinpath(doc_id, frag_id).to_dir()

    def get_textract_document_analysis_text_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_document_analysis_text.joinpath(doc_id, frag_id)

    def get_textract_document_analysis_json_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_document_analysis_json.joinpath(doc_id, frag_id)

    def get_textract_expense_analysis_output_s3dir(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_expense_analysis_output.joinpath(doc_id, frag_id).to_dir()

    def get_textract_expense_analysis_text_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_expense_analysis_text.joinpath(doc_id, frag_id)

    def get_textract_expense_analysis_json_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_expense_analysis_json.joinpath(doc_id, frag_id)

    def get_textract_lending_analysis_output_s3dir(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_lending_analysis_output.joinpath(doc_id, frag_id).to_dir()

    def get_textract_lending_analysis_text_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_lending_analysis_text.joinpath(doc_id, frag_id)

    def get_textract_lending_analysis_json_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_textract_lending_analysis_json.joinpath(doc_id, frag_id)
    
    def get_extracted_data_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_extracted_data.joinpath(doc_id, frag_id)

    def get_extracted_data_hil_output_s3dir(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_extracted_data_hil_output.joinpath(doc_id, frag_id).to_dir()

    def get_extracted_data_hil_post_process_s3path(self, doc_id: str, frag_id: str) -> S3Path:
        return self.s3dir_extracted_data_hil_post_process.joinpath(doc_id, frag_id)
    # fmt: on
