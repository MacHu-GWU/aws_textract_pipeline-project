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
        return self.s3dir.joinpath("010-landing").to_dir()

    @property
    def s3dir_raw(self) -> S3Path:
        return self.s3dir.joinpath("020-raw").to_dir()

    @property
    def s3dir_component(self) -> S3Path:
        return self.s3dir.joinpath("030-component").to_dir()

    @property
    def s3dir_image(self) -> S3Path:
        return self.s3dir.joinpath("040-image").to_dir()

    @property
    def s3dir_textract_output(self) -> S3Path:
        return self.s3dir.joinpath("050-textract-output").to_dir()

    @property
    def s3dir_text(self) -> S3Path:
        return self.s3dir.joinpath("060-text").to_dir()

    @property
    def s3dir_json(self) -> S3Path:
        return self.s3dir.joinpath("070-json").to_dir()

    @property
    def s3dir_extracted_data(self) -> S3Path:
        return self.s3dir.joinpath("080-extracted-data").to_dir()

    @property
    def s3dir_extracted_data_hil_output(self) -> S3Path:
        return self.s3dir.joinpath("090-extracted-data-hil-output").to_dir()

    @property
    def s3dir_extracted_data_hil_post_process(self) -> S3Path:
        return self.s3dir.joinpath("090-extracted-data-hil-post-process").to_dir()

    def get_raw_s3path(self, doc_id: str) -> S3Path:
        return self.s3dir_raw.joinpath(doc_id)

    def get_component_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_component.joinpath(doc_id, comp_id)

    def get_image_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_image.joinpath(doc_id, comp_id)

    def get_textract_output_s3dir(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_textract_output.joinpath(doc_id, comp_id).to_dir()

    def get_text_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_text.joinpath(doc_id, comp_id)

    def get_json_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_json.joinpath(doc_id, comp_id)

    def get_extracted_data_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_extracted_data.joinpath(doc_id, comp_id)

    def get_extracted_data_hil_output_s3dir(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_extracted_data_hil_output.joinpath(doc_id, comp_id).to_dir()

    def get_extracted_data_hil_post_process_s3path(self, doc_id: str, comp_id: str) -> S3Path:
        return self.s3dir_extracted_data_hil_post_process.joinpath(doc_id, comp_id)
    # fmt: on
