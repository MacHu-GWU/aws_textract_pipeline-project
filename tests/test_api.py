# -*- coding: utf-8 -*-

from aws_textract_pipeline import api


def test():
    _ = api
    _ = api.DocTypeEnum
    _ = api.S3ContentTypeEnum
    _ = api.Workspace
    _ = api.MetadataKeyEnum
    _ = api.LandingDocument
    _ = api.get_md5_of_bytes
    _ = api.get_tar_file_md5
    _ = api.BaseStatusAndUpdateTimeIndex
    _ = api.BaseTracker

    _ = api.types.DocTypeEnum
    _ = api.types.ext_to_doc_type_mapper
    _ = api.types.S3ContentTypeEnum
    _ = api.types.doc_type_to_content_type_mapper
    _ = api.fragments.pdf.DividePdfIntoPagesResult
    _ = api.fragments.pdf.divide_pdf_into_pages


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.api", preview=False)
