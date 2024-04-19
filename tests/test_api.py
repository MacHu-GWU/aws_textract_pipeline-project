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


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.api", preview=False)
