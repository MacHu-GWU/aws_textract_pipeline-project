# -*- coding: utf-8 -*-

from s3pathlib import S3Path

from aws_textract_pipeline.landing import (
    LandingDocument,
    MetadataKeyEnum,
    DocTypeEnum,
    get_tar_file_md5,
)
from aws_textract_pipeline.paths import dir_unit_test
from aws_textract_pipeline.tests.mock_test import BaseTest


class TestLandingDocument(BaseTest):
    @classmethod
    def setup_class_post_hook(cls):
        cls.setup_s3_and_dynamodb()

    def test_load_and_dump(self):
        s3path = S3Path(self.bucket, "landing/test.pdf")

        doc = LandingDocument(s3uri=s3path.uri, doc_type=DocTypeEnum.pdf.value)
        doc.dump(bsm=self.bsm, body=b"test")

        s3path.head_object(bsm=self.bsm)
        assert s3path.read_text() == "test"
        assert s3path.metadata[MetadataKeyEnum.doc_type.value] == DocTypeEnum.pdf.value

        doc = LandingDocument.load(bsm=self.bsm, s3path=s3path)
        assert doc.s3uri == s3path.uri
        assert doc.doc_type == DocTypeEnum.pdf.value

    def test_get_tar_file_md5(self):
        path = dir_unit_test / "src.tar.gz"
        s3path = S3Path(self.bucket, "src.tar.gz")
        s3path.upload_file(path)
        md5 = get_tar_file_md5(bsm=self.bsm, s3path=s3path)
        assert len(md5) == 32


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.landing", preview=False)
