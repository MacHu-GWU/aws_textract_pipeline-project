# -*- coding: utf-8 -*-

from aws_textract_pipeline.doc_type import DocTypeEnum


class TestDocTypeEnum:
    def test_detect_doc_type(self):
        assert DocTypeEnum.detect_doc_type("test.pdf") == "pdf"
        assert DocTypeEnum.detect_doc_type("test.pdf.gz") == "pdf"
        assert DocTypeEnum.detect_doc_type("test.xyz") == "unknown"
        assert DocTypeEnum.detect_doc_type("test") == "unknown"
        assert DocTypeEnum.detect_doc_type("test.gz") == "unknown"
        assert DocTypeEnum.detect_doc_type("test.xyz.gz") == "unknown"


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.doc_type", preview=False)
