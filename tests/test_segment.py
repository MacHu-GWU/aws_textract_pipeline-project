# -*- coding: utf-8 -*-

from aws_textract_pipeline.segment import segment_pdf
from aws_textract_pipeline.paths import dir_unit_test


def test_segment_pdf():
    path_pdf = dir_unit_test / "data" / "f1040.pdf"
    res = segment_pdf(path_pdf.read_bytes())


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.segment", preview=False)
