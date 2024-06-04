# -*- coding: utf-8 -*-

from aws_textract_pipeline.fragments.pdf import divide_pdf_into_pages
from aws_textract_pipeline.paths import dir_unit_test


def test_divide_pdf_into_pages():
    path_pdf = dir_unit_test / "data" / "f1040.pdf"
    res = divide_pdf_into_pages(pdf_content=path_pdf.read_bytes(), extract_image=True)
    assert len(res.page_pdf_list) == 2
    assert len(res.page_image_list) == 2


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.fragments.pdf", preview=False)
