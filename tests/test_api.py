# -*- coding: utf-8 -*-

from aws_textract_pipeline import api


def test():
    _ = api


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.api", preview=False)
