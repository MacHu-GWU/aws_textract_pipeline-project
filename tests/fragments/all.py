# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.fragments", is_folder=True, preview=False)
