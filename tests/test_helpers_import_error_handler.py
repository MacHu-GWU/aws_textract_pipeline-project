# -*- coding: utf-8 -*-

import pytest
from aws_textract_pipeline.tests.import_error_handler import module1, module2


def test():
    assert module1.module1_func1() == "module1_func1"
    with pytest.raises(ImportError):
        module2.module2_func1()


if __name__ == "__main__":
    from aws_textract_pipeline.tests import run_cov_test

    run_cov_test(__file__, "aws_textract_pipeline.helpers", preview=False)
