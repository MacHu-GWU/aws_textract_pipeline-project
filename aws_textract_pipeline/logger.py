# -*- coding: utf-8 -*-

from .vendor.nested_logger import NestedLogger

logger = NestedLogger(
    name="aws_textract_pipeline",
    log_format="%(message)s",
)
