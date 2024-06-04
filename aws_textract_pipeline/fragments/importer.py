# -*- coding: utf-8 -*-

from ..helpers import ImportErrorHandler

try:
    from . import pdf_api as pdf

    _pdf = pdf
except ImportError as e:
    pdf = ImportErrorHandler(
        path="aws_textract_pipeline.fragments.pdf",
        dependencies=["PyMuPDF>=1.23.26,<2.0.0"],
        error=e,
    )
