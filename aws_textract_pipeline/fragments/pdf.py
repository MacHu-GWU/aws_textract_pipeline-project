# -*- coding: utf-8 -*-

"""
This module provides utilities to segment a document into components.

See:

- :class:`SegmentPdfResult`
- :func:`segment_pdf`
"""

import typing as T
import io
import dataclasses

import fitz
from ..vendor.better_dataclasses import DataClass


@dataclasses.dataclass
class DividePdfIntoPagesResult(DataClass):
    """
    Returned object of :func:`divide_pdf`.

    To save ``fitz.Document`` object to local file, use the following code::

        >>> res = DividePdfIntoPagesResult(...)
        >>> page = res.page_pdf_list[0]
        >>> page.save("/path/to/save/page.pdf")

    To save ``fitz.Pixmap`` object to local file, use the following code::

        >>> res = DividePdfIntoPagesResult(...)
        >>> pixmap = res.page_image_list[0]
        >>> pixmap.save("/path/to/save/image.png", output="png")

    To get width and height of the image, use the following code::

        >>> pixmap.width
        >>> pixmap.height
    """

    page_pdf_list: T.List[fitz.Document] = dataclasses.field(default_factory=list)
    page_image_list: T.List[fitz.Pixmap] = dataclasses.field(default_factory=list)


def divide_pdf_into_pages(
    pdf_content: bytes,
    extract_image: bool = False,
    dpi: int = 200,
) -> DividePdfIntoPagesResult:
    """
    Divide PDF into pages.

    :param pdf_content: PDF content in bytes.
    :param extract_image: do we extract image from PDF.
    :param dpi: DPI of the image.
    """
    # read original PDF into memory
    pdf = fitz.Document(stream=pdf_content)

    # Repair any issues (hopefully) before we hit them
    # See this https://github.com/pymupdf/PyMuPDF/issues/856
    buffer = io.BytesIO()
    # write the document to in-memory buffer
    buffer.write(pdf.write(clean=True, garbage=4))
    new_content = buffer.getvalue()
    buffer.close()

    pdf_cleaned = fitz.Document(stream=new_content)

    page_pdf_list = list()
    page_image_list = list()

    for page_num, page in enumerate(pdf_cleaned, start=1):
        # extract page as PDF
        pdf_page = fitz.Document()
        pdf_page.insert_pdf(
            pdf_cleaned,
            from_page=page_num - 1,
            to_page=page_num - 1,
        )
        page_pdf_list.append(pdf_page)

        # extract page as image
        if extract_image:
            pixmap = page.get_pixmap(dpi=dpi)
            page_image_list.append(pixmap)

    return DividePdfIntoPagesResult(
        page_pdf_list=page_pdf_list,
        page_image_list=page_image_list,
    )
