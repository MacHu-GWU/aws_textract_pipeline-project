# -*- coding: utf-8 -*-

"""
todo: add docstring
"""

import typing as T

from .vendor.better_enum import BetterStrEnum


class DocTypeEnum(BetterStrEnum):
    """
    Document Type Enum.
    """

    pdf = "pdf"
    jpg = "jpg"
    png = "png"
    bmp = "bmp"
    gif = "gif"
    tiff = "tiff"
    text = "text"
    word = "word"
    excel = "excel"
    ppt = "ppt"
    json = "json"
    csv = "csv"
    tsv = "tsv"
    unknown = "unknown"

    @classmethod
    def detect_doc_type(cls, filename: str) -> str:
        parts = filename.lower().split(".")
        if len(parts) == 1:
            return cls.unknown.value
        elif parts[-1] == "gz":
            if len(parts) >= 3:
                return ext_to_doc_type_mapper.get(parts[-2], cls.unknown.value)
            else:
                return cls.unknown.value
        else:
            return ext_to_doc_type_mapper.get(parts[-1], cls.unknown.value)


ext_to_doc_type_mapper = {
    "pdf": DocTypeEnum.pdf.value,
    "jpg": DocTypeEnum.jpg.value,
    "jpeg": DocTypeEnum.jpg.value,
    "png": DocTypeEnum.png.value,
    "bmp": DocTypeEnum.bmp.value,
    "tiff": DocTypeEnum.tiff.value,
    "gif": DocTypeEnum.gif.value,
    "txt": DocTypeEnum.text.value,
    "doc": DocTypeEnum.word.value,
    "docx": DocTypeEnum.word.value,
    "xls": DocTypeEnum.excel.value,
    "xlsx": DocTypeEnum.excel.value,
    "ppt": DocTypeEnum.ppt.value,
    "pptx": DocTypeEnum.ppt.value,
    "json": DocTypeEnum.json.value,
    "csv": DocTypeEnum.csv.value,
    "tsv": DocTypeEnum.tsv.value,
}


class S3ContentTypeEnum(BetterStrEnum):
    """
    AWS S3 Content Type

    Ref:

    - https://www.ibm.com/docs/en/aspera-on-cloud?topic=SS5W4X/dita/content/aws_s3_content_types.htm
    """

    # pure text
    text_plain = "text/plain"

    # image
    image_png = "image/png"
    image_jpg = "image/jpeg"
    image_bmp = "image/bmp"
    image_tiff = "image/tiff"
    image_gif = "image/gif"

    # document
    ms_word = "application/msword"
    ms_ppt = "application/mspowerpoint"
    ms_excel = "application/x-msexcel"

    pdf = "	application/pdf"

    # archive
    zip = "application/zip"
    gzip = "application/x-gzip"
    tar = "application/x-tar"
    tgz = "application/x-compressed"

    # data format
    json = "application/json"
    csv = "text/csv"


doc_type_to_content_type_mapper: T.Dict[str, T.Optional[str]] = {
    DocTypeEnum.pdf.value: S3ContentTypeEnum.pdf.value,
    DocTypeEnum.jpg.value: S3ContentTypeEnum.image_jpg.value,
    DocTypeEnum.png.value: S3ContentTypeEnum.image_png.value,
    DocTypeEnum.bmp.value: S3ContentTypeEnum.image_bmp.value,
    DocTypeEnum.gif.value: S3ContentTypeEnum.image_gif.value,
    DocTypeEnum.tiff.value: S3ContentTypeEnum.image_tiff.value,
    DocTypeEnum.text.value: S3ContentTypeEnum.text_plain.value,
    DocTypeEnum.word.value: S3ContentTypeEnum.ms_word.value,
    DocTypeEnum.excel.value: S3ContentTypeEnum.ms_excel.value,
    DocTypeEnum.ppt.value: S3ContentTypeEnum.ms_ppt.value,
    DocTypeEnum.json.value: S3ContentTypeEnum.json.value,
    DocTypeEnum.csv.value: S3ContentTypeEnum.csv.value,
    DocTypeEnum.tsv.value: S3ContentTypeEnum.csv.value,
    DocTypeEnum.unknown.value: None,
}
