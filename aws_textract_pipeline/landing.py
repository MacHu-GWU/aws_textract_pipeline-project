# -*- coding: utf-8 -*-

"""
Landing bucket is where the intake documents are stored at the beginning of the pipeline.
"""

import dataclasses
import tarfile

from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from .vendor.better_enum import BetterStrEnum
from .vendor.better_dataclasses import DataClass
from .vendor.hashes import hashes, HashAlgoEnum

from .doc_type import DocTypeEnum, doc_type_to_content_type_mapper


class MetadataKeyEnum(BetterStrEnum):
    landing_s3uri = "landing_s3uri"
    doc_type = "doc_type"
    doc_id = "doc_id"
    component_id = "component_id"


@dataclasses.dataclass
class LandingDocument(DataClass):
    """
    Represent a document in landing zone. A document in landing zone is a single
    S3 object. The metadata of the S3 object should include the following information::

        {
            "landing_s3uri": "s3://bucket/key" # the S3 URI of the document in landing zone
            "doc_type": "pdf|word|excel|ppt|image|..." # the type of the document
        }
    """

    s3uri: str = dataclasses.field()
    doc_type: str = dataclasses.field()

    @classmethod
    def load(
        cls,
        bsm: "BotoSesManager",
        s3path: "S3Path",
    ):
        """
        Load a LandingDocument object from S3 object.

        :param bsm: the ``boto_session_manager.BotoSesManager`` object.
        :param s3path: the S3 path of the document in landing zone.
        """
        s3path.head_object(bsm=bsm)
        doc_type = s3path.metadata[MetadataKeyEnum.doc_type.value]
        DocTypeEnum.ensure_is_valid_value(doc_type)
        return cls(
            s3uri=s3path.uri,
            doc_type=doc_type,
        )

    def dump(
        self,
        bsm: "BotoSesManager",
        body: bytes,
    ) -> "S3Path":
        """
        Dump the LandingDocument object to S3 object.

        This method is used in the ingestion pipeline (prior to the Textract pipeline)
        to dump the document to the landing zone.

        :param bsm: the ``boto_session_manager.BotoSesManager`` object.
        :param body: the binary content of the document.
        """
        return S3Path(self.s3uri).write_bytes(
            body,
            metadata={
                MetadataKeyEnum.landing_s3uri.value: self.s3uri,
                MetadataKeyEnum.doc_type.value: self.doc_type,
            },
            content_type=doc_type_to_content_type_mapper[self.doc_type],
            bsm=bsm,
        )


def get_md5_of_bytes(b: bytes) -> str:
    """
    Get md5 of a binary object.
    """
    return hashes.of_bytes(b=b, algo=HashAlgoEnum.md5, hexdigest=True)


def get_tar_file_md5(
    bsm: "BotoSesManager",
    s3path: "S3Path",
) -> str:
    """
    Get md5 of all files in a tar file on S3. This md5 is deterministic.
    This md5 value is used as the content-based unique id of a document.
    """
    with s3path.open("rb", bsm=bsm) as fileobj:
        with tarfile.open(fileobj=fileobj, mode="r") as tar:
            file_members = [member for member in tar.getmembers() if member.isfile()]
            sorted_file_members = list(
                sorted(
                    file_members,
                    key=lambda x: x.name,
                )
            )
            md5_list = list()
            for member in sorted_file_members:
                f = tar.extractfile(member)
                if f is not None:
                    content = f.read()
                    md5 = get_md5_of_bytes(content)
                    md5_list.append(md5)
    md5 = get_md5_of_bytes("-".join(md5_list).encode("utf-8"))
    return md5


def get_doc_md5(
    bsm: "BotoSesManager",
    s3path: "S3Path",
    doc_type: str,
) -> str:
    """
    Get the md5 of the document based on it's content. In Landing zone, we may use
    the file name as the S3 object key. However, the file name is not unique. The md5
    of the content is a better value for the S3 object key.
    """
    if doc_type in [
        DocTypeEnum.pdf.value,
        DocTypeEnum.jpg.value,
        DocTypeEnum.png.value,
        DocTypeEnum.bmp.value,
        DocTypeEnum.gif.value,
        DocTypeEnum.tiff.value,
        DocTypeEnum.text.value,
        DocTypeEnum.word.value,
        DocTypeEnum.excel.value,
        DocTypeEnum.ppt.value,
        DocTypeEnum.json.value,
        DocTypeEnum.csv.value,
        DocTypeEnum.tsv.value,
    ]:
        return get_md5_of_bytes(s3path.read_bytes(bsm=bsm))
    else:
        raise TypeError(f"Unsupported doc_type: {doc_type}")
