# -*- coding: utf-8 -*-

"""
todo: add docstring
"""

import dataclasses
import tarfile

from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from .vendor.better_enum import BetterStrEnum
from .vendor.better_dataclasses import DataClass
from .vendor.hashes import hashes, HashAlgoEnum

from .doc_type import DocTypeEnum


class MetadataKeyEnum(BetterStrEnum):
    doc_type = "doc_type"


@dataclasses.dataclass
class LandingDocument(DataClass):
    """
    Represent a document in landing zone.

    The metadata of the file in landing zone should include the following information::

        {
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
        return S3Path(self.s3uri).write_bytes(
            body,
            metadata={
                MetadataKeyEnum.doc_type.value: self.doc_type,
            },
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
