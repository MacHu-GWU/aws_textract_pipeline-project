# -*- coding: utf-8 -*-

# standard library
import typing as T
import dataclasses

# third party libraries
import pynamodb_mate.api as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager
import aws_textract.api as aws_textract
from ..vendor.better_enum import BetterStrEnum
from ..vendor.better_dataclasses import DataClass

# aws_textract_pipeline modules
from ..constants import USE_CASE_ID
from ..logger import logger
from ..doc_type import DocTypeEnum, S3ContentTypeEnum
from ..landing import MetadataKeyEnum, LandingDocument, get_doc_md5
from ..segment import segment_pdf
from ..workspace import Workspace

# current module
from .status import StatusEnum


st = pm.patterns.status_tracker


# ------------------------------------------------------------------------------
# DynamoDB ORM Model
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class Fragment(DataClass):
    """
    Metadata for each component.
    """

    id: str = dataclasses.field()


@dataclasses.dataclass
class FragmentToTextractOutputResult(DataClass):
    """
    The returned object for creating textract output for all components of a
    document. This information will be used to parse the textract output data
    later.

    :param is_single_textract_api_call: it is more efficient to use single
        textract API call instead multiple API calls on each component.
        we try to use single API if the document fit the quota. Otherwise,
        we split and make multiple API calls.
    :param job_id: the textract job id, only available if we only made one API call.
    :param job_id_list: the textract job id for each component, only available if we
        made multiple API calls.
    """

    is_single_textract_api_call: bool = dataclasses.field()
    job_id: T.Optional[str] = dataclasses.field()
    job_id_list: T.Optional[T.List[str]] = dataclasses.field()

    def wait_document_analysis_job_to_succeed(
        self,
        bsm: "BotoSesManager",
        delays: int = 5,
        timeout: int = 60,
        verbose: bool = True,
    ):  # pragma: no cover
        """
        Wait all Textract API call to succeed for this document.
        """
        if self.is_single_textract_api_call:
            aws_textract.better_boto.wait_document_analysis_job_to_succeed(
                textract_client=bsm.textract_client,
                job_id=self.job_id,
                delays=delays,
                timeout=timeout,
                verbose=verbose,
            )
        else:
            for job_id in self.job_id_list:
                aws_textract.better_boto.wait_document_analysis_job_to_succeed(
                    textract_client=bsm.textract_client,
                    job_id=job_id,
                    delays=delays,
                    timeout=timeout,
                    verbose=verbose,
                )
                if verbose:
                    print("")


@dataclasses.dataclass
class TextractOutputToTextAndJsonResult(DataClass):
    text_list: T.List[str] = dataclasses.field(default_factory=list)
    json_list: T.List[dict] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Data(DataClass):
    """
    Additional data about this document.

    :param landing_uri: where is the original s3 object in landing. This is because
        given a landing file, we can easily calculate the doc id, but cannot do it
        reversely. so we have to store this value and attach to s3 objects in
        sub-sequence logics.
    :param doc_type: the document type.
    :param components:
    :param component_to_textract_output_result:
    """

    # fmt: off
    landing_uri: str = dataclasses.field()
    doc_type: str = dataclasses.field()
    features: T.List[str] = dataclasses.field(default_factory=list)
    fragments: T.List[Fragment] = Fragment.list_of_nested_field(default_factory=list)
    fragment_to_textract_output_result: T.Optional[FragmentToTextractOutputResult] = FragmentToTextractOutputResult.nested_field(default=None)
    # fmt: on

    @property
    def n_fragments(self):
        """
        Number of fragments.
        """
        return len(self.fragments)


@dataclasses.dataclass
class Errors(DataClass):
    """
    Runtime error information for debug.

    :param error: error message.
    :param traceback: Python traceback information.
    """

    error: T.Optional[str] = dataclasses.field(default=None)
    traceback: T.Optional[str] = dataclasses.field(default=None)


class BaseStatusAndUpdateTimeIndex(
    pm.patterns.status_tracker.StatusAndUpdateTimeIndex,
):
    """
    Status Tracker GSI index, to allow lookup by status.
    """

    pass


class BaseTask(st.BaseTask):
    @property
    def doc_id(self) -> str:
        return self.task_id

    @property
    def data_obj(self) -> Data:
        return Data.from_dict(self.data)

    @property
    def errors_obj(self) -> Errors:
        return Errors.from_dict(self.errors)

    @classmethod
    def new_from_landing_doc(
        cls,
        bsm: "BotoSesManager",
        landing_doc: LandingDocument,
        save: bool = True,
    ):
        """
        Create a new tracker item in DynamoDB based on the document in landing bucket.
        During the creation of the tracker item, we calculate the doc_id based on the
        content of the document in landing bucket.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param landing_doc: :class:`aws_textract_pipeline.landing.LandingDocument` object.
        """
        doc_id = get_doc_md5(
            bsm=bsm,
            s3path=S3Path(landing_doc.s3uri),
            doc_type=landing_doc.doc_type,
        )
        task = cls.make(
            task_id=doc_id,
            data=Data(
                landing_uri=landing_doc.s3uri,
                doc_type=landing_doc.doc_type,
                features=landing_doc.features,
            ).to_dict(),
        )
        if save:
            task.save()
        return task


def make_tracker_config(
    pending_status: int,
    in_progress_status: int,
    failed_status: int,
    succeeded_status: int,
    ignored_status: int,
) -> st.TrackerConfig:
    return st.TrackerConfig.make(
        use_case_id=USE_CASE_ID,
        pending_status=pending_status,
        in_progress_status=in_progress_status,
        failed_status=failed_status,
        succeeded_status=succeeded_status,
        ignored_status=ignored_status,
        n_pending_shard=5,
        n_in_progress_shard=5,
        n_failed_shard=5,
        n_succeeded_shard=10,
        n_ignored_shard=5,
        status_zero_pad=6,
        status_shard_zero_pad=4,
        max_retry=3,
        lock_expire_seconds=600,
    )
