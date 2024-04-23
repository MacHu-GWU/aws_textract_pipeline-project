# -*- coding: utf-8 -*-

"""
See:

- :class:`ComponentToTextractOutputResult`
- :class:`Component`
- :class:`Data`
- :class:`Errors`
- :class:`StatusEnum`
- :class:`BaseStatusAndUpdateTimeIndex`
- :class:`BaseTracker`
"""

import typing as T
import dataclasses

from pathlib_mate import Path, T_PATH_ARG
import pynamodb_mate as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

import aws_textract.api as aws_textract
from .vendor.better_enum import BetterStrEnum
from .vendor.better_dataclasses import DataClass

from .logger import logger
from .doc_type import DocTypeEnum, S3ContentTypeEnum
from .landing import MetadataKeyEnum, LandingDocument, get_doc_md5
from .segment import segment_pdf
from .workspace import Workspace


dir_tmp = Path(Path("/").resolve().anchor) / "tmp"

_root_ = "_root_"


import json


# ------------------------------------------------------------------------------
# DynamoDB ORM Model
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class Component(DataClass):
    """
    Metadata for each component.
    """

    id: str = dataclasses.field()


@dataclasses.dataclass
class ComponentToTextractOutputResult(DataClass):
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
    components: T.List[Component] = Component.list_of_nested_field(default_factory=list)
    component_to_textract_output_result: T.Optional[ComponentToTextractOutputResult] = ComponentToTextractOutputResult.nested_field(default=None)
    # fmt: on

    @property
    def n_components(self):
        """
        Number of components.
        """
        return len(self.components)


@dataclasses.dataclass
class Errors(DataClass):
    """
    Runtime error information for debug.

    :param error: error message.
    :param traceback: Python traceback information.
    """

    error: T.Optional[str] = dataclasses.field(default=None)
    traceback: T.Optional[str] = dataclasses.field(default=None)


class StatusEnum(pm.patterns.status_tracker.BaseStatusEnum):
    """
    Textract pipeline status enum.
    """

    # landing to raw
    s01000_landing_to_raw_pending = 1000
    s01020_landing_to_raw_in_progress = 1020
    s01040_landing_to_raw_failed = 1040
    s01060_landing_to_raw_succeeded = 1060
    s01080_landing_to_raw_ignored = 1080

    # raw to component
    s02000_raw_to_component_pending = 2000
    s02020_raw_to_component_in_progress = 2020
    s02040_raw_to_component_failed = 2040
    s02060_raw_to_component_succeeded = 2060
    s02080_raw_to_component_ignored = 2080

    # component to textract_output
    s03000_component_to_textract_output_pending = 3000
    s03020_component_to_textract_output_in_progress = 3020
    s03040_component_to_textract_output_failed = 3040
    s03060_component_to_textract_output_succeeded = 3060
    s03080_component_to_textract_output_ignored = 3080

    # textract_output to text and json
    s05000_textract_output_to_text_and_json_pending = 5000
    s05020_textract_output_to_text_and_json_in_progress = 5020
    s05040_textract_output_to_text_and_json_failed = 5040
    s05060_textract_output_to_text_and_json_succeeded = 5060
    s05080_textract_output_to_text_and_json_ignored = 5080

    # textract_output to text and json
    s07000_json_to_extracted_data_pending = 7000
    s07020_json_to_extracted_data_in_progress = 7020
    s07040_json_to_extracted_data_failed = 7040
    s07060_json_to_extracted_data_succeeded = 7060
    s07080_json_to_extracted_data_ignored = 7080

    # textract_output to text and json
    s08000_extracted_data_to_hil_output_pending = 8000
    s08020_extracted_data_to_hil_output_in_progress = 8020
    s08040_extracted_data_to_hil_output_failed = 8040
    s08060_extracted_data_to_hil_output_succeeded = 8060
    s08080_extracted_data_to_hil_output_ignored = 8080

    # textract_output to text and json
    s09000_hil_output_to_hil_post_process_pending = 9000
    s09020_hil_output_to_hil_post_process_in_progress = 9020
    s09040_hil_output_to_hil_post_process_failed = 9040
    s09060_hil_output_to_hil_post_process_succeeded = 9060
    s09080_hil_output_to_hil_post_process_ignored = 9080


class StepEnum(BetterStrEnum):
    do_nothing = "do_nothing"
    landing_to_raw = "landing_to_raw"
    raw_to_component = "raw_to_component"
    component_to_textract_output = "component_to_textract_output"
    textract_output_to_text_and_json = "textract_output_to_text_and_json"


@dataclasses.dataclass
class MoveToNextStepResult(DataClass):
    # fmt: off
    step: str = dataclasses.field()
    components: T.List[Component] = dataclasses.field(default_factory=list)
    component_to_textract_output_result: T.Optional[ComponentToTextractOutputResult] = dataclasses.field(default=None)
    textract_output_to_text_and_json_result: T.Optional[TextractOutputToTextAndJsonResult] = dataclasses.field(default=None)
    # fmt: on


class BaseStatusAndUpdateTimeIndex(
    pm.patterns.status_tracker.StatusAndUpdateTimeIndex,
):
    """
    Status Tracker GSI index, to allow lookup by status.
    """

    pass


class BaseTracker(
    pm.patterns.status_tracker.BaseStatusTracker,
):
    """
    Status tracker DynamoDB table ORM model. It is the main class of
    the ``aws_textract_pipeline`` library. All the ETL logics are implemented
    as its methods.

    Main ETL Logics:

    - :meth:`new_from_landing_doc`
    - :meth:`landing_to_raw`
    - :meth:`raw_to_component`
    - :meth:`component_to_textract_output`
    - :meth:`textract_output_to_text_and_json`

    Status tracking management:

    - :meth:`start_landing_to_raw`
    - :meth:`start_raw_to_component`
    - :meth:`start_component_to_textract_output`
    - :meth:`start_textract_output_to_text_and_json`
    - :meth:`start_json_to_extracted_data`
    - :meth:`start_extracted_data_to_hil_output`
    - :meth:`start_hil_output_to_hil_post_process`

    Usage example:

    .. code-block:: python

        import aws_textract_pipeline.api as aws_textract_pipeline

        class StatusAndUpdateTimeIndex(aws_textract_pipeline.BaseStatusAndUpdateTimeIndex):
            pass

        class Tracker(aws_textract_pipeline.BaseTracker):
            class Meta:
                table_name = "aws_textract_pipeline-tracker"
                region = bsm.aws_region
                billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

            status_and_update_time_index = StatusAndUpdateTimeIndex()

            # (optional) override default settings
            JOB_ID = "your_own_project_name"
            STATUS_ZERO_PAD = 6 # status code will be padded to 6 digits
            MAX_RETRY = 3 # for each task, you can retry 3 times
            LOCK_EXPIRE_SECONDS = 900 # lock will expire in 900 seconds
            DEFAULT_STATUS = StatusEnum.s01000_landing_to_raw_pending.value # default status at very beginning of this pipeline
            STATUS_ENUM = StatusEnum # you can extend the status enum if you want to add more status code and more ETL steps

    You can find a more detailed example at https://github.com/MacHu-GWU/aws_textract_pipeline-project/blob/main/debug/test_pipeline.py

    This implementation is based on the
    `pynamodb_mate Status Tracker <https://github.com/MacHu-GWU/pynamodb_mate-project/blob/master/examples/patterns/status-tracker.ipynb>`_
    framework.
    """

    JOB_ID = "tt_pipe"
    STATUS_ZERO_PAD = 6
    MAX_RETRY = 3
    LOCK_EXPIRE_SECONDS = 900
    DEFAULT_STATUS = StatusEnum.s01000_landing_to_raw_pending.value
    STATUS_ENUM = StatusEnum

    @property
    def doc_id(self) -> str:
        return self.task_id

    @property
    def data_obj(self) -> Data:
        return Data.from_dict(self.data)

    @property
    def errors_obj(self) -> Errors:
        return Errors.from_dict(self.errors)

    def start_landing_to_raw(
        self,
        debug: bool = False,
    ):
        """
        Transition from "landing" to "textract".
        """
        return self.start(
            in_process_status=StatusEnum.s01000_landing_to_raw_pending.value,
            failed_status=StatusEnum.s01040_landing_to_raw_failed.value,
            success_status=StatusEnum.s01060_landing_to_raw_succeeded.value,
            ignore_status=StatusEnum.s01080_landing_to_raw_ignored.value,
            debug=debug,
        )

    def start_raw_to_component(
        self,
        debug: bool = False,
    ):
        """
        Transition from "raw" to "component".
        """
        return self.start(
            in_process_status=StatusEnum.s02020_raw_to_component_in_progress.value,
            failed_status=StatusEnum.s02040_raw_to_component_failed.value,
            success_status=StatusEnum.s02060_raw_to_component_succeeded.value,
            ignore_status=StatusEnum.s02080_raw_to_component_ignored.value,
            debug=debug,
        )

    def start_component_to_textract_output(
        self,
        debug: bool = False,
    ):
        """
        Transition from "component" to "textract output".
        """
        return self.start(
            in_process_status=StatusEnum.s03020_component_to_textract_output_in_progress.value,
            failed_status=StatusEnum.s03040_component_to_textract_output_failed.value,
            success_status=StatusEnum.s03060_component_to_textract_output_succeeded.value,
            ignore_status=StatusEnum.s03080_component_to_textract_output_ignored.value,
            debug=debug,
        )

    def start_textract_output_to_text_and_json(
        self,
        debug: bool = False,
    ):
        """
        Transition from "textract output" to "text and json".
        """
        return self.start(
            in_process_status=StatusEnum.s05020_textract_output_to_text_and_json_in_progress.value,
            failed_status=StatusEnum.s05040_textract_output_to_text_and_json_failed.value,
            success_status=StatusEnum.s05060_textract_output_to_text_and_json_succeeded.value,
            ignore_status=StatusEnum.s05080_textract_output_to_text_and_json_ignored.value,
            debug=debug,
        )

    def start_json_to_extracted_data(
        self,
        debug: bool = False,
    ):
        """
        Transition from "json" to "extracted data".
        """
        return self.start(
            in_process_status=StatusEnum.s07020_json_to_extracted_data_in_progress.value,
            failed_status=StatusEnum.s07040_json_to_extracted_data_failed.value,
            success_status=StatusEnum.s07060_json_to_extracted_data_succeeded.value,
            ignore_status=StatusEnum.s07080_json_to_extracted_data_ignored.value,
            debug=debug,
        )

    def start_extracted_data_to_hil_output(
        self,
        debug: bool = False,
    ):
        """
        Transition from "extracted data" to "hil output".
        """
        return self.start(
            in_process_status=StatusEnum.s08020_extracted_data_to_hil_output_in_progress.value,
            failed_status=StatusEnum.s08040_extracted_data_to_hil_output_failed.value,
            success_status=StatusEnum.s08060_extracted_data_to_hil_output_succeeded.value,
            ignore_status=StatusEnum.s08080_extracted_data_to_hil_output_ignored.value,
            debug=debug,
        )

    def start_hil_output_to_hil_post_process(
        self,
        debug: bool = False,
    ):
        """
        Transition from "hil output" to "hil post process".
        """
        return self.start(
            in_process_status=StatusEnum.s09020_hil_output_to_hil_post_process_in_progress.value,
            failed_status=StatusEnum.s09040_hil_output_to_hil_post_process_failed.value,
            success_status=StatusEnum.s09060_hil_output_to_hil_post_process_succeeded.value,
            ignore_status=StatusEnum.s09080_hil_output_to_hil_post_process_ignored.value,
            debug=debug,
        )

    def check_status_range(
        self,
        valid_status: T.List[int],
    ):
        """
        Check the current status before executing ETL logics. Raise error
        if the current status doesn't meet expectation. For example, in order to
        segment the raw document into components, the raw document has to be
        successfully copied from landing. If the status code is not the following,
        we should not execute raw to components logic:

        - ``StatusEnum.s01060_landing_to_raw_succeeded``: we are ready .
        - ``StatusEnum.s02000_raw_to_component_pending``: we are ready.
        - ``StatusEnum.s02040_raw_to_component_failed``: we have tried, but failed,
            we are ready for the next try.

        :param valid_status: list of valid status.
        """
        if self.status not in valid_status:
            msg = "{} is invalid for this! " "valid status are: {}".format(
                self.status,
                [StatusEnum.value_to_name(self.status) for status in valid_status],
            )
            raise ValueError(msg)

    @classmethod
    def new_from_landing_doc(
        cls,
        bsm: "BotoSesManager",
        landing_doc: LandingDocument,
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
        return cls.new(
            task_id=doc_id,
            data=Data(
                landing_uri=landing_doc.s3uri,
                doc_type=landing_doc.doc_type,
            ).to_dict(),
            save=True,
        )

    @logger.start_and_end(msg="Landing to Raw")
    def _landing_to_raw(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        debug: bool = False,
    ):
        """
        Copy document from landing to raw.
        """
        self.check_status_range(
            valid_status=[
                self.STATUS_ENUM.s01000_landing_to_raw_pending.value,
                self.STATUS_ENUM.s01040_landing_to_raw_failed.value,
            ]
        )
        with self.start_landing_to_raw(debug=debug):
            s3path_landing = S3Path(self.data_obj.landing_uri)
            s3path_raw = workspace.get_raw_s3path(doc_id=self.doc_id)
            metadata = s3path_landing.metadata.copy()
            metadata[MetadataKeyEnum.doc_id.value] = self.doc_id
            logger.info(f"Copy from {s3path_landing.uri} to {s3path_raw.uri}")
            s3path_landing.copy_to(
                dst=s3path_raw,
                overwrite=True,
                metadata=metadata,
                bsm=bsm,
            )

    def landing_to_raw(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        debug: bool = False,
    ):
        """
        Wrapper of the :meth:`BaseTracker._landing_to_raw` method.
        """
        with logger.disabled(disable=not debug):
            return self._landing_to_raw(bsm=bsm, workspace=workspace, debug=debug)

    @logger.start_and_end(msg="Raw to Component")
    def _raw_to_component(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        tmp_dir: T_PATH_ARG = dir_tmp,
        clear_tmp_dir: bool = False,
        debug: bool = False,
    ) -> T.List[Component]:
        """
        Segment raw document into components.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param tmp_dir: temporary directory on local File system to store
            the intermediate files
        :param clear_tmp_dir: whether to clear the temporary directory after the
            operation.
        :param debug:
        """
        self.check_status_range(
            valid_status=[
                self.STATUS_ENUM.s01060_landing_to_raw_succeeded.value,
                self.STATUS_ENUM.s02000_raw_to_component_pending.value,
                self.STATUS_ENUM.s02040_raw_to_component_failed.value,
            ]
        )

        tmp_dir = Path(tmp_dir)
        dir_root = tmp_dir / self.doc_id
        dir_root.mkdir(parents=True, exist_ok=True)
        s3path_raw = workspace.get_raw_s3path(doc_id=self.doc_id)
        metadata = s3path_raw.metadata.copy()

        components = list()
        with self.start_raw_to_component(debug=debug):
            # ------------------------------------------------------------------
            # PDF
            # ------------------------------------------------------------------
            if self.data_obj.doc_type == DocTypeEnum.pdf.value:
                res = segment_pdf(s3path_raw.read_bytes())
                for ith, (pdf, pixmap) in enumerate(
                    zip(res.page_pdf_list, res.page_image_list),
                    start=1,
                ):
                    component_id = f"{ith:06d}"
                    path_page = dir_root / f"{component_id}.pdf"
                    path_image = dir_root / f"{component_id}.png"
                    s3path_component = workspace.get_component_s3path(
                        doc_id=self.doc_id, comp_id=component_id
                    )
                    s3path_image = workspace.get_image_s3path(
                        doc_id=self.doc_id, comp_id=component_id
                    )
                    metadata[MetadataKeyEnum.component_id.value] = component_id

                    logger.info(f"Create component: {s3path_component.uri}")
                    pdf.save(path_page.abspath)
                    s3path_component.write_bytes(
                        path_page.read_bytes(),
                        metadata=metadata,
                        content_type=S3ContentTypeEnum.pdf.value,
                        bsm=bsm,
                    )

                    logger.info(f"Create image: {s3path_image.uri}")
                    pixmap.save(path_image.abspath, output="png")
                    s3path_image.write_bytes(
                        path_image.read_bytes(),
                        metadata=metadata,
                        content_type=S3ContentTypeEnum.image_png.value,
                        bsm=bsm,
                    )
                    component = Component(id=component_id)
                    components.append(component)
                    if clear_tmp_dir:
                        path_page.unlink()
                        path_image.unlink()
                data_obj = self.data_obj
                data_obj.components = components
                self.set_data(data_obj.to_dict())
                return components
            else:
                raise NotImplementedError

    def raw_to_component(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        tmp_dir: T_PATH_ARG = dir_tmp,
        clear_tmp_dir: bool = True,
        debug: bool = False,
    ) -> T.List[Component]:
        """
        Wrapper of the :meth:`BaseTracker._raw_to_component` method.
        """
        with logger.disabled(disable=not debug):
            return self._raw_to_component(
                bsm=bsm,
                workspace=workspace,
                tmp_dir=tmp_dir,
                clear_tmp_dir=clear_tmp_dir,
                debug=debug,
            )

    def _component_to_textract_output_helper(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        doc_id: str,
        comp_id: str,
        feature_types: T.List[str],
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
    ):  # pragma: no cover
        s3path_component = workspace.get_component_s3path(
            doc_id=self.doc_id,
            comp_id=comp_id,
        )
        s3dir_textract_output = workspace.get_textract_output_s3dir(
            doc_id=doc_id,
            comp_id=comp_id,
        )
        document_location, output_config = (
            aws_textract.better_boto.preprocess_input_output_config(
                input_bucket=s3path_component.bucket,
                input_key=s3path_component.key,
                input_version=None,
                output_bucket=s3dir_textract_output.bucket,
                output_prefix=s3dir_textract_output.key,
            )
        )
        kwargs = dict(
            DocumentLocation=document_location,
            FeatureTypes=feature_types,
            OutputConfig=output_config,
            JobTag=doc_id,
        )
        if sns_topic_arn:
            kwargs["NotificationChannel"] = dict(
                SNSTopicArn=sns_topic_arn,
                RoleArn=role_arn,
            )
        logger.info(f"analyze {feature_types} for: {s3path_component.uri}")
        res = bsm.textract_client.start_document_analysis(**kwargs)
        job_id = res["JobId"]
        logger.info(f"JobId: {job_id}")
        return job_id

    @logger.start_and_end(msg="Component to Textract Output")
    def _component_to_textract_output(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        use_table_feature: bool = False,
        use_form_feature: bool = False,
        use_query_feature: bool = False,
        use_signature_feature: bool = False,
        use_layout_feature: bool = False,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
        debug: bool = False,
    ) -> ComponentToTextractOutputResult:  # pragma: no cover
        """
        See the :meth:`BaseTracker.component_to_textract_output` method for more details.
        """
        self.check_status_range(
            valid_status=[
                self.STATUS_ENUM.s02060_raw_to_component_succeeded.value,
                self.STATUS_ENUM.s03000_component_to_textract_output_pending.value,
                self.STATUS_ENUM.s03040_component_to_textract_output_failed.value,
            ]
        )

        # prepare textract API arguments
        feature_types = list()
        for flag, feature in [
            (use_table_feature, "TABLES"),
            (use_form_feature, "FORMS"),
            (use_query_feature, "QUERIES"),
            (use_signature_feature, "SIGNATURES"),
            (use_layout_feature, "LAYOUT"),
        ]:
            if flag:
                feature_types.append(feature)
        if len(feature_types) == 0:
            raise ValueError("At least one feature must be enabled.")

        with self.start_component_to_textract_output(debug=debug):
            doc_id = self.doc_id
            data_obj = self.data_obj
            s3path_raw = workspace.get_raw_s3path(doc_id=doc_id)
            # ------------------------------------------------------------------
            # PDF
            # ------------------------------------------------------------------
            if data_obj.doc_type == DocTypeEnum.pdf.value:
                # check if the document fit Amazon Textract Async API quota
                # if fit, then only make one API call for the whole document.
                if (
                    # s3path_raw.size <= 300_000_000
                    s3path_raw.size <= 1
                    and data_obj.n_components <= 3000
                ):
                    comp_id = _root_
                    job_id = self._component_to_textract_output_helper(
                        bsm=bsm,
                        workspace=workspace,
                        doc_id=doc_id,
                        comp_id=comp_id,
                        feature_types=feature_types,
                        sns_topic_arn=sns_topic_arn,
                        role_arn=role_arn,
                    )
                    component_to_textract_output_result = (
                        ComponentToTextractOutputResult(
                            is_single_textract_api_call=True,
                            job_id=job_id,
                            job_id_list=None,
                        )
                    )
                # if doesn't fit, then make multiple API calls for each component.
                else:
                    component_to_textract_output_result = (
                        ComponentToTextractOutputResult(
                            is_single_textract_api_call=False,
                            job_id=None,
                            job_id_list=[],
                        )
                    )
                    for comp in data_obj.components:
                        comp_id = comp.id
                        job_id = self._component_to_textract_output_helper(
                            bsm=bsm,
                            workspace=workspace,
                            doc_id=doc_id,
                            comp_id=comp_id,
                            feature_types=feature_types,
                            sns_topic_arn=sns_topic_arn,
                            role_arn=role_arn,
                        )
                        component_to_textract_output_result.job_id_list.append(job_id)
                data_obj.component_to_textract_output_result = (
                    component_to_textract_output_result
                )
                self.set_data(data_obj.to_dict())
                return component_to_textract_output_result
            else:
                raise NotImplementedError

    def component_to_textract_output(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        use_table_feature: bool = False,
        use_form_feature: bool = False,
        use_query_feature: bool = False,
        use_signature_feature: bool = False,
        use_layout_feature: bool = False,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
        debug: bool = False,
    ) -> ComponentToTextractOutputResult:  # pragma: no cover
        """
        Run textract analysis document API for each component.

        Wrapper of the :meth:`BaseTracker._component_to_textract_output` method.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param use_table_feature: at least one feature must be enabled.
        :param use_form_feature: at least one feature must be enabled.
        :param use_query_feature: at least one feature must be enabled.
        :param use_signature_feature: at least one feature must be enabled.
        :param use_layout_feature: at least one feature must be enabled.
        :param sns_topic_arn: AWS SNS topic arn if you want to send a notification
            when the job is done.
        :param role_arn: the role arn that allows Amazon Textract to publish to the
            SNS topic.
        :param debug:
        """
        with logger.disabled(disable=not debug):
            return self._component_to_textract_output(
                bsm=bsm,
                workspace=workspace,
                use_table_feature=use_table_feature,
                use_form_feature=use_form_feature,
                use_query_feature=use_query_feature,
                use_signature_feature=use_signature_feature,
                use_layout_feature=use_layout_feature,
                sns_topic_arn=sns_topic_arn,
                role_arn=role_arn,
                debug=debug,
            )

    def _textract_output_to_text_and_json_helper(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        job_id: str,
        comp_id: str,
        base_metadata: dict,
    ) -> T.Tuple[str, dict]:  # pragma: no cover
        """
        This is a utility function to simplify the code.
        """
        base_metadata[MetadataKeyEnum.component_id.value] = comp_id

        # Get merged data
        res = aws_textract.better_boto.get_document_analysis(
            textract_client=bsm.textract_client,
            job_id=job_id,
            all_pages=True,
        )
        if "ResponseMetadata" in res:
            del res["ResponseMetadata"]

        # Text
        text = aws_textract.res.blocks_to_text(res.get("Blocks", []))
        s3path_text = workspace.get_text_s3path(
            doc_id=self.doc_id,
            comp_id=comp_id,
        )
        logger.info(
            f"create text view for doc_id = {self.doc_id}, comp_id = {comp_id} at: {s3path_text.uri}"
        )
        s3path_text.write_text(
            text,
            bsm=bsm,
            metadata=base_metadata,
            content_type=S3ContentTypeEnum.text_plain.value,
        )

        # Json
        s3path_json = workspace.get_json_s3path(
            doc_id=self.doc_id,
            comp_id=comp_id,
        )
        logger.info(
            f"create JSON view for doc_id = {self.doc_id}, comp_id = {comp_id} at: {s3path_json.uri}"
        )
        s3path_json.write_text(
            json.dumps(res),
            bsm=bsm,
            metadata=base_metadata,
            content_type=S3ContentTypeEnum.json.value,
        )
        return text, res

    @logger.start_and_end(msg="Textract Output to Text and Json")
    def _textract_output_to_text_and_json(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        debug: bool = False,
    ) -> TextractOutputToTextAndJsonResult:  # pragma: no cover
        """
        See :meth:`BaseTracker.textract_output_to_text_and_json` for details.
        """
        self.check_status_range(
            valid_status=[
                self.STATUS_ENUM.s03060_component_to_textract_output_succeeded.value,
                self.STATUS_ENUM.s05000_textract_output_to_text_and_json_pending.value,
                self.STATUS_ENUM.s05040_textract_output_to_text_and_json_failed.value,
            ]
        )

        data_obj = self.data_obj
        component_to_textract_output_result = (
            data_obj.component_to_textract_output_result
        )
        s3path_raw = workspace.get_raw_s3path(doc_id=self.doc_id)
        metadata = s3path_raw.metadata.copy()
        textract_output_to_text_and_json_result = TextractOutputToTextAndJsonResult()
        with self.start_textract_output_to_text_and_json(debug=debug):
            if component_to_textract_output_result.is_single_textract_api_call:
                comp_id = _root_
                job_id = component_to_textract_output_result.job_id
                text, res = self._textract_output_to_text_and_json_helper(
                    bsm=bsm,
                    workspace=workspace,
                    job_id=job_id,
                    comp_id=comp_id,
                    base_metadata=metadata,
                )
                textract_output_to_text_and_json_result.text_list.append(text)
                textract_output_to_text_and_json_result.json_list.append(res)
            else:
                for comp, job_id in zip(
                    data_obj.components,
                    component_to_textract_output_result.job_id_list,
                ):
                    comp_id = comp.id
                    text, res = self._textract_output_to_text_and_json_helper(
                        bsm=bsm,
                        workspace=workspace,
                        job_id=job_id,
                        comp_id=comp_id,
                        base_metadata=metadata,
                    )
                    textract_output_to_text_and_json_result.text_list.append(text)
                    textract_output_to_text_and_json_result.json_list.append(res)
        return textract_output_to_text_and_json_result

    def textract_output_to_text_and_json(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        debug: bool = False,
    ) -> TextractOutputToTextAndJsonResult:  # pragma: no cover
        """
        Parse textract output data, and convert them into text and json view.

        Wrapper of the :meth:`BaseTracker._textract_output_to_text_and_json` method.

        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param debug:
        """
        with logger.disabled(disable=not debug):
            return self._textract_output_to_text_and_json(
                bsm=bsm,
                workspace=workspace,
                debug=debug,
            )

    def get_next_step(self) -> StepEnum:  # pragma: no cover
        """
        Identify the next step of the pipeline based on the current status.
        """
        if self.status in [
            self.STATUS_ENUM.s01000_landing_to_raw_pending.value,
            self.STATUS_ENUM.s01020_landing_to_raw_in_progress.value,
        ]:
            return StepEnum.landing_to_raw
        elif self.status == self.STATUS_ENUM.s01020_landing_to_raw_in_progress.value:
            if self.is_locked() is False:
                return StepEnum.landing_to_raw
        elif self.status in [
            self.STATUS_ENUM.s01060_landing_to_raw_succeeded.value,
            self.STATUS_ENUM.s02000_raw_to_component_pending.value,
            self.STATUS_ENUM.s02040_raw_to_component_failed.value,
        ]:
            return StepEnum.raw_to_component
        elif self.status == self.STATUS_ENUM.s02020_raw_to_component_in_progress.value:
            if self.is_locked() is False:
                return StepEnum.raw_to_component
        elif self.status in [
            self.STATUS_ENUM.s02060_raw_to_component_succeeded.value,
            self.STATUS_ENUM.s03000_component_to_textract_output_pending.value,
            self.STATUS_ENUM.s03040_component_to_textract_output_failed.value,
        ]:
            return StepEnum.component_to_textract_output
        elif (
            self.status
            == self.STATUS_ENUM.s03020_component_to_textract_output_in_progress.value
        ):
            if self.is_locked() is False:
                return StepEnum.component_to_textract_output
        elif self.status in [
            self.STATUS_ENUM.s03060_component_to_textract_output_succeeded.value,
            self.STATUS_ENUM.s05000_textract_output_to_text_and_json_pending.value,
            self.STATUS_ENUM.s05040_textract_output_to_text_and_json_failed.value,
        ]:
            return StepEnum.textract_output_to_text_and_json
        elif (
            self.status
            == self.STATUS_ENUM.s05020_textract_output_to_text_and_json_in_progress.value
        ):
            if self.is_locked() is False:
                return StepEnum.textract_output_to_text_and_json
        else:  # ignored status
            pass

    def move_to_next_stage(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        tmp_dir: T_PATH_ARG = dir_tmp,
        clear_tmp_dir: bool = True,
        use_table_feature: bool = False,
        use_form_feature: bool = False,
        use_query_feature: bool = False,
        use_signature_feature: bool = False,
        use_layout_feature: bool = False,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
        debug: bool = False,
    ):  # pragma: no cover
        """
        Move the document to the next step of the pipeline. Smartly execute
        one of the following step:

        - :meth:`landing_to_raw`
        - :meth:`raw_to_component`
        - :meth:`component_to_textract_output`
        - :meth:`textract_output_to_text_and_json`
        """
        next_step = self.get_next_step()
        if next_step is StepEnum.landing_to_raw:
            self.landing_to_raw(bsm=bsm, workspace=workspace, debug=debug)
            return MoveToNextStepResult(
                step=StepEnum.landing_to_raw.value,
            )
        elif next_step is StepEnum.raw_to_component:
            components = self.raw_to_component(
                bsm=bsm,
                workspace=workspace,
                tmp_dir=tmp_dir,
                clear_tmp_dir=clear_tmp_dir,
                debug=debug,
            )
            return MoveToNextStepResult(
                step=StepEnum.raw_to_component.value,
                components=components,
            )
        elif next_step is StepEnum.component_to_textract_output:
            component_to_textract_output_result = self.component_to_textract_output(
                bsm=bsm,
                workspace=workspace,
                use_table_feature=use_table_feature,
                use_form_feature=use_form_feature,
                use_query_feature=use_query_feature,
                use_signature_feature=use_signature_feature,
                use_layout_feature=use_layout_feature,
                sns_topic_arn=sns_topic_arn,
                role_arn=role_arn,
                debug=debug,
            )
            return MoveToNextStepResult(
                step=StepEnum.component_to_textract_output.value,
                component_to_textract_output_result=component_to_textract_output_result,
            )
        elif next_step is StepEnum.textract_output_to_text_and_json:
            textract_output_to_text_and_json_result = (
                self.textract_output_to_text_and_json(
                    bsm=bsm, workspace=workspace, debug=debug
                )
            )
            return MoveToNextStepResult(
                step=StepEnum.textract_output_to_text_and_json.value,
                textract_output_to_text_and_json_result=textract_output_to_text_and_json_result,
            )
        else:  # ignored status
            return MoveToNextStepResult(
                step=StepEnum.do_nothing.value,
            )
        # fmt: on
