# -*- coding: utf-8 -*-

import typing as T

import pynamodb_mate.api as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager
import aws_textract.api as aws_textract

from ..logger import logger
from ..workspace import Workspace
from ..types import api as types
from ..constants import ROOT_FRAG_ID

from .status import StatusEnum
from .orm import (
    FragmentToTextractTextDetectionOutputResult,
    make_tracker_config,
    BaseTask,
)


class FragmentToTextractTextDetectionOutputTask(BaseTask):
    config = make_tracker_config(
        pending_status=StatusEnum.s0300_fragment_to_textract_text_detection_output_pending.value,
        in_progress_status=StatusEnum.s0320_fragment_to_textract_text_detection_output_in_progress.value,
        failed_status=StatusEnum.s0340_fragment_to_textract_text_detection_output_failed.value,
        succeeded_status=StatusEnum.s0360_fragment_to_textract_text_detection_output_succeeded.value,
        ignored_status=StatusEnum.s0380_fragment_to_textract_text_detection_output_ignored.value,
    )

    def _run(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        s3path_fragment: S3Path,
        doc_id: str,
        frag_id: str,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
    ):  # pragma: no cover
        s3dir_textract_output = workspace.get_textract_text_detection_output_s3dir(
            doc_id=doc_id,
            frag_id=frag_id,
        )
        document_location, output_config = (
            aws_textract.better_boto.preprocess_input_output_config(
                input_bucket=s3path_fragment.bucket,
                input_key=s3path_fragment.key,
                input_version=None,
                output_bucket=s3dir_textract_output.bucket,
                output_prefix=s3dir_textract_output.key,
            )
        )
        kwargs = dict(
            DocumentLocation=document_location,
            OutputConfig=output_config,
            JobTag=doc_id,
        )
        if sns_topic_arn:
            kwargs["NotificationChannel"] = dict(
                SNSTopicArn=sns_topic_arn,
                RoleArn=role_arn,
            )
        logger.info(f"detect text for: {s3path_fragment.uri}")
        res = bsm.textract_client.start_document_text_detection(**kwargs)
        job_id = res["JobId"]
        logger.info(f"JobId: {job_id}")
        return job_id

    @classmethod
    def run(
        cls,
        doc_id: str,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        single_api_call: T.Optional[bool] = None,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
        detailed_error: bool = False,
        debug: bool = False,
    ) -> FragmentToTextractTextDetectionOutputResult:
        """
        Use textract start text detection API to extract text from the document.

        :param doc_id: document id.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param single_api_call: if None, the library will automatically decide
            whether to use single API call or multiple API calls based on the
            document size and number of fragments. If True, only one API call
            will be made for the whole document. If False, multiple API calls
            will be made for each fragment.
        :param sns_topic_arn: AWS SNS topic arn if you want to send a notification
            when the job is done.
        :param role_arn: the role arn that allows Amazon Textract to publish to the
            SNS topic.
        :param detailed_error:
        :param debug:
        """
        exec_ctx: pm.patterns.status_tracker.ExecutionContext
        with cls.start(
            task_id=doc_id,
            allowed_status=[
                StatusEnum.s0260_raw_to_fragment_succeeded.value,
                # other situation
                # StatusEnum.s0360_fragment_to_textract_text_detection_output_succeeded.value,
                # StatusEnum.s0460_textract_text_detection_output_to_text_and_json_succeeded.value,
                # StatusEnum.s0560_fragment_to_textract_document_analysis_output_succeeded.value,
                StatusEnum.s0660_textract_document_analysis_output_to_text_and_json_succeeded.value,
                # StatusEnum.s0760_fragment_to_textract_expense_analysis_output_succeeded.value,
                StatusEnum.s0860_textract_expense_analysis_output_to_text_and_json_succeeded.value,
                # StatusEnum.s0960_fragment_to_textract_lending_analysis_output_succeeded.value,
                StatusEnum.s1060_textract_lending_analysis_output_to_text_and_json_succeeded.value,
            ],
            detailed_error=detailed_error,
            debug=debug,
        ) as exec_ctx:
            task: "FragmentToTextractTextDetectionOutputTask" = exec_ctx.task
            data_obj = task.data_obj
            s3path_raw = workspace.get_raw_s3path(doc_id=doc_id)
            # ------------------------------------------------------------------
            # PDF
            # ------------------------------------------------------------------
            if data_obj.doc_type == types.DocTypeEnum.pdf.value:
                if single_api_call is None:
                    # check if the document fit Amazon Textract Async API quota
                    # if fit, then only make one API call for the whole document.
                    if s3path_raw.size <= 300_000_000 and data_obj.n_fragments <= 3000:
                        is_single_textract_api_call = True
                    else:
                        is_single_textract_api_call = False
                else:
                    is_single_textract_api_call = single_api_call

                if is_single_textract_api_call:
                    frag_id = ROOT_FRAG_ID
                    job_id = task._run(
                        bsm=bsm,
                        workspace=workspace,
                        s3path_fragment=s3path_raw,
                        doc_id=doc_id,
                        frag_id=frag_id,
                        sns_topic_arn=sns_topic_arn,
                        role_arn=role_arn,
                    )
                    fragment_to_textract_text_detection_output_result = (
                        FragmentToTextractTextDetectionOutputResult(
                            is_single_textract_api_call=True,
                            job_id=job_id,
                            job_id_list=None,
                        )
                    )
                # if doesn't fit, then make multiple API calls for each fragment.
                else:
                    fragment_to_textract_text_detection_output_result = (
                        FragmentToTextractTextDetectionOutputResult(
                            is_single_textract_api_call=False,
                            job_id=None,
                            job_id_list=[],
                        )
                    )
                    for fragment in data_obj.fragments:
                        frag_id = fragment.id
                        job_id = task._run(
                            bsm=bsm,
                            workspace=workspace,
                            s3path_fragment=workspace.get_fragment_s3path(
                                doc_id=doc_id,
                                frag_id=frag_id,
                            ),
                            doc_id=doc_id,
                            frag_id=frag_id,
                            sns_topic_arn=sns_topic_arn,
                            role_arn=role_arn,
                        )
                        fragment_to_textract_text_detection_output_result.job_id_list.append(
                            job_id
                        )
                data_obj.fragment_to_textract_text_detection_output_result = (
                    fragment_to_textract_text_detection_output_result
                )
                exec_ctx.set_data(data_obj.to_dict())
                return fragment_to_textract_text_detection_output_result
            else:
                raise NotImplementedError
