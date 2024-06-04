# -*- coding: utf-8 -*-

import typing as T

import dataclasses
import pynamodb_mate.api as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager
import aws_textract.api as aws_textract
from ..vendor.better_dataclasses import DataClass

from ..logger import logger
from ..workspace import Workspace
from ..types import api as types
from ..constants import ROOT_FRAG_ID

from .status import StatusEnum
from .orm import (
    FragmentToTextractDocumentAnalysisOutputResult,
    make_tracker_config,
    BaseTask,
)


class FragmentToTextractDocumentAnalysisOutputTask(BaseTask):
    config = make_tracker_config(
        pending_status=StatusEnum.s0500_fragment_to_textract_document_analysis_output_pending.value,
        in_progress_status=StatusEnum.s0520_fragment_to_textract_document_analysis_output_in_progress.value,
        failed_status=StatusEnum.s0540_fragment_to_textract_document_analysis_output_failed.value,
        succeeded_status=StatusEnum.s0560_fragment_to_textract_document_analysis_output_succeeded.value,
        ignored_status=StatusEnum.s0580_fragment_to_textract_document_analysis_output_ignored.value,
    )

    def _run(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        s3path_component: S3Path,
        doc_id: str,
        frag_id: str,
        feature_types: T.List[str],
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
    ):  # pragma: no cover
        s3dir_textract_output = workspace.get_textract_document_analysis_output_s3dir(
            doc_id=doc_id,
            frag_id=frag_id,
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

    @classmethod
    def run(
        cls,
        doc_id: str,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        single_api_call: T.Optional[bool] = None,
        use_table_feature: bool = False,
        use_form_feature: bool = False,
        use_query_feature: bool = False,
        use_signature_feature: bool = False,
        use_layout_feature: bool = False,
        sns_topic_arn: T.Optional[str] = None,
        role_arn: T.Optional[str] = None,
        detailed_error: bool = False,
        debug: bool = False,
    ) -> FragmentToTextractDocumentAnalysisOutputResult:
        """
        Use textract start document analysis API to analyze the document.

        :param doc_id: document id.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param detailed_error:
        :param debug:
        """
        # prepare textract API arguments
        # for feature types, if user manually specified the feature types
        # in this method, then use it. Otherwise, use the feature types
        # from the landing document S3 object metadata
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

        exec_ctx: pm.patterns.status_tracker.ExecutionContext
        with cls.start(
            task_id=doc_id,
            allowed_status=[
                StatusEnum.s0260_raw_to_fragment_succeeded.value,
                StatusEnum.s0360_fragment_to_textract_text_detection_output_succeeded.value,
                # StatusEnum.s0560_fragment_to_textract_document_analysis_output_succeeded.value,
                StatusEnum.s0760_fragment_to_textract_expense_analysis_output_succeeded.value,
                StatusEnum.s0960_fragment_to_textract_lending_analysis_output_succeeded.value,
            ],
            detailed_error=detailed_error,
            debug=debug,
        ) as exec_ctx:
            task: "FragmentToTextractDocumentAnalysisOutputTask" = exec_ctx.task
            data_obj = task.data_obj
            if len(feature_types) == 0:
                if data_obj.features:
                    feature_types = data_obj.features
                else:
                    raise ValueError(
                        "Cannot find textract features specification in S3 object metadata!"
                    )

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
                        s3path_component=s3path_raw,
                        doc_id=doc_id,
                        frag_id=frag_id,
                        feature_types=feature_types,
                        sns_topic_arn=sns_topic_arn,
                        role_arn=role_arn,
                    )
                    fragment_to_textract_document_analysis_output_result = (
                        FragmentToTextractDocumentAnalysisOutputResult(
                            is_single_textract_api_call=True,
                            job_id=job_id,
                            job_id_list=None,
                        )
                    )
                # if doesn't fit, then make multiple API calls for each component.
                else:
                    fragment_to_textract_document_analysis_output_result = (
                        FragmentToTextractDocumentAnalysisOutputResult(
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
                            s3path_component=workspace.get_fragment_s3path(
                                doc_id=doc_id,
                                frag_id=frag_id,
                            ),
                            doc_id=doc_id,
                            frag_id=frag_id,
                            feature_types=feature_types,
                            sns_topic_arn=sns_topic_arn,
                            role_arn=role_arn,
                        )
                        fragment_to_textract_document_analysis_output_result.job_id_list.append(
                            job_id
                        )
                data_obj.fragment_to_textract_document_analysis_output_result = (
                    fragment_to_textract_document_analysis_output_result
                )
                exec_ctx.set_data(data_obj.to_dict())
                return fragment_to_textract_document_analysis_output_result
            else:
                raise NotImplementedError
