# -*- coding: utf-8 -*-

import typing as T
import json

import pynamodb_mate.api as pm
from boto_session_manager import BotoSesManager
import aws_textract.api as aws_textract

from ..logger import logger
from ..landing import MetadataKeyEnum
from ..workspace import Workspace
from ..types import api as types
from ..constants import ROOT_FRAG_ID

from .status import StatusEnum
from .orm import make_tracker_config, BaseTask, TextractOutputToTextAndJsonResult


class TextractTextDetectionOutputToTextAndJsonTask(BaseTask):
    config = make_tracker_config(
        pending_status=StatusEnum.s0400_textract_text_detection_output_to_text_and_json_pending.value,
        in_progress_status=StatusEnum.s0420_textract_text_detection_output_to_text_and_json_in_progress.value,
        failed_status=StatusEnum.s0440_textract_text_detection_output_to_text_and_json_failed.value,
        succeeded_status=StatusEnum.s0460_textract_text_detection_output_to_text_and_json_succeeded.value,
        ignored_status=StatusEnum.s0480_textract_text_detection_output_to_text_and_json_ignored.value,
        more_pending_status=[
            StatusEnum.s0360_fragment_to_textract_text_detection_output_succeeded.value,
        ],
    )

    def _run(
        self,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        job_id: str,
        frag_id: str,
        base_metadata: dict,
    ) -> T.Tuple[str, dict]:  # pragma: no cover
        """
        This is a utility function to simplify the code.
        """
        base_metadata[MetadataKeyEnum.frag_id.value] = frag_id

        # Get merged data
        res = aws_textract.better_boto.get_document_text_detection(
            textract_client=bsm.textract_client,
            job_id=job_id,
            all_pages=True,
        )
        if "ResponseMetadata" in res:
            del res["ResponseMetadata"]

        # Text
        text = aws_textract.res.blocks_to_text(res.get("Blocks", []))
        s3path_text = workspace.get_textract_text_detection_text_s3path(
            doc_id=self.doc_id,
            frag_id=frag_id,
        )
        logger.info(
            f"create text view for doc_id = {self.doc_id}, frag_id = {frag_id} at: {s3path_text.uri}"
        )
        s3path_text.write_text(
            text,
            bsm=bsm,
            metadata=base_metadata,
            content_type=types.S3ContentTypeEnum.text_plain.value,
        )

        # Json
        s3path_json = workspace.get_textract_text_detection_json_s3path(
            doc_id=self.doc_id,
            frag_id=frag_id,
        )
        logger.info(
            f"create JSON view for doc_id = {self.doc_id}, frag_id = {frag_id} at: {s3path_json.uri}"
        )
        s3path_json.write_text(
            json.dumps(res),
            bsm=bsm,
            metadata=base_metadata,
            content_type=types.S3ContentTypeEnum.json.value,
        )
        return text, res

    @classmethod
    def run(
        cls,
        doc_id: str,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        detailed_error: bool = False,
        debug: bool = False,
    ):
        """
        Convert textract text detection output to human / machine friendly
        text and json.

        :param doc_id: document id.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param detailed_error:
        :param debug:
        """
        exec_ctx: pm.patterns.status_tracker.ExecutionContext
        with cls.start(
            task_id=doc_id,
            detailed_error=detailed_error,
            debug=debug,
        ) as exec_ctx:
            task: "TextractTextDetectionOutputToTextAndJsonTask" = exec_ctx.task

            data_obj = task.data_obj
            fragment_to_textract_text_detection_output_result = (
                data_obj.fragment_to_textract_text_detection_output_result
            )
            s3path_raw = workspace.get_raw_s3path(doc_id=task.doc_id)
            metadata = s3path_raw.metadata.copy()
            textract_output_to_text_and_json_result = (
                TextractOutputToTextAndJsonResult()
            )
            if (
                fragment_to_textract_text_detection_output_result.is_single_textract_api_call
            ):
                frag_id = ROOT_FRAG_ID
                job_id = fragment_to_textract_text_detection_output_result.job_id
                text, res = task._run(
                    bsm=bsm,
                    workspace=workspace,
                    job_id=job_id,
                    frag_id=frag_id,
                    base_metadata=metadata,
                )
                textract_output_to_text_and_json_result.text_list.append(text)
                textract_output_to_text_and_json_result.json_list.append(res)
            else:
                for fragment, job_id in zip(
                    data_obj.fragments,
                    fragment_to_textract_text_detection_output_result.job_id_list,
                ):
                    frag_id = fragment.id
                    text, res = task._run(
                        bsm=bsm,
                        workspace=workspace,
                        job_id=job_id,
                        frag_id=frag_id,
                        base_metadata=metadata,
                    )
                    textract_output_to_text_and_json_result.text_list.append(text)
                    textract_output_to_text_and_json_result.json_list.append(res)
        return textract_output_to_text_and_json_result
