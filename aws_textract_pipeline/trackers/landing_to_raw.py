# -*- coding: utf-8 -*-

import pynamodb_mate.api as pm
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

from ..logger import logger
from ..landing import MetadataKeyEnum
from ..workspace import Workspace

from .status import StatusEnum
from .orm import make_tracker_config, BaseTask


class LandingToRawTask(BaseTask):
    config = make_tracker_config(
        pending_status=StatusEnum.s0100_landing_to_raw_pending.value,
        in_progress_status=StatusEnum.s0120_landing_to_raw_in_progress.value,
        failed_status=StatusEnum.s0140_landing_to_raw_failed.value,
        succeeded_status=StatusEnum.s0160_landing_to_raw_succeeded.value,
        ignored_status=StatusEnum.s0180_landing_to_raw_ignored.value,
        more_pending_status=[],
    )

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
        Move document file from landing to raw zone.

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
            task: "LandingToRawTask" = exec_ctx.task
            s3path_landing = S3Path(task.data_obj.landing_uri)
            s3path_raw = workspace.get_raw_s3path(doc_id=task.doc_id)
            metadata = s3path_landing.metadata.copy()
            metadata[MetadataKeyEnum.doc_id.value] = task.doc_id
            logger.info(f"Copy from {s3path_landing.uri} to {s3path_raw.uri}")
            s3path_landing.copy_to(
                dst=s3path_raw,
                overwrite=True,
                metadata=metadata,
                bsm=bsm,
            )
