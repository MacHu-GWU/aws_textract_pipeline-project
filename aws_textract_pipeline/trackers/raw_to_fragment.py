# -*- coding: utf-8 -*-

from pathlib_mate import Path, T_PATH_ARG
import pynamodb_mate.api as pm
from boto_session_manager import BotoSesManager

from ..logger import logger
from ..landing import MetadataKeyEnum
from ..workspace import Workspace
from ..types import api as types
from ..fragments import api as fragments

from .status import StatusEnum
from .orm import Fragment, make_tracker_config, BaseTask

dir_tmp = Path(Path("/").resolve().anchor) / "tmp"


class RawToFragmentTask(BaseTask):
    config = make_tracker_config(
        pending_status=StatusEnum.s0200_raw_to_fragment_pending.value,
        in_progress_status=StatusEnum.s0220_raw_to_fragment_in_progress.value,
        failed_status=StatusEnum.s0240_raw_to_fragment_failed.value,
        succeeded_status=StatusEnum.s0260_raw_to_fragment_succeeded.value,
        ignored_status=StatusEnum.s0280_raw_to_fragment_ignored.value,
    )

    @classmethod
    def run(
        cls,
        doc_id: str,
        bsm: "BotoSesManager",
        workspace: "Workspace",
        tmp_dir: T_PATH_ARG = dir_tmp,
        clear_tmp_dir: bool = False,
        detailed_error: bool = False,
        debug: bool = False,
    ):
        """
        Divide raw document into fragments.

        :param doc_id: document id.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.
        :param workspace: :class:`aws_textract_pipeline.workspace.Workspace` object.
        :param tmp_dir: temporary directory on local File system to store
            the intermediate files
        :param clear_tmp_dir: whether to clear the temporary directory after the
            operation.
        :param detailed_error:
        :param debug:
        """
        exec_ctx: pm.patterns.status_tracker.ExecutionContext
        with cls.start(
            task_id=doc_id,
            allowed_status=[
                StatusEnum.s0160_landing_to_raw_succeeded.value,
            ],
            detailed_error=detailed_error,
            debug=debug,
        ) as exec_ctx:
            task: "RawToFragmentTask" = exec_ctx.task
            tmp_dir = Path(tmp_dir)
            dir_root = tmp_dir / task.doc_id
            dir_root.mkdir(parents=True, exist_ok=True)
            s3path_raw = workspace.get_raw_s3path(doc_id=task.doc_id)
            metadata = s3path_raw.metadata.copy()

            fragment_list = list()
            # ------------------------------------------------------------------
            # PDF
            # ------------------------------------------------------------------
            if task.data_obj.doc_type == types.DocTypeEnum.pdf.value:
                res = fragments.pdf.divide_pdf_into_pages(
                    pdf_content=s3path_raw.read_bytes(),
                    extract_image=True,
                    dpi=200,
                )
                for ith, (pdf, pixmap) in enumerate(
                    zip(res.page_pdf_list, res.page_image_list),
                    start=1,
                ):
                    frag_id = f"{ith:06d}"
                    path_page = dir_root / f"{frag_id}.pdf"
                    path_image = dir_root / f"{frag_id}.png"
                    s3path_component = workspace.get_fragment_s3path(
                        doc_id=task.doc_id, frag_id=frag_id
                    )
                    s3path_image = workspace.get_image_s3path(
                        doc_id=task.doc_id, frag_id=frag_id
                    )
                    metadata[MetadataKeyEnum.frag_id.value] = frag_id

                    logger.info(f"Create component: {s3path_component.uri}")
                    pdf.save(path_page.abspath)
                    s3path_component.write_bytes(
                        path_page.read_bytes(),
                        metadata=metadata,
                        content_type=types.S3ContentTypeEnum.pdf.value,
                        bsm=bsm,
                    )

                    logger.info(f"Create image: {s3path_image.uri}")
                    pixmap.save(path_image.abspath, output="png")
                    s3path_image.write_bytes(
                        path_image.read_bytes(),
                        metadata=metadata,
                        content_type=types.S3ContentTypeEnum.image_png.value,
                        bsm=bsm,
                    )
                    fragment = Fragment(id=frag_id)
                    fragment_list.append(fragment)
                    if clear_tmp_dir:
                        path_page.unlink()
                        path_image.unlink()
                data_obj = task.data_obj
                data_obj.fragments = fragment_list
                exec_ctx.set_data(data_obj.to_dict())
                return fragment_list
            else:
                raise NotImplementedError
