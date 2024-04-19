# -*- coding: utf-8 -*-

"""
todo: add docstring
"""

import typing as T
import dataclasses

import pynamodb_mate as pm

from .vendor.better_dataclasses import DataClass


@dataclasses.dataclass
class Component(DataClass):
    id: str = dataclasses.field()


@dataclasses.dataclass
class Data(DataClass):
    landing_uri: str = dataclasses.field()
    doc_type: str = dataclasses.field()
    components: T.List[Component] = Component.list_of_nested_field(default_factory=list)

    @property
    def n_components(self):
        return len(self.components)


@dataclasses.dataclass
class Errors(DataClass):
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
    Status tracker DynamoDB table ORM model.
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

    def start_landing_to_textract(
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
