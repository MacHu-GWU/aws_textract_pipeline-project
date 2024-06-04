# -*- coding: utf-8 -*-

import pynamodb_mate.api as pm

from ..vendor.better_enum import BetterStrEnum


class StepEnum(BetterStrEnum):
    # fmt: off
    do_nothing = "do_nothing"
    landing_to_raw = "landing_to_raw"
    raw_to_fragment = "raw_to_fragment"
    fragment_to_textract_text_detection_output = "fragment_to_textract_text_detection_output"
    textract_text_detection_output_to_text_and_json = "textract_text_detection_output_to_text_and_json"
    fragment_to_textract_document_analysis_output = "fragment_to_textract_document_analysis_output"
    textract_document_analysis_output_to_text_and_json = "textract_document_analysis_output_to_text_and_json"
    fragment_to_textract_expense_analysis_output = "fragment_to_textract_expense_analysis_output"
    textract_expense_analysis_output_to_text_and_json = "textract_expense_analysis_output_to_text_and_json"
    fragment_to_textract_lending_analysis_output = "fragment_to_textract_lending_analysis_output"
    textract_lending_analysis_output_to_text_and_json = "textract_lending_analysis_output_to_text_and_json"
    text_and_json_to_extracted_data_pending = "text_and_json_to_extracted_data_pending"
    extracted_data_to_hil_output_failed = "extracted_data_to_hil_output_failed"
    hil_output_to_hil_post_process_in_progress = "hil_output_to_hil_post_process_in_progress"
    # fmt: on


class StatusEnum(pm.patterns.status_tracker.BaseStatusEnum):
    """
    Textract pipeline status enum.
    """

    # landing to raw
    s0100_landing_to_raw_pending = 100
    s0120_landing_to_raw_in_progress = 120
    s0140_landing_to_raw_failed = 140
    s0160_landing_to_raw_succeeded = 160
    s0180_landing_to_raw_ignored = 180

    # raw to fragment
    s0200_raw_to_fragment_pending = 200
    s0220_raw_to_fragment_in_progress = 220
    s0240_raw_to_fragment_failed = 240
    s0260_raw_to_fragment_succeeded = 260
    s0280_raw_to_fragment_ignored = 280

    # fragment to textract_text_detection_output
    s0300_fragment_to_textract_text_detection_output_pending = 300
    s0320_fragment_to_textract_text_detection_output_in_progress = 320
    s0340_fragment_to_textract_text_detection_output_failed = 340
    s0360_fragment_to_textract_text_detection_output_succeeded = 360
    s0380_fragment_to_textract_text_detection_output_ignored = 380

    # textract_text_detection_output to text_and_json
    s0400_textract_text_detection_output_to_text_and_json_pending = 400
    s0420_textract_text_detection_output_to_text_and_json_in_progress = 420
    s0440_textract_text_detection_output_to_text_and_json_failed = 440
    s0460_textract_text_detection_output_to_text_and_json_succeeded = 460
    s0480_textract_text_detection_output_to_text_and_json_ignored = 480

    # fragment to textract_document_analysis_output
    s0500_fragment_to_textract_document_analysis_output_pending = 500
    s0520_fragment_to_textract_document_analysis_output_in_progress = 520
    s0540_fragment_to_textract_document_analysis_output_failed = 540
    s0560_fragment_to_textract_document_analysis_output_succeeded = 560
    s0580_fragment_to_textract_document_analysis_output_ignored = 580

    # textract_document_analysis_output to text_and_json
    s0600_textract_document_analysis_output_to_text_and_json_pending = 600
    s0620_textract_document_analysis_output_to_text_and_json_in_progress = 620
    s0640_textract_document_analysis_output_to_text_and_json_failed = 640
    s0660_textract_document_analysis_output_to_text_and_json_succeeded = 660
    s0680_textract_document_analysis_output_to_text_and_json_ignored = 680

    # fragment to textract_expense_analysis_output
    s0700_fragment_to_textract_expense_analysis_output_pending = 700
    s0720_fragment_to_textract_expense_analysis_output_in_progress = 720
    s0740_fragment_to_textract_expense_analysis_output_failed = 740
    s0760_fragment_to_textract_expense_analysis_output_succeeded = 760
    s0780_fragment_to_textract_expense_analysis_output_ignored = 780

    # textract_expense_analysis_output to text_and_json
    s0800_textract_expense_analysis_output_to_text_and_json_pending = 800
    s0820_textract_expense_analysis_output_to_text_and_json_in_progress = 820
    s0840_textract_expense_analysis_output_to_text_and_json_failed = 840
    s0860_textract_expense_analysis_output_to_text_and_json_succeeded = 860
    s0880_textract_expense_analysis_output_to_text_and_json_ignored = 880

    # fragment to textract_lending_analysis_output
    s0900_fragment_to_textract_lending_analysis_output_pending = 900
    s0920_fragment_to_textract_lending_analysis_output_in_progress = 920
    s0940_fragment_to_textract_lending_analysis_output_failed = 940
    s0960_fragment_to_textract_lending_analysis_output_succeeded = 960
    s0980_fragment_to_textract_lending_analysis_output_ignored = 980

    # textract_lending_analysis_output to text_and_json
    s1000_textract_lending_analysis_output_to_text_and_json_pending = 1000
    s1020_textract_lending_analysis_output_to_text_and_json_in_progress = 1020
    s1040_textract_lending_analysis_output_to_text_and_json_failed = 1040
    s1060_textract_lending_analysis_output_to_text_and_json_succeeded = 1060
    s1080_textract_lending_analysis_output_to_text_and_json_ignored = 1080

    # text_and_json to extracted_data
    s1100_text_and_json_to_extracted_data_pending = 1100
    s1120_text_and_json_to_extracted_data_in_progress = 1120
    s1140_text_and_json_to_extracted_data_failed = 1140
    s1160_text_and_json_to_extracted_data_succeeded = 1160
    s1180_text_and_json_to_extracted_data_ignored = 1180

    # extracted_data to human_in_the_loop_output
    s1200_extracted_data_to_hil_output_pending = 1200
    s1220_extracted_data_to_hil_output_in_progress = 1220
    s1240_extracted_data_to_hil_output_failed = 1240
    s1260_extracted_data_to_hil_output_succeeded = 1260
    s1280_extracted_data_to_hil_output_ignored = 1280

    # human_in_the_loop_output to human_in_the_loop_post_process
    s1300_hil_output_to_hil_post_process_pending = 1300
    s1320_hil_output_to_hil_post_process_in_progress = 1320
    s1340_hil_output_to_hil_post_process_failed = 1340
    s1360_hil_output_to_hil_post_process_succeeded = 1360
    s1380_hil_output_to_hil_post_process_ignored = 1380
