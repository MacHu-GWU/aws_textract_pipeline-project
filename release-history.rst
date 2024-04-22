.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.3.1 (tbd)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add the following public API:
    - ``aws_textract_pipeline.api.TextractOutputToTextAndJsonResult``
    - ``aws_textract_pipeline.api.StepEnum``
    - ``aws_textract_pipeline.api.MoveToNextStepResult``
    - ``aws_textract_pipeline.api.BaseTracker.move_to_next_step_result``


0.2.1 (2024-04-21)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- Add the following public API:
    - ``aws_textract_pipeline.api.ext_to_doc_type_mapper``
    - ``aws_textract_pipeline.api.doc_type_to_content_type_mapper``
    - ``aws_textract_pipeline.api.get_doc_md5``
    - ``aws_textract_pipeline.api.SegmentPdfResult``
    - ``aws_textract_pipeline.api.segment_pdf``
    - ``aws_textract_pipeline.api.ComponentToTextractOutputResult``
    - ``aws_textract_pipeline.api.Component``
    - ``aws_textract_pipeline.api.Data``


0.1.1 (2024-04-19)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- First release
- Add the following public API:
    - ``aws_textract_pipeline.api.DocTypeEnum``
    - ``aws_textract_pipeline.api.S3ContentTypeEnum``
    - ``aws_textract_pipeline.api.Workspace``
    - ``aws_textract_pipeline.api.MetadataKeyEnum``
    - ``aws_textract_pipeline.api.LandingDocument``
    - ``aws_textract_pipeline.api.get_md5_of_bytes``
    - ``aws_textract_pipeline.api.get_tar_file_md5``
    - ``aws_textract_pipeline.api.BaseStatusAndUpdateTimeIndex``
    - ``aws_textract_pipeline.api.BaseTracker``

