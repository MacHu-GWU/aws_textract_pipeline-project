# -*- coding: utf-8 -*-

from ..helpers import ImportErrorHandler

try:
    from . import import_error_handler_module1 as module1

    _module1 = module1
except ImportError as e:
    module1: "_module1" = ImportErrorHandler(
        path="aws_textract_pipeline.tests.import_error_handler_module1",
        dependencies=["module1_dependency1", "module1_dependency2"],
        error=e,
    )

try:
    from . import import_error_handler_module2 as module2

    _module2 = module2
except ImportError as e:
    module2: "_module2" = ImportErrorHandler(
        path="aws_textract_pipeline.tests.import_error_handler_module2",
        dependencies=["module1_dependency2", "module2_dependency2"],
        error=e,
    )
