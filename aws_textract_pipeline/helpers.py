# -*- coding: utf-8 -*-

import typing as T


class ImportErrorHandler:
    def __init__(
        self,
        path: str,
        dependencies: T.List[str],
        error: Exception,
    ):
        self.path = path
        self.error = error
        self.dependencies = dependencies

    def __getattr__(self, item):
        raise ImportError(
            f"failed to import {self.path!r}, "
            f"you may need to pip install the following dependencies: {self.dependencies}, "
            f"error: {self.error}"
        )
