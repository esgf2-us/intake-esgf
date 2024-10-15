"""Exceptions for intake-esgf."""

from pathlib import Path

from globus_sdk import GlobusHTTPResponse


class IntakeESGFException(Exception):
    """Exceptions from the intake-esgf package."""


class NoSearchResults(IntakeESGFException):
    """Search returned no results."""


class LocalCacheNotWritable(IntakeESGFException):
    """You do not have permission to write in the cache directories."""

    def __init__(self, paths: list[Path]):
        self.paths = paths

    def __str__(self):
        return f"You do not have write permission in the cache directories specified: {self.paths}"


class ProjectNotSupported(IntakeESGFException):
    """You searched for a project that we do not yet support."""

    def __init__(self, project: str):
        self.project = project

    def __str__(self):
        return f"The '{self.project}' project is not yet supported by intake-esgf"


class MissingFileInformation(IntakeESGFException):
    """There was incomplete file access information"""

    def __init__(self, problem_keys: list[str]):
        self.problem_keys = problem_keys

    def __str__(self):
        return f"We were unable to find file information for these keys: {self.problem_keys}. Your access options could affect the possibilties."


class DatasetInitError(IntakeESGFException):
    """There was a problem initializing datasets."""

    def __init__(self, problem_keys: list[str]):
        self.problem_keys = problem_keys

    def __str__(self):
        return (
            f"xarray threw an exception while loading these keys: {self.problem_keys}"
        )


class GlobusTransferError(IntakeESGFException):
    """The globus task return a non-successful status."""

    def __init__(self, task_doc: GlobusHTTPResponse):
        self.task_doc = task_doc

    def __str__(self):
        return f"The Globus Transfer task is no longer active and was not successful: {self.task_doc}"
