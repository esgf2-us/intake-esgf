"""Exceptions for intake-esgf."""

from pathlib import Path


class IntakeESGFException(Exception):
    """Exceptions from the intake-esgf package."""


class NoSearchResults(IntakeESGFException):
    """Search returned no results."""


class SearchError(IntakeESGFException):
    """Search was used incorrectly."""


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
