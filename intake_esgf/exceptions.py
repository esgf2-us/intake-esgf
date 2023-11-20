"""Exceptions for intake-esgf."""


class IntakeESGFException(Exception):
    """Exceptions from the intake-esgf package."""


class NoSearchResults(IntakeESGFException):
    """Search returned no results."""


class SearchError(IntakeESGFException):
    """Search was used incorrectly."""
