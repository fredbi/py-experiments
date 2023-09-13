"""
Module exceptions wraps exception types for all etl modules.

This allows for a clear disambiguification about the origin of exceptions.
"""

from __future__ import annotations

__author__ = "fredbi"


class InvalidFileError(Exception):
    "Input file is invalid"


class InvalidDataError(Exception):
    "Some content in the input file is invalid"


class RuntimeEtlError(Exception):
    "Runtime ETL error"


class IOEtlError(Exception):
    "IO ETL error"


class DevMistakeError(Exception):
    "Development mistake"


class ConfigError(Exception):
    "Configuration error"


class FlagsError(Exception):
    "CLI fags arguments error"


class UndeclaredFieldError(DevMistakeError):
    "Internal error: a reference was made to an undeclared field"
