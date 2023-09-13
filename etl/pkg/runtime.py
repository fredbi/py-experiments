"""
Module runtime exposes a runtime dependency injector.

The Runtime component allows consumers to use shared resources
such as database connections, loggers, etc.
"""

from __future__ import annotations

__author__ = "fredbi"

from typing import Any, Iterator
from dataclasses import dataclass
from contextlib import contextmanager

import structlog

from etl.pkg import config
from etl.pkg import exceptions


@dataclass(frozen=False)
class Runtime:
    """
    Runtime injects runtime dependencies.

    This object is extensible and aimed at providing consumer objects
    with common things like DB connection, config, logger, trace exporter etc.
    """

    resources: Any

    _config: config.Registry = None
    _logger: Any = None

    def __post_init__(self):
        """
        _summary_ TODO

        Raises:
            DevMistakeError: _description_
        """
        self._config = config.Registry().load()
        self._logger = structlog.get_logger()

        if self._logger is None or self._config is None:
            raise exceptions.DevMistakeError from TypeError(
                "invalid arguments when calling the Runtime constructor"
            )

    def start(self) -> Runtime:
        """
        start initializes all resources, e.g. database connections etc.
        """
        return self

    def stop(self) -> None:
        """
        stop relinquishes all resources.
        """

    def _open(self) -> None:
        """_summary_"""

    def _close(self) -> None:
        """_summary_"""

    def get_config(self) -> config.Registry:
        """
        get_config returns the global config registry

        Returns:
            config.Registry: a dict-like registry storing settings
        """
        return self._config

    def get_logger(self) -> Any:
        """
        get_logger returns the runtime logger

        Returns:
            Any: _description_
        """
        return self._logger

    def get_target_db_connection(self) -> Any:
        """_summary_

        Returns:
            Any: _description_

        Todo: acquire resources
        """
        return None


@contextmanager
def context(runtime: Runtime) -> Iterator[Runtime]:
    """
    context provides a context manager to handle a Runtime resource.

    It wraps the start and close of an instance of a runtime.

    Args:
        kwargs: key-value arguments used to instantiate a new (unitialized) Runtime

    Yields:
        Iterator[Runtime]: yields one value with the started runtime (with database resources)
    """
    logger = runtime.get_logger().bind(module="runtime")

    try:
        runtime_acquired = runtime.start()
    except (
        Exception
    ) as error:  # we immediately reraise after logging, so pylint: disable=broad-exception-caught
        logger.error("could not properly start the runtime", err=error)

        raise exceptions.RuntimeEtlError from error

    try:
        yield runtime_acquired
    except exceptions.RuntimeEtlError as error:
        logger.error("runtime error", err=error)

        raise error
    except (
        Exception
    ) as error:  # we immediately reraise after logging, so pylint: disable=broad-exception-caught
        logger.error("app error", err=error)

        raise error
    finally:
        try:
            runtime.stop()
        except (
            Exception
        ) as error:  # we immediately reraise after logging, so pylint: disable=broad-exception-caught
            raise exceptions.RuntimeEtlError from error
        else:
            logger.info("etl runtime stopped")
