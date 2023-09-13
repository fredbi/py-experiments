"""
Module config exposes a dict-like Config class.

This is a configuration loader that abides by
the 12-factors app requirements.
"""

from __future__ import annotations

__author__ = "fredbi"

import os
import pathlib
import dataclasses
from dataclasses import dataclass
from typing import Any, List

from dynaconf import Dynaconf

from etl.pkg import exceptions

DEFAULT_ENV_PREFIX = "APP_"

DEFAULT_CONTEXT_ENV = "APP_ENV"
SUPPORTED_CONTEXTS = {
    "dev": True,
    "uat": True,
    "prod": True,
    "local-testing": True,
    "ci-testing": True,
}
DEFAULT_CONTEXT = "local-testing"
DEFAULTS_SETTINGS = "default"

DEFAULT_SETTING_ROOT_PATH_ENV = "APP_CONFIG_PATH"
DEFAULT_SETTING_ROOT_PATH = "."

DEFAULT_SETTING_CONTEXT_FOLDER = "config.d"
DEFAULT_SETTING_CONFIG_NAME = "config"


@dataclass
class Registry(dict):
    """
    Registry provides configurable parameters for a job.

    It equips the app with a 12-factor setup, configurable from YAML or JSON files
    as well as from environment variables.

    Environment-specific configuration and secrets are merged
    on top of the default configuration.

    See: https://www.dynaconf.com/

    Possible alternative contemplated: https://configloader.readthedocs.io/en/stable/


    TODO(fredbi, before Azure integration): Azure key Vault secrets:
    make an extension loader based on Azure SDK

    ## NOTE: Hashicorp Vault configuration
    VAULT_URL_FOR_DYNACONF="http://localhost:8200"
    # Specify the secrets engine for kv, default is 1
    VAULT_KV_VERSION_FOR_DYNACONF=1
    # Authenticate with token https://www.vaultproject.io/docs/auth/token
    VAULT_TOKN_FOR_DYNACONF="myroot"
    # Authenticate with AppRole https://www.vaultproject.io/docs/auth/approle
    VAULT_ROLE_ID_FOR_DYNACONF="role-id"
    VAULT_SECRET_ID_FOR_DYNACONF="secret-id"
    # Authenticate with AWS IAM https://www.vaultproject.io/docs/auth/aws
    # The IAM Credentials can be retrieved from the standard providers:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    VAULT_AUTH_WITH_IAM_FOR_DYNACONF=True
    VAULT_AUTH_ROLE_FOR_DYNACONF="vault-role"
    # Authenticate with root token
    VAULT_ROOT_TOKEN_FOR_DYNACONF="root-token"
    """

    context_var: str = DEFAULT_CONTEXT_ENV
    root_var: str = DEFAULT_SETTING_ROOT_PATH_ENV
    env_prefix = DEFAULT_ENV_PREFIX
    config_context_folder = DEFAULT_SETTING_CONTEXT_FOLDER
    config_name: str = DEFAULT_SETTING_CONFIG_NAME
    context_folder: str = DEFAULT_SETTING_CONTEXT_FOLDER
    defaults: str = DEFAULTS_SETTINGS

    root: str = ""
    context: str = ""
    preload_defaults: List[str] = dataclasses.field(default=list)
    settings_files: List[str] = dataclasses.field(default=list)
    include_files: List[str] = dataclasses.field(default=list)

    secret_suffix: str = ".secrets"
    config_ext: List[str] = dataclasses.field(default=list)

    _inner: Dynaconf = None

    def __post_init__(self):
        if not self.config_ext:
            self.config_ext = ["yaml", "yml", "json"]

        if not self.context:
            self.context = os.environ.get(self.context_var, DEFAULT_CONTEXT)

        if not SUPPORTED_CONTEXTS[self.context]:
            raise exceptions.ConfigError from Exception(
                f"not a supported context: {self.context}",
            )

        if not self.root:
            self.root = os.environ.get(self.root_var, DEFAULT_SETTING_ROOT_PATH)

        if not self.preload_defaults:
            for ext in self.config_ext:
                # search for {root}/config.default.{ext}
                default_config = pathlib.Path(
                    self.root, f"{self.config_name}.{self.defaults}.{ext}"
                )
                self.preload_defaults.append(str(default_config))

        if not self.settings_files:
            for ext in self.config_ext:
                # search for {root}/config.{ext}
                root_config = pathlib.Path(self.root, f"{self.config_name}.{ext}")
                self.settings_files.append(root_config)

        if not self.include_files:
            for ext in self.config_ext:
                # search for {root}/config.d/{context}/config.{ext}
                # (or {root}/config.d/{context}/config.local.{ext})
                context_config = pathlib.Path(
                    self.root,
                    self.context_folder,
                    self.context,
                    f"{self.config_name}.{ext}",
                )
                self.include_files.append(context_config)

        try:
            self._inner = Dynaconf(
                autocast=False,  # we don't want this magic wand
                # Dynaconf's builtin handling of "environments" (aka contexts)
                # is based on layered config sections, not merges
                environments=False,
                apply_default_on_none=True,  # this is for YAML caveats with python
                merge_enabled=True,
                core_loaders=["YAML", "JSON"],
                env_switcher=self.context_var,
                root_path=self.root,
                envvar_prefix=self.env_prefix,
                preload=self.preload_defaults,
                settings_files=self.settings_files,
                include_files=self.include_files,
            )
        except (
            Exception
        ) as exception:  # we immediately reraise after logging, so pylint: disable=broad-exception-caught
            raise exceptions.ConfigError from exception

    def load(self) -> Registry:
        """
        load the configuration from file or environment.

        NOTE: with Dynaconf, this doesn't actually do anything, since
        Dynaconf's design is to perform the load at object instantiation time.
        """

        return self

    def get(self, key: str, default: Any) -> Any:
        """
        get a key from the configuration registry.

        Args:
            key (str): the config key, e.g. "path.to.this.key"
            default (Any): the default value  to use if the key is not defined

        Returns:
            Any: the configured value
        """
        return self._inner.get(key, default)

    def dump(self) -> None:
        """
        dump the loaded config.
        """
        self._inner.inspect_settings()
