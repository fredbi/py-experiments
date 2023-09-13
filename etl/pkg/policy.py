"""
Module policy expose types to tell a job how to  handle data issues.
"""

from __future__ import annotations

__author__ = "fredbi"

import dataclasses
from dataclasses import dataclass
from enum import Enum, unique
from typing import Dict, TypedDict

from etl.pkg import config


@unique
class ActionOnIssue(Enum):
    """
    ActionOnIssue tells what to do
    whenever a data issue is detected.
    """

    FAIL = 0
    TO_RECYCLE_BIN = 1
    SKIP = 2
    IGNORE = 3
    CREATE = 4


class PolicyDict(TypedDict):
    """
    PolicyDict defines actions on each kind of recognized data issue.
    """

    when_any_is_not_identified: ActionOnIssue
    when_any_is_not_valid: ActionOnIssue
    when_any_taxonomy_is_not_mapped: ActionOnIssue
    when_field_is_not_identified: Dict[str, ActionOnIssue]
    when_field_is_not_valid: Dict[str, ActionOnIssue]
    when_field_taxonomy_is_not_mapped: Dict[str, ActionOnIssue]


@dataclass
class Policy:
    """
    Policy defines an issue handling policy.

    It determines the action to carry out
    when faced with data issues.
    """

    dict: PolicyDict = dataclasses.field(
        default_factory=lambda x: {
            "when_any_is_not_identified": ActionOnIssue.FAIL,
            "when_any_is_not_valid": ActionOnIssue.FAIL,
            "when_any_taxonomy_is_not_mapped": ActionOnIssue.FAIL,
            "when_field_is_not_identified": {},
            "when_field_is_not_valid": {},
            "when_field_taxonomy_is_not_mapped": {},
        }
    )

    def action_on_invalid_field(self, column_name: str) -> ActionOnIssue:
        """action_on_invalid_field yields the policy set for a given input column"""

        strictiest_action: ActionOnIssue = ActionOnIssue.IGNORE
        action: ActionOnIssue = ActionOnIssue.IGNORE

        if self.dict["when_any_is_not_valid"]:
            strictiest_action = self.dict["when_any_is_not_valid"]

        if self.dict["when_field_is_not_valid"]:
            action = self.dict["when_field_is_not_valid"].get(
                column_name, ActionOnIssue.IGNORE
            )

        if action == ActionOnIssue.FAIL:
            strictiest_action = action

        if action == ActionOnIssue.SKIP and strictiest_action != ActionOnIssue.FAIL:
            strictiest_action = action

        return strictiest_action


@dataclass
class PolicyResolver:
    """
    _summary_

    Returns:
        _type_: _description_
    """

    policy: Policy

    def tolerates_data_issues(self) -> bool:
        """_summary_ (TODO)

        Returns:
            bool: _description_
        """
        return self["policy.when_any_is_not_identified"] == ActionOnIssue.FAIL


def strict() -> Policy:
    """
    strict yields a basic policy which fails on any kind of mismatch.

    Returns:
        Policy: the (default) strictiest data issue handling policy.
    """

    return Policy(
        {
            "when_any_is_not_identified": ActionOnIssue.FAIL,
            "when_any_is_not_valid": ActionOnIssue.FAIL,
            "when_any_taxonomy_is_not_mapped": ActionOnIssue.FAIL,
            "when_field_is_not_identified": {},
            "when_field_is_not_valid": {},
            "when_field_taxonomy_is_not_mapped": {},
        }
    )

def skip() -> Policy:
    """
    skip yields a basic policy which skips lines with data errors.    

    Returns:
        Policy: a policy skipping all problems
    """
    return Policy(
        {
            "when_any_is_not_identified": ActionOnIssue.SKIP,
            "when_any_is_not_valid": ActionOnIssue.SKIP,
            "when_any_taxonomy_is_not_mapped": ActionOnIssue.SKIP,
            "when_field_is_not_identified": {},
            "when_field_is_not_valid": {},
            "when_field_taxonomy_is_not_mapped": {},
        }
    )

def from_config(_cfg: config.Registry) -> Policy:
    """
    from_config creates a Policy based on the configuration

    Args:
        configConfig (_type_): _description_

    TODO

    Returns:
        Policy: _description_
    """

    return strict()
