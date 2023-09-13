""""
Module resolvers exposes resolvers to map categorized fields.

Categorized fields abide by some externally defined taxonomy.
"""

from __future__ import annotations

__author__ = "fredbi"

from typing import Dict


class FieldResolver(Dict[str, str]):
    """
    FieldResolver ...

    Args:
        Dict (_type_): _description_
    """

    def get_mnemonic(self, field: str) -> str:
        """get_mnemonic ...

        Args:
            field (str): _description_

        Returns:
            str: _description_
        """
        return super().get(field)

    # @cached_property
    def get_id(self, __field__: str) -> int:
        """get_id ... TODO

        Returns:
            int: _description_
        """
        return 0


class IdentifierResolver(FieldResolver):
    """
    IdentifierResolver associates an identifier field to
    an identifier mnemnonic code.

    Example:
    IdentifierResolver({"key": "isin"}) will resolve the field "key"
    to identifier type "isin".

    TODO: should cache the numerical ID from the DB in memory.
    """


class TaxonomyResolver(FieldResolver):
    """
    TaxonomyResolver associates a taxonomy field to
    a taxonomy mnemnonic code.

    TODO: should cache the numerical ID from the DB in memory.
    """

class SchemaResolver(FieldResolver):
    """
    SchemaResolver associates an input field to a data type.
    """
