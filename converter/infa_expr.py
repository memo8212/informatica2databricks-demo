"""Backwards-compatible re-export of the canonical translator.

The implementation now lives in `infa2dbx.expr` (shared by the app and the
converter).
"""
from infa2dbx.expr import UNSUPPORTED, translate_expression  # noqa: F401

__all__ = ["translate_expression", "UNSUPPORTED"]
