from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def flatten_json(
    data: Any,
    parent_key: str = "",
    sep: str = ".",
) -> dict[str, Any]:
    """
    Recursively flattens nested dicts/lists into a single-level dict.

    Examples:
        {"a": {"b": 1}} -> {"a.b": 1}
        {"x": [10, 20]} -> {"x[0]": 10, "x[1]": 20}
    """
    items: dict[str, Any] = {}

    if isinstance(data, Mapping):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
            items.update(flatten_json(value, new_key, sep=sep))

    elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        for index, value in enumerate(data):
            new_key = f"{parent_key}[{index}]"
            items.update(flatten_json(value, new_key, sep=sep))

    else:
        items[parent_key] = data

    return items