from typing import Any


def is_scalar(item: Any) -> bool:
    return isinstance(item, (int, float, str, bool))


def is_dict(item: Any) -> bool:
    return isinstance(item, dict)


def is_list(item: Any) -> bool:
    return isinstance(item, list)
