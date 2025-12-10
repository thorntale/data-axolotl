import time

from typing import (
    List,
)


def is_timed_out(deadlines: List[float]):
    """
    Given a list of monotonic timeout times, check whether we've gone past any of them.
    Args:
        deadlines: List of time.monotonic() + timeout durations

    Returns:
        True if we've passed any of the deadlines,
    """

    return time.monotonic() > min(deadlines)

def to_string_array(items: List[str] | None) -> str:
    """
    takes ["a", "b", "c"] -> "('a', 'b', 'c')"
    """
    return f"""('{ "', '".join(items or [])}')"""
