from typing import Any, Optional
import pytest


def approxify(x: Any, rel: Optional[float] = None, abs: Optional[float] = None) -> Any:
    """
    Test helper to approximately check (nested) dict/list/tuple constructs containing floats/ints,
    (using `pytest.approx`).

    >>> assert {"foo": [10.001, 2.3001]} == approxify({"foo": [10, 2.3]}, abs=0.1)
    """
    if isinstance(x, dict):
        return {k: approxify(v, rel=rel, abs=abs) for k, v in x.items()}
    elif isinstance(x, (list, tuple)):
        return type(x)(approxify(v, rel=rel, abs=abs) for v in x)
    elif isinstance(x, str):
        return x
    elif isinstance(x, (float, int)):
        return pytest.approx(x, rel=rel, abs=abs)
    elif x is None:
        return x
    else:
        # TODO: support more types
        raise ValueError(x)