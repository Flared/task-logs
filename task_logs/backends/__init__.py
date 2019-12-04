import warnings
from typing import Any
from datetime import datetime


def json_encoder(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()


from .stub import StubBackend

try:
    from .elastic import ElasticsearchBackend
except ImportError:  # pragma: no cover
    warnings.warn(
        "ElasticsearchBackend is not available.  Run `pip install task_logs[elasticsearch]` "
        "to add support for that backend.",
        ImportWarning,
    )

__all__ = ["ElasticsearchBackend", "StubBackend"]
