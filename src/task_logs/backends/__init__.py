import warnings

from .stub import StubBackend

try:
    from .elastic import ElasticsearchBackend
except ImportError:  # pragma: no cover
    warnings.warn(
        "ElasticsearchBackend is not available.  Run `pip install "
        "task_logs[elasticsearch]` to add support for that backend.",
        ImportWarning,
    )

__all__ = ["ElasticsearchBackend", "StubBackend"]
