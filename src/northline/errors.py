"""Pipeline exceptions.

Retryable storage conflicts are not these types. Reconciliation and induced
failures must stop the run instead of being replayed inside the retry helper.
"""


class PipelineError(Exception):
    """Base error for a Northline pipeline failure."""


class ConfigError(PipelineError):
    """Invalid environment or missing non-secret configuration."""


class ReconciliationError(PipelineError):
    """A control check failed after the audit row was written."""


class InducedFailure(PipelineError):
    """Deliberate stop used to prove a rerun can finish gold."""
