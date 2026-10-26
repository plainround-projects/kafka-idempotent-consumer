"""Named errors of the domain."""


class PoisonMessageError(ValueError):
    """The record can never be processed, however often it is retried."""
