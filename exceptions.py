class TemporaryProcessingError(Exception):
    """Raised when order processing fails temporarily and can be retried."""
    pass


class PermanentProcessingError(Exception):
    """Raised when order processing fails permanently and should not be retried."""
    pass