class TemporaryProcessingError(Exception):
    """Raised when order processing can be retried."""

class PermanentProcessingError(Exception):
    pass