import time


class Timeout:
    """
    Utility class for keeping track of assorted timeouts.

    Attributes:
        timeout_seconds: The duration of the timeout in seconds.
        detail: Optional descriptive detail about the timeout's purpose.
        started_at: The monotonic time when the timeout was started (set by start()).
    """

    def __init__(self, timeout_seconds: float, detail: str = ""):
        """
        Args:
            timeout_seconds: The duration of the timeout in seconds.
            detail: Optional descriptive detail about the timeout's purpose.
        """
        self.timeout_seconds = timeout_seconds
        self.detail = detail

    def __repr__(self) -> str:
        started = hasattr(self, "started_at")
        if started:
            return f"Timeout(timeout_seconds={self.timeout_seconds}, detail={self.detail!r}, started_at={self.started_at})"
        return (
            f"Timeout(timeout_seconds={self.timeout_seconds}, detail={self.detail!r})"
        )

    def start(self) -> float:
        """
        Start the timeout timer.

        Returns:
            The monotonic time when the timeout was started.
        """
        self.started_at = time.monotonic()
        return self.started_at

    @property
    def deadline(self) -> float:
        """
        Get the monotonic time when the timeout will expire.

        Returns:
            The monotonic time representing the deadline.

        Raises:
            RuntimeError: If the timeout has not been started yet.
        """
        if not hasattr(self, "started_at"):
            raise RuntimeError("Timeout must be started before we know the deadline.")
        return self.started_at + self.timeout_seconds

    def is_timed_out(self) -> bool:
        """
        Check if the timeout period has elapsed.

        Returns:
            True if the current time has passed the deadline, False otherwise.
        """
        return time.monotonic() > self.deadline
