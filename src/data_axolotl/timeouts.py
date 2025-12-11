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
            return f"Timeout(timeout_seconds={self.timeout_seconds}, detail={self.detail!r}, started_at={self.started_at}, time_remaining={self.time_remaining})"
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

    @property
    def duration(self) -> float:
        """
        Get the duration this timer has been running, or how long it ran, if it's already stopped

        Returns:
            The druation.

        Raises:
            RuntimeError: If the timeout has not been started yet.
        """
        if not hasattr(self, "started_at"):
            raise RuntimeError("Timeout must be started before can find the duration.")

        if hasattr(self, "stopped_at"):
            return self.stopped_at - self.started_at
        return time.monotonic() - self.started_at

    @property
    def time_remaining(self) -> float:
        """
        Get the amount of time left on this timer.

        Returns:
            The time in seconds before the deadline.

        Raises:
            RuntimeError: If the timeout has not been started yet or the timer is already stopped.
        """
        if not hasattr(self, "started_at"):
            raise RuntimeError("Timeout must be started before can find the duration.")
        if hasattr(self, "stopped_at"):
            raise RuntimeError("Timeout is already stopped")

        return self.deadline - time.monotonic()

    def stop(self) -> float:
        """
        Stops the timeout timer. Returns the duration.

        Returns:
            The duration between the current time and the start time

        Raises:
            RuntimeError: If the timeout has not been started yet.
        """
        if not hasattr(self, "started_at"):
            raise RuntimeError("Timeout must be started before it can be stopped")
        if hasattr(self, "stopped_at"):
            raise RuntimeError("Timeout is already stopped")

        self.stopped_at = time.monotonic()
        return self.stopped_at - self.started_at
