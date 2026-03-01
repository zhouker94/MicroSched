class MicroSchedError(Exception):
    """Base exception class for all MicroSched errors."""
    pass


class MasterConnectionError(MicroSchedError):
    """Raised when a Worker fails to communicate with the Master."""
    pass


class TaskExecutionError(MicroSchedError):
    """Raised when a task fails during execution in the Worker."""

    def __init__(self, message: str, returncode: int = -1, stdout: str = "", stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
