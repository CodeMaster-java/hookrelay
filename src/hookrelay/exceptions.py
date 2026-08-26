class HookRelayError(Exception):
    """Base class for all hookrelay errors."""


class EventNotFoundError(HookRelayError):
    """Raised when an operation references an event id that does not exist in the backend."""

    def __init__(self, event_id: str) -> None:
        super().__init__(f"webhook event not found: {event_id}")
        self.event_id = event_id
