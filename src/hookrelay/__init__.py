from importlib.metadata import version as _version

from hookrelay.backends.base import Backend
from hookrelay.backends.memory import MemoryBackend
from hookrelay.exceptions import EventNotFoundError, HookRelayError
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy
from hookrelay.worker import Worker

# Reads the installed package's version instead of hardcoding a copy here, so it
# cannot drift from the one in pyproject.toml (as it already did once).
__version__ = _version("hookrelay")

__all__ = [
    "Backend",
    "MemoryBackend",
    "EventStatus",
    "WebhookEvent",
    "RetryPolicy",
    "Worker",
    "HookRelayError",
    "EventNotFoundError",
]
