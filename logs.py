"""Logging setup: laravel-cloud-logging plus per-request/job context fields."""
from __future__ import annotations

import atexit
import contextvars
import logging
import threading
from contextlib import contextmanager

from laravel_cloud_logging import configure

CONTEXT = contextvars.ContextVar("log_context", default={})
LOGGER = logging.getLogger("cloud_demo")
LOCK = threading.RLock()


@contextmanager
def context(**values):
    token = CONTEXT.set({**CONTEXT.get(), **values})
    try:
        yield
    finally:
        CONTEXT.reset(token)


class ContextFilter(logging.Filter):
    """Copy role and context() values into the record; laravel-cloud-logging puts them in context."""

    def __init__(self, role):
        super().__init__()
        self.role = role

    def filter(self, record):
        # extra= wins over request/job context, so a field named like a context key never raises.
        for key, value in {"role": self.role, **CONTEXT.get()}.items():
            record.__dict__.setdefault(key, value)
        return True


def setup(role: str):
    if role not in ("web", "worker"):
        raise ValueError("role must be web or worker")
    with LOCK:
        if not getattr(setup, "registered", False):
            setup.registered = True
            atexit.register(lambda: LOGGER.info("shutdown"))
        configure()  # replaces the root handlers, so every call adds the filter to fresh ones
        for owned in logging.getLogger().handlers:
            owned.addFilter(ContextFilter(role))
    return LOGGER


def self_check():
    from laravel_cloud_logging import CloudHandler
    setup("web")
    setup("worker")
    root = logging.getLogger().handlers
    assert len(root) == 1 and isinstance(root[0], CloudHandler) and LOGGER.propagate
    assert [f.role for f in root[0].filters if isinstance(f, ContextFilter)] == ["worker"]
    record = LOGGER.makeRecord(LOGGER.name, logging.INFO, __file__, 1, "x", (), None, extra={"run": "extra"})
    with context(run="context", job="demo"):
        next(f for f in root[0].filters if isinstance(f, ContextFilter)).filter(record)
    assert (record.role, record.run, record.job) == ("worker", "extra", "demo")
    assert CONTEXT.get() == {}
    setup("web")
    print("Logging setup and context checks passed")
