import os
import time
import random
from functools import wraps
from typing import Callable, Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


def singleton(cls, *args, **kw):
    """
    Per-process singleton decorator.

    Caches one instance per class per OS process (os.getpid()), so each worker
    process in a daemon (producer, consumer, Spark executor) gets its own instance.
    """
    instances = {}

    def _singleton():
        key = str(cls) + str(os.getpid())
        if key not in instances:
            instances[key] = cls(*args, **kw)
        return instances[key]
    return _singleton


def retry(
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    jitter: bool = True,
):
    """
    Retry decorator with exponential backoff and optional jitter.

    Args:
        max_attempts: Maximum number of attempts (including first try)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries in seconds
        exceptions: Tuple of exception types to retry on
        jitter: Add random jitter to delay to prevent thundering herd
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    if jitter:
                        delay = delay * (0.8 + 0.4 * random.random())
                    time.sleep(delay)
            raise last_exception  # type: ignore
        return cast(F, wrapper)
    return decorator
