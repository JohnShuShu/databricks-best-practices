"""
Retry utilities with exponential backoff.

Usage:
    from utils.retry import with_retry

    @with_retry(max_attempts=3, retryable_exceptions=(TimeoutError, ConnectionError))
    def call_external_api():
        # Your code here
        pass
"""

import time
from functools import wraps
from typing import Tuple, Type, Callable, Any
import logging

logger = logging.getLogger(__name__)


def with_retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Callable[[Exception, int], None] = None
):
    """
    Decorator for retry logic with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        backoff_factor: Multiplier for delay between retries
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        retryable_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback function called on each retry

    Returns:
        Decorated function with retry logic

    Example:
        @with_retry(max_attempts=3, retryable_exceptions=(TimeoutError,))
        def fetch_data():
            return requests.get(url, timeout=10)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt < max_attempts - 1:
                        # Calculate delay with cap
                        sleep_time = min(delay, max_delay)

                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}. "
                            f"Retrying in {sleep_time:.1f}s..."
                        )

                        # Call optional callback
                        if on_retry:
                            on_retry(e, attempt + 1)

                        time.sleep(sleep_time)
                        delay *= backoff_factor
                    else:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}. "
                            f"Last error: {e}"
                        )

            raise last_exception

        return wrapper
    return decorator


def retry_on_exception(
    func: Callable,
    max_attempts: int = 3,
    delay: float = 1.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Any:
    """
    Functional retry wrapper (non-decorator version).

    Args:
        func: Function to call
        max_attempts: Maximum attempts
        delay: Delay between attempts
        exceptions: Exception types to catch

    Returns:
        Result of successful function call

    Example:
        result = retry_on_exception(
            lambda: requests.get(url),
            max_attempts=3,
            exceptions=(requests.Timeout,)
        )
    """
    last_exception = None

    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_attempts - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(delay)

    raise last_exception


class RetryableOperation:
    """
    Class-based retry handler for more complex scenarios.

    Example:
        op = RetryableOperation(max_attempts=3)

        with op:
            result = risky_operation()
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.initial_delay = initial_delay
        self.retryable_exceptions = retryable_exceptions
        self.attempts = 0
        self.last_exception = None

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic."""
        delay = self.initial_delay

        for attempt in range(self.max_attempts):
            self.attempts = attempt + 1
            try:
                return func(*args, **kwargs)
            except self.retryable_exceptions as e:
                self.last_exception = e
                if attempt < self.max_attempts - 1:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= self.backoff_factor

        raise self.last_exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Could add cleanup logic here
        pass
