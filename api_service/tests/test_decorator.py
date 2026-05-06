"""Tests for common.decorator.retry."""
import pytest
from unittest.mock import MagicMock, call
from common.decorator import retry


class TestRetryDecorator:
    """Tests for the retry decorator with exponential backoff."""

    def test_success_on_first_try(self):
        """Should return immediately if function succeeds on first try."""
        mock_func = MagicMock(return_value="success")
        decorated = retry(max_attempts=3)(mock_func)

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_on_exception_then_success(self):
        """Should retry after failures and eventually succeed."""
        mock_func = MagicMock(side_effect=[Exception("fail"), Exception("fail"), "success"])
        decorated = retry(max_attempts=3, base_delay=0.1)(mock_func)

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 3

    def test_raises_after_max_attempts(self):
        """Should raise last exception after exhausting retries."""
        mock_func = MagicMock(side_effect=Exception("persistent failure"))
        decorated = retry(max_attempts=3, base_delay=0.01)(mock_func)

        with pytest.raises(Exception, match="persistent failure"):
            decorated()

        assert mock_func.call_count == 3

    def test_retry_specific_exception(self):
        """Should only retry on specified exception types."""
        mock_func = MagicMock(side_effect=[ValueError("retry me"), "success"])
        decorated = retry(max_attempts=3, exceptions=(ValueError,))(mock_func)

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 2

    def test_no_retry_on_different_exception(self):
        """Should not retry if exception type is not in the list."""
        mock_func = MagicMock(side_effect=TypeError("wrong type"))
        decorated = retry(max_attempts=3, exceptions=(ValueError,))(mock_func)

        with pytest.raises(TypeError):
            decorated()

        assert mock_func.call_count == 1

    def test_exponential_backoff(self):
        """Should increase delay between retries."""
        mock_func = MagicMock(side_effect=[Exception("fail"), "success"])
        sleep_times = []

        def mock_sleep(delay):
            sleep_times.append(delay)

        # Patch time.sleep
        import time as time_module
        original_sleep = time_module.sleep
        time_module.sleep = mock_sleep

        try:
            decorated = retry(max_attempts=2, base_delay=1.0, jitter=False)(mock_func)
            decorated()
            assert len(sleep_times) == 1
            assert sleep_times[0] == pytest.approx(1.0, rel=0.01)  # base_delay * 2^0 = 1.0
        finally:
            time_module.sleep = original_sleep

    def test_max_delay_cap(self):
        """Should cap delay at max_delay."""
        mock_func = MagicMock(side_effect=[Exception("fail"), "success"])
        sleep_times = []

        def mock_sleep(delay):
            sleep_times.append(delay)

        import time as time_module
        original_sleep = time_module.sleep
        time_module.sleep = mock_sleep

        try:
            decorated = retry(max_attempts=5, base_delay=10, max_delay=5, jitter=False)(mock_func)
            decorated()
            assert len(sleep_times) == 1
            assert sleep_times[0] == pytest.approx(5.0, rel=0.01)  # capped at max_delay
        finally:
            time_module.sleep = original_sleep

    def test_jitter_variation(self):
        """Should add jitter to delay."""
        mock_func = MagicMock(side_effect=[Exception("fail"), "success"])
        sleep_times = []

        def mock_sleep(delay):
            sleep_times.append(delay)

        import time as time_module
        original_sleep = time_module.sleep
        time_module.sleep = mock_sleep

        try:
            # With jitter, delay should vary within ±20%
            decorated = retry(max_attempts=2, base_delay=1.0, jitter=True)(mock_func)
            decorated()
            assert len(sleep_times) == 1
            # Expected range: 0.8 to 1.2
            assert 0.8 <= sleep_times[0] <= 1.2
        finally:
            time_module.sleep = original_sleep
