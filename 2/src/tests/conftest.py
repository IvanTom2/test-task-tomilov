import sys
from pathlib import Path
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from logger import logger
from src.scraper import RateLimit
from src.rate_limit import SlidingWindowDequeRateLimiter


@pytest.fixture
def test_logger():
    return logger


@pytest.fixture
def sliding_window_rate_limiter(test_logger):
    def _create(
        max_concurrent: int,
        max_requests_per_time: int,
        time_window_seconds: int,
    ):
        limit = RateLimit(
            max_concurrent=max_concurrent,
            max_requests_per_time=max_requests_per_time,
            time_window_seconds=time_window_seconds,
        )
        return SlidingWindowDequeRateLimiter(
            logger=test_logger,
            limit=limit,
            timeout_adjustment=0.001,
        )

    return _create
