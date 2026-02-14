import asyncio
import time
from logging import Logger
from collections import namedtuple
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from logger import logger
from src.scraper import REQUEST_ID
from src.rate_limit import IRateLimiter
from src.rate_limit import SlidingWindowDequeRateLimiter


RequestInHistory = namedtuple("RequestInHistory", ["id", "timestamp", "state"])


class LimitCheckBackend:
    def __init__(
        self,
        rate_limit: IRateLimiter,
        logger: Logger,
        max_concurrent: int,
        max_requests_per_time: int,
        time_window_seconds: int,
        delay: float = 0.05,
        request_count: int = 100,
    ) -> None:
        self._logger = logger
        self._max_concurrent = max_concurrent
        self._max_requests_per_time = max_requests_per_time
        self._time_window_seconds = time_window_seconds

        self._rate_limit = rate_limit
        self._request_count = request_count

        self._delay = delay
        self._concurrent = 0
        self._h: list[float] = []  # история времени начала запросов
        self._fh: list[RequestInHistory] = []  # полная история запроса

        self._rid = 0

    def get_rid(self) -> int:
        self._rid += 1
        return self._rid

    async def request(self) -> None:
        t = time.time()
        self._h.append(t)
        self._fh.append(
            RequestInHistory(
                id=REQUEST_ID.get(),
                timestamp=t,
                state="start",
            )
        )
        await asyncio.sleep(self._delay)
        self._fh.append(
            RequestInHistory(
                id=REQUEST_ID.get(),
                timestamp=time.time(),
                state="end",
            )
        )

    async def limited_request(self) -> None:
        REQUEST_ID.set(self.get_rid())
        async with self._rate_limit:
            await self.request()

    def _check_history(self) -> list[tuple[float, float, int]]:
        sh = sorted(self._h)  # sorted history
        n = len(sh)
        i = 0
        j = 0
        exceeds = []
        while i < n and j < n:
            if sh[j] - sh[i] > self._time_window_seconds:
                i += 1
                continue
            while j < n and sh[j] - sh[i] <= self._time_window_seconds:
                if sh[j] - sh[i] <= self._time_window_seconds:
                    count = j - i + 1
                    if count > self._max_requests_per_time:
                        self._logger.error(
                            f"Найдено превышение RPS: окно {sh[i]:.3f}-{sh[j]:.3f}, кол-во={count} > {self._max_requests_per_time}"
                        )
                        exceeds.append((sh[i], sh[j], count))
                j += 1
        return exceeds

    def _check_full_history(self) -> list[tuple[int, float, int]]:
        concurrent: set[int] = set()
        exceeds = []
        for r in self._fh:
            if r.state == "start":
                concurrent.add(r.id)
            else:
                concurrent.remove(r.id)
            if len(concurrent) > self._max_concurrent:
                exceeds.append((r.id, r.timestamp, len(concurrent)))
        return exceeds

    def check_history(self) -> None:
        self._logger.info("Проверяю историю запросов на превышение RPS-лимита")
        rps_exceeds = self._check_history()
        if len(rps_exceeds) > 0:
            raise Exception("Превышен лимит запросов за временное окно")
        self._logger.info(
            "Проверяю историю запросов на превышение количества конкурентных запросов"
        )
        max_concurrent_exceeds = self._check_full_history()
        if len(max_concurrent_exceeds) > 0:
            raise Exception("Превышен максимальный уровень конкуррентных запросов")
        self._logger.info("Проверка истории запросов завершена успешно")

    async def run(self) -> None:
        tasks = [self.limited_request() for _ in range(self._request_count)]
        await asyncio.gather(*tasks)
        self.check_history()


@pytest.mark.parametrize(
    "max_concurrent, max_requests_per_time, time_window_seconds, delay, request_count",
    [
        (3, 20, 1, 0.05, 50),
        (5, 25, 1, 0.05, 100),
    ],
)
@pytest.mark.asyncio
async def test_sliding_window_rate_limiter(
    sliding_window_rate_limiter,
    test_logger,
    max_concurrent,
    max_requests_per_time,
    time_window_seconds,
    delay,
    request_count,
):
    rate_limit = sliding_window_rate_limiter(
        max_concurrent,
        max_requests_per_time,
        time_window_seconds,
    )
    backend = LimitCheckBackend(
        rate_limit=rate_limit,
        logger=test_logger,
        max_concurrent=max_concurrent,
        max_requests_per_time=max_requests_per_time,
        time_window_seconds=time_window_seconds,
        delay=delay,
        request_count=request_count,
    )
    await backend.run()
