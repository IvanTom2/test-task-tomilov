import time
import asyncio
from logging import Logger
from collections import deque
from typing import Deque

from src.scraper import RateLimit
from src.scraper import IRateLimiter
from src.scraper import IRateLimiterResourceExtended
from src.scraper import REQUEST_ID


class BaseRateLimiter(IRateLimiter):
    def __init__(
        self,
        logger: Logger,
        limit: RateLimit,
    ) -> None:
        self._logger = logger
        self._lim = limit

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()


class MockRateLimiter(BaseRateLimiter):
    async def acquire(self) -> None:
        pass

    async def release(self) -> None:
        pass


class SlidingWindowDequeRateLimiter(BaseRateLimiter):
    def __init__(
        self,
        logger: Logger,
        limit: RateLimit,
        timeout_adjustment: float | None = 0.001,
    ) -> None:
        """
        Рейт-лимит через алгоритм скользящего окна на основе mono queue.
        Важно! Не является потокобезопасным.
        """

        super().__init__(logger, limit)
        if limit.max_concurrent is None:
            self._sem = None
        else:
            self._sem = asyncio.Semaphore(limit.max_concurrent)
        self._q: Deque[float] = deque()

        if timeout_adjustment == 0:
            timeout_adjustment = None
        self._timeout_adjustment = timeout_adjustment

    async def acquire(self) -> None:
        rid = REQUEST_ID.get()
        self._logger.debug(f"Жду семафор для запроса {rid}")
        if self._sem is not None:
            await self._sem.acquire()
        self._logger.debug(f"Получил семафор для запроса {rid}")

        while True:
            t = time.time()
            while self._q and t - self._q[0] > self._lim.time_window_seconds:
                self._q.popleft()
            self._logger.debug(f"Проверяю лимит запросов перед одобрением {rid}")
            if len(self._q) >= self._lim.max_requests_per_time:
                timeout = self._lim.time_window_seconds - (t - self._q[0])
                if timeout > 0:
                    if self._timeout_adjustment is None:
                        adjusted_timeout = timeout
                    else:
                        adjusted_timeout = timeout + self._timeout_adjustment
                    self._logger.debug(
                        f"Превышен лимит запросов, жду {adjusted_timeout} секунд перед одобрением {rid}"
                    )
                    await asyncio.sleep(adjusted_timeout)
                    continue
            break

        self._logger.debug(f"Регистрирую запрос в очереди {rid}")
        self._q.append(t)
        self._logger.debug(f"Запрос {rid} одобрен")

    async def release(self) -> None:
        rid = REQUEST_ID.get()
        self._logger.debug(f"Освобождаю семафор для запроса {rid}")
        if self._sem is not None:
            self._sem.release()


class SlidingWindowDequeRateLimiterUnion(IRateLimiter):
    def __init__(
        self,
        limiters: list[SlidingWindowDequeRateLimiter],
    ) -> None:
        self._limiters = limiters

    async def acquire(self) -> None:
        for limiter in self._limiters:
            await limiter.acquire()

    async def release(self) -> None:
        for limiter in reversed(self._limiters):
            await limiter.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()


class SlidingWindowDequeRateLimiterResourceExtended(IRateLimiterResourceExtended):
    def __init__(
        self,
        logger: Logger,
        limit: RateLimit,
        resource_limits: dict[str, list[RateLimit]] | None = None,
        timeout_adjustment: float | None = 0.001,
    ) -> None:
        self._logger = logger
        self._common_limiter = SlidingWindowDequeRateLimiter(
            logger,
            limit,
            timeout_adjustment,
        )
        self._limiters: dict[str, SlidingWindowDequeRateLimiterUnion] = self._combine(
            resource_limits,
            timeout_adjustment,
        )

    def _combine(
        self,
        resource_limits: dict[str, list[RateLimit]] | None,
        timeout_adjustment: float | None = 0.001,
    ) -> dict[str, SlidingWindowDequeRateLimiterUnion]:
        if not resource_limits:
            return {}
        limiters_map: dict[str, SlidingWindowDequeRateLimiterUnion] = {}
        for resource, limits in resource_limits.items():
            limiters = [self._common_limiter]
            for limit in limits:
                limiter = SlidingWindowDequeRateLimiter(
                    logger=self._logger,
                    limit=limit,
                    timeout_adjustment=timeout_adjustment,
                )
                limiters.append(limiter)
            limiters_map[resource] = SlidingWindowDequeRateLimiterUnion(limiters)
        return limiters_map

    def __call__(self, resource: str) -> IRateLimiter:
        if resource in self._limiters:
            return self._limiters[resource]
        return self._common_limiter
