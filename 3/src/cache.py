import time
from typing import Any
from collections import OrderedDict

from src.scraper import IAPICache


class InMemoryLRUAPICache(IAPICache):
    def __init__(self, maxlen: int = 1000) -> None:
        self._cache: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()
        self._maxlen = maxlen

    async def get(self, key: str) -> Any | None:
        if key in self._cache:
            value, expiry = self._cache[key]
            if expiry is None or time.time() < expiry:
                self._cache.move_to_end(key)
                return value
            else:
                del self._cache[key]
        return None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        expiry = time.time() + ttl if ttl else None
        self._cache[key] = (value, expiry)
        self._cache.move_to_end(key)
        if len(self._cache) > self._maxlen:
            self._cache.popitem(last=False)

    async def close(self) -> None:
        return None
