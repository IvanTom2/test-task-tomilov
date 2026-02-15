import math
import asyncio
from datetime import datetime, timezone
import contextvars
from dataclasses import dataclass

from abc import ABC
from abc import abstractmethod
import json
from typing import Final, Any
from logging import Logger

from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import ClientResponse

from settings import timezone as tz
from src.use_case import IGithubReposScrapper
from src.models import Repository
from src.models import RepositoryAuthorCommitsNum

from src.exceptions import GitHubAPIError
from src.exceptions import GitHubAPIRateLimitError
from src.exceptions import GitHubAPINotFoundError
from src.exceptions import GitHubAPIValidationError
from src.exceptions import GitHubAPIServerError
from src.exceptions import GitHubAPIUnauthorizedError
from src.exceptions import GitHubAPIForbiddenError
from src.exceptions import GitHubAPIBadRequestError
from src.exceptions import GitHubAPIConflictError

from src.exceptions import RetryFailedError


GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"
REQUEST_ID = contextvars.ContextVar("request_id", default=0)


@dataclass
class RateLimit:
    max_concurrent: int | None
    max_requests_per_time: int
    time_window_seconds: int

    def __post_init__(self):
        if self.max_concurrent is not None and self.max_concurrent < 0:
            raise ValueError("max_concurrent должен быть неотрицательным числом")
        if self.max_requests_per_time <= 0:
            raise ValueError("max_requests_per_time должен быть положительным числом")
        if self.time_window_seconds <= 0:
            raise ValueError("time_window_seconds должен быть положительным числом")


class IRateLimiter(ABC):
    @abstractmethod
    async def acquire(self) -> None:
        """
        Занимает разрешение на выполнение запроса.
        """

        pass

    @abstractmethod
    async def release(self) -> None:
        """
        Освобождает разрешение на выполнение запроса.
        """

        pass

    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class IRateLimiterResourceExtended(ABC):
    @abstractmethod
    def __call__(self, resource: str) -> IRateLimiter:
        pass


class IAPICache(ABC):
    @abstractmethod
    async def get(self, key: str) -> Any | None:
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class GithubReposScrapper(IGithubReposScrapper):
    def __init__(
        self,
        access_token: str,
        logger: Logger,
        rate_limit: IRateLimiterResourceExtended,
        timeout: int = 15,
        max_retries: int = 3,
        cache: IAPICache | None = None,
    ) -> None:
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=ClientTimeout(total=timeout),
        )
        self._rate_limit = rate_limit
        self._logger = logger
        self._max_retries = max_retries
        self._id = 0

        self._cache = cache

    def _get_request_id(self) -> int:
        self._id += 1
        return self._id

    async def _validate_response(self, response: ClientResponse) -> Any:
        url = str(response.url)
        if 200 <= response.status < 300:
            return await response.json()

        try:
            error_data = await response.json()
            message = error_data.get("message", "Unknown error")
        except Exception:
            message = "Failed to parse error response"

        if response.status == 400:
            raise GitHubAPIBadRequestError(message, url)
        elif response.status == 401:
            raise GitHubAPIUnauthorizedError(message, url)
        elif response.status == 403:
            remaining = response.headers.get("X-RateLimit-Remaining", "1")
            reset_time = response.headers.get("X-RateLimit-Reset")
            if remaining == "0":
                raise GitHubAPIRateLimitError(
                    message,
                    url,
                    int(reset_time) if reset_time else None,
                )
            else:
                raise GitHubAPIForbiddenError(message, url)
        elif response.status == 404:
            raise GitHubAPINotFoundError(message, url)
        elif response.status == 409:
            raise GitHubAPIConflictError(message, url)
        elif response.status == 422:
            raise GitHubAPIValidationError(message, url)
        elif 500 <= response.status < 600:
            raise GitHubAPIServerError(response.status, message, url)
        else:
            raise GitHubAPIError(response.status, message, url)

    async def _make_request(
        self,
        endpoint: str,
        resource: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        cached: bool = False,
    ) -> Any:
        REQUEST_ID.set(self._get_request_id())
        self._logger.debug(f"Выполняю запрос {method} {endpoint}")
        URL = f"{GITHUB_API_BASE_URL}/{endpoint}"
        cache_key = f"{method}:{endpoint}:{json.dumps(params, sort_keys=True)}"
        if cached and self._cache is not None:
            cached_data = await self._cache.get(cache_key)
            if cached_data is not None:
                self._logger.debug(f"Использую кэш для запроса {cache_key}")
                return cached_data

        async with self._rate_limit(resource):
            async with self._session.request(
                method,
                URL,
                params=params,
            ) as response:
                data = await self._validate_response(response)
                if cached and self._cache is not None:
                    self._logger.debug(f"Кэширую ответ для запроса {cache_key}")
                    await self._cache.set(cache_key, data, ttl=60 * 15)
                return data

    async def _make_request_retry(
        self,
        endpoint: str,
        resource: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        max_retries: int | None = None,
        wait_reset_sec_max: int = 10,
    ) -> Any:
        if max_retries is None:
            max_retries = self._max_retries
        for retry in range(max_retries):
            try:
                return await self._make_request(
                    endpoint=endpoint,
                    resource=resource,
                    method=method,
                    params=params,
                )

            except GitHubAPIRateLimitError as ex:
                if retry == max_retries - 1:
                    raise RetryFailedError from ex

                ridv = REQUEST_ID.get()
                rid = f" для запроса {ridv}" if ridv else ""
                reset_time = ex.reset_time
                if reset_time is not None and reset_time > wait_reset_sec_max:
                    raise RetryFailedError from ex
                if reset_time:
                    ws = reset_time - int(datetime.now(tz=timezone.utc).timestamp()) + 1
                    if ws > 0:
                        self._logger.warning(f"Превышен лимит{rid}. Ожидаю {ws} сек.")
                        await asyncio.sleep(ws)
                        continue

            except GitHubAPIServerError as ex:
                try_ = f"Попытка {retry + 1}/{max_retries}"
                self._logger.error(
                    f"Ошибка при получении репозиториев. {try_}. Ошибка: {ex}"
                )
                if retry == max_retries - 1:
                    raise RetryFailedError from ex
                await asyncio.sleep(2**retry)

            except (
                GitHubAPIUnauthorizedError,
                GitHubAPINotFoundError,
                GitHubAPIValidationError,
                GitHubAPIForbiddenError,
                GitHubAPIError,
                Exception,
            ) as ex:
                self._logger.error(f"Ошибка при получении репозиториев: {ex}")
                raise RetryFailedError from ex

        return None

    async def _get_top_repositories(
        self,
        limit: int = 100,
        page: int = 1,
        max_retries: int | None = None,
    ) -> list[Repository]:
        """
        GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories
        """
        if limit < 0:
            raise ValueError("limit должен быть положительным числом")
        if limit > 100:
            self._logger.warning(
                "limit не может быть больше 100, будет установлен в 100"
            )

        limit = max(1, min(limit, 100))
        if max_retries is None:
            max_retries = self._max_retries

        self._logger.info(f"Получаю топ-{limit} репозиториев страница {page}")
        try:
            data = await self._make_request_retry(
                endpoint="search/repositories",
                resource="search/repositories",
                method="GET",
                params={
                    "q": "stars:>1",
                    "sort": "stars",
                    "order": "desc",
                    "page": page,
                    "per_page": limit,
                },
                max_retries=3,
                wait_reset_sec_max=5,
            )
        except RetryFailedError as ex:
            self._logger.error(f"Не удалось получить топ-{limit} репозиториев: {ex}")
            return []
        if "items" not in data:
            return []
        return [Repository.from_api(x, pos) for pos, x in enumerate(data["items"])]

    def _commits_to_models(
        self,
        owner: str,
        repo: str,
        items: list[dict[str, Any]],
    ) -> list[RepositoryAuthorCommitsNum]:
        authors: dict[str, RepositoryAuthorCommitsNum] = {}
        for item in items:
            sha = item.get("sha", "Unknown")
            author = item.get("commit", {}).get("author", {}).get("name", None)
            if author is None:
                self._logger.warning(
                    f"Отсутствует информация об авторе коммита {sha} в репозитории {owner}/{repo}"
                )
                continue
            if author not in authors:
                authors[author] = RepositoryAuthorCommitsNum(
                    author=author,
                    commits_num=0,
                )
            authors[author].commits_num += 1
        return list(authors.values())

    async def _get_repository_commits_page(
        self,
        owner: str,
        repo: str,
        since: str,
        page: int,
        max_retries: int | None = None,
    ) -> list[dict[str, Any]]:
        if max_retries is None:
            max_retries = self._max_retries

        URL = f"repos/{owner}/{repo}/commits"
        try:
            data = await self._make_request_retry(
                URL,
                resource="repos/commits",
                method="GET",
                params={
                    "per_page": 100,
                    "page": page,
                    "since": since,
                },
                max_retries=max_retries,
                wait_reset_sec_max=5,
            )
            return data
        except RetryFailedError as ex:
            self._logger.error(
                f"Не удалось получить страницу {page} коммитов репозитория {owner}/{repo}: {ex}"
            )
            return []

    async def _get_repository_commits(
        self,
        owner: str,
        repo: str,
        max_pages: int = 100,
        since: datetime | None = None,
    ) -> list[RepositoryAuthorCommitsNum]:
        """
        GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits
        """

        if since is None:
            now = datetime.now(tz)
            since = now.replace(hour=0, minute=0, second=0, microsecond=0)

        since_utc = since.astimezone(timezone.utc)
        since_str = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        page = 1
        items = []
        self._logger.info(f"Получаю коммиты репозитория {owner}/{repo} с {since_str}")
        for page in range(1, max_pages + 1):
            self._logger.debug(
                f"Получаю страницу {page} коммитов репозитория {owner}/{repo}"
            )
            try:
                data = await self._get_repository_commits_page(
                    owner,
                    repo,
                    since_str,
                    page,
                )
                if not data:
                    break
                items.extend(data)
            except Exception as ex:
                self._logger.error(
                    f"Ошибка при получении коммитов страницы {page} репозитория {owner}/{repo}: {ex}"
                )
                break
        return self._commits_to_models(owner, repo, items)

    async def _get_all_repositories(
        self,
        qty: int = 100,
        limit: int = 100,
    ) -> list[Repository]:
        if limit < 0:
            raise ValueError("limit должен быть положительным числом")
        if qty <= 0:
            raise ValueError("qty должен быть положительным числом")
        if qty > 1000:
            self._logger.warning(
                "qty не может быть больше 1000, будет установлен в 1000"
            )
            qty = 1000
        if limit > 100:
            self._logger.warning(
                "limit не может быть больше 100, будет установлен в 100"
            )

        limit = max(1, min(limit, 100))
        qty = max(1, min(qty, 1000))

        self._logger.info(f"Получаю топ-{qty} репозиториев на GitHub")
        pages = math.ceil(qty / limit)
        tasks = [
            self._get_top_repositories(
                limit=min(limit, qty - page * limit),
                page=page + 1,
            )
            for page in range(pages)
        ]
        repositories: list[Repository] = []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for page, res in enumerate(results):
            page_limit = min(limit, qty - page * limit)
            start = page * limit
            end = start + page_limit
            if isinstance(res, BaseException):
                self._logger.error(
                    f"Ошибка при получении репозиториев {start}-{end}: {res}"
                )
                continue
            repositories.extend(res)

        self._logger.info(f"Получено {len(repositories)} репозиториев")
        return repositories

    async def get_repositories(
        self,
        qty: int = 100,
        limit: int = 100,
    ) -> list[Repository]:
        if limit < 0:
            raise ValueError("limit должен быть положительным числом")
        if qty <= 0:
            raise ValueError("qty должен быть положительным числом")
        if qty > 1000:
            self._logger.warning(
                "qty не может быть больше 1000, будет установлен в 1000"
            )
        if limit > 100:
            self._logger.warning(
                "limit не может быть больше 100, будет установлен в 100"
            )

        limit = max(1, min(limit, 100))
        qty = max(1, min(qty, 1000))

        repositories = await self._get_all_repositories(qty=qty, limit=limit)

        self._logger.info("Формирую список задач для получения коммитов репозиториев")
        tasks = [self._get_repository_commits(r.owner, r.name) for r in repositories]
        commits_lists = await asyncio.gather(*tasks, return_exceptions=True)
        for repo, commits in zip(repositories, commits_lists):
            if isinstance(commits, BaseException):
                self._logger.error(
                    f"Ошибка при получении коммитов {repo.name}: {commits}"
                )
                continue
            repo.add_commits(commits)
        return repositories

    async def close(self):
        await self._session.close()
