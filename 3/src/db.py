import asyncio
from datetime import datetime
from logging import Logger

import clickhouse_connect

from settings import timezone
from src.use_case import IDatabaseRepository
from src.models import Repository


class ClickHouseRepository(IDatabaseRepository):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        logger: Logger,
        batch_size: int = 1000,
        connect_timeout: float = 10.0,
        send_receive_timeout: float = 30.0,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._logger = logger
        self._batch_size = batch_size
        self._connect_timeout = connect_timeout
        self._send_receive_timeout = send_receive_timeout

    async def init(self) -> None:
        self._client = await clickhouse_connect.get_async_client(
            host=self._host,
            port=self._port,
            username=self._user,
            password=self._password,
            database=self._database,
            connect_timeout=self._connect_timeout,
            send_receive_timeout=self._send_receive_timeout,
        )

    @property
    def client(self):
        if not hasattr(self, "_client"):
            raise RuntimeError(
                "Клиент базы данных не инициализирован: запустите init()"
            )
        return self._client

    async def save_repositories(self, repositories: list[Repository]) -> None:
        updated = datetime.now(timezone)
        batch = []
        column_names = [
            "name",
            "owner",
            "stars",
            "watchers",
            "forks",
            "language",
            "updated",
        ]
        for repo in repositories:
            batch.append(
                (
                    repo.name,
                    repo.owner,
                    repo.stars,
                    repo.watchers,
                    repo.forks,
                    repo.language,
                    updated,
                )
            )
            if len(batch) >= self._batch_size:
                await self._client.insert(
                    "repositories",
                    batch,
                    column_names,
                )
                batch.clear()
        if batch:
            await self._client.insert(
                "repositories",
                batch,
                column_names,
            )

    async def save_commits(self, repositories: list[Repository]) -> None:
        batch = []
        column_names = ["repository", "author", "commits"]
        for repo in repositories:
            for author_commit in repo.authors_commits_num_today:
                batch.append(
                    (
                        repo.name,
                        author_commit.author,
                        author_commit.commits_num,
                    )
                )
                if len(batch) >= self._batch_size:
                    await self._client.insert(
                        "repositories_authors_commits",
                        batch,
                        column_names,
                    )
                    batch.clear()
        if batch:
            await self._client.insert(
                "repositories_authors_commits",
                batch,
                column_names,
            )

    async def save_positions(self, repositories: list[Repository]) -> None:
        batch = []
        column_names = ["repository", "position", "language"]
        for repo in repositories:
            batch.append(
                (
                    repo.name,
                    repo.position,
                    repo.language,
                )
            )
            if len(batch) >= self._batch_size:
                await self._client.insert(
                    "repositories_positions",
                    batch,
                    column_names,
                )
                batch.clear()
        if batch:
            await self._client.insert(
                "repositories_positions",
                batch,
                column_names,
            )

    async def save_repositories_commits_positions(
        self,
        repositories: list[Repository],
    ) -> None:
        try:
            tasks = [
                self.save_repositories(repositories),
                self.save_commits(repositories),
                self.save_positions(repositories),
            ]
            errors = await asyncio.gather(*tasks, return_exceptions=True)
            for err in errors:
                if isinstance(err, BaseException):
                    self._logger.error(err)
                    raise err

        except Exception as ex:
            raise ex

    async def close(self) -> None:
        if not hasattr(self, "_client"):
            return None
        await self._client.close()
