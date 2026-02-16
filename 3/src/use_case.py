from abc import ABC
from abc import abstractmethod
from logging import Logger

from src.models import Repository


class IGithubReposScrapper(ABC):
    @abstractmethod
    async def get_repositories(
        self,
        qty: int = 100,
        limit: int = 100,
    ) -> list[Repository]:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class IDatabaseRepository(ABC):
    @abstractmethod
    async def save_repositories(self, repositories: list[Repository]) -> None:
        pass

    @abstractmethod
    async def save_commits(self, repositories: list[Repository]) -> None:
        pass

    @abstractmethod
    async def save_positions(self, repositories: list[Repository]) -> None:
        pass

    @abstractmethod
    async def save_repositories_commits_positions(
        self,
        repositories: list[Repository],
    ) -> None:
        pass

    @abstractmethod
    async def init(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class ICollectAndSaveGitHubRepositoriesUseCase(ABC):
    @abstractmethod
    async def execute(self, qty: int = 100, limit: int = 100) -> None:
        pass


class CollectAndSaveGitHubRepositoriesUseCase(ICollectAndSaveGitHubRepositoriesUseCase):
    def __init__(
        self,
        scrapper: IGithubReposScrapper,
        db_repository: IDatabaseRepository,
        logger: Logger,
    ) -> None:
        self._scrapper = scrapper
        self._db_repository = db_repository
        self._logger = logger

    async def execute(self, qty: int = 100, limit: int = 100) -> None:
        try:
            self._logger.info("Начинаю сбор данных о репозиториях и коммитах в GitHub")
            repositories = await self._scrapper.get_repositories(qty, limit)
            await self._db_repository.save_repositories_commits_positions(repositories)
        except Exception as ex:
            self._logger.error(
                f"Ошибка при сборе данных о репозиториях и коммитах в GitHub: {ex}"
            )
            raise ex
