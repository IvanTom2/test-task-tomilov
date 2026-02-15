from abc import ABC
from abc import abstractmethod

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
    async def save_repositories(
        self,
        repositories: list[Repository],
    ) -> None:
        pass
