from dataclasses import dataclass
from typing import Any




@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int

    @classmethod
    def from_api(cls, api_data: dict[str, Any]) -> "RepositoryAuthorCommitsNum":
        return cls(
            author=api_data["author"]["login"],
            commits_num=api_data["total"],
        )


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]

    @classmethod
    def from_api(
        cls,
        api_data: dict[str, Any],
        position: int,
        commits: list[RepositoryAuthorCommitsNum] | None = None,
    ) -> "Repository":
        if commits is None:
            commits = []
        return cls(
            name=api_data["name"],
            owner=api_data["owner"]["login"],
            position=position,
            stars=api_data["stargazers_count"],
            watchers=api_data["watchers_count"],
            forks=api_data["forks_count"],
            language=api_data.get("language", "Unknown"),
            authors_commits_num_today=commits,
        )

    def add_commits(self, commits: list[RepositoryAuthorCommitsNum]) -> None:
        if not self.authors_commits_num_today:
            self.authors_commits_num_today = commits
            return None
        author_to_commit = {
            x.author: i for i, x in enumerate(self.authors_commits_num_today)
        }
        for commit in commits:
            if commit.author in author_to_commit:
                idx = author_to_commit[commit.author]
                self.authors_commits_num_today[idx].commits_num += commit.commits_num
            else:
                self.authors_commits_num_today.append(commit)
