import asyncio

from logger import logger
from settings import settings
from src.scraper import GithubReposScrapper
from src.rate_limit import RateLimit
from src.rate_limit import SlidingWindowDequeRateLimiterResourceExtended
from src.cache import InMemoryLRUAPICache


async def main():
    try:
        cache = InMemoryLRUAPICache(maxlen=1000)
        # лимиты определены с запасом по докам GitHub API и response headers
        sw_rate_limit = SlidingWindowDequeRateLimiterResourceExtended(
            logger=logger,
            limit=RateLimit(
                max_concurrent=50,  # 100 max по докам
                max_requests_per_time=4500,  # 5000 в час по докам
                time_window_seconds=60 * 60,  # часовой интервал
            ),
            resource_limits={
                # search-лимит по ответам API 30 на 60 секунд, поставил с запасом
                "search/repositories": [
                    RateLimit(
                        max_concurrent=None,  # контроль concurrent с общего лимита
                        max_requests_per_time=20,  # 30 в минуту по ответам
                        time_window_seconds=60,  # минутный интервал
                    )
                ],
                # не обнаружил конкретных лимитов для эндпоинта repos/commits кроме общего
            },
        )
        scrapper = GithubReposScrapper(
            access_token=settings.GITHUB_TOKEN.get_secret_value(),
            logger=logger,
            rate_limit=sw_rate_limit,
            timeout=15,
            cache=cache,
        )
        repositories = await scrapper.get_repositories(qty=1000, limit=100)
        uniq = set()
        for repo in repositories:
            # print(repo)
            uniq.add((repo.owner, repo.name))

        print(f"Всего репозиториев получено: {len(repositories)}")
        print(f"Уникальных репозиториев: {len(uniq)}")

    finally:
        await scrapper.close()
        await cache.close()


if __name__ == "__main__":
    asyncio.run(main())
