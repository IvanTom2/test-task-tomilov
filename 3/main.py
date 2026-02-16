import asyncio

from logger import logger
from settings import settings

from src.db import ClickHouseRepository
from src.use_case import CollectAndSaveGitHubRepositoriesUseCase

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
        db_repository = ClickHouseRepository(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD.get_secret_value(),
            database=settings.CLICKHOUSE_DATABASE,
            logger=logger,
            batch_size=1000,
            connect_timeout=10.0,
            send_receive_timeout=30.0,
        )
        await db_repository.init()

        use_case = CollectAndSaveGitHubRepositoriesUseCase(
            scrapper=scrapper,
            db_repository=db_repository,
            logger=logger,
        )
        await use_case.execute(qty=1000, limit=100)

    finally:
        await scrapper.close()
        await cache.close()
        await db_repository.close()


if __name__ == "__main__":
    asyncio.run(main())
