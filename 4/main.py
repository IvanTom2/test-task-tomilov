import asyncio

from logger import logger
from settings import settings
from src.db import ClickHouseDataBaseRepository


async def main():
    db_repository = ClickHouseDataBaseRepository(
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
    query_stats = await db_repository.get_views(campaign_id=1111111)
    for query, stats in query_stats.items():
        print(query, stats)


if __name__ == "__main__":
    asyncio.run(main())
