from typing import AsyncGenerator

import asyncpg
from fastapi import Request

from settings import settings
from logger import logger


async def setup_asyncpg_pool(
    has_pgbouncer: bool = False,
) -> asyncpg.Pool:
    logger.info("Создаю пул подключений к БД asyncpg")
    pgbouncer_setup = {}
    if has_pgbouncer:
        logger.info("Отключены Prepared Statements ввиду наличия PgBouncer")
        pgbouncer_setup = {
            "statement_cache_size": 0,
            "max_cached_statement_lifetime": 0,
        }
    connection_params = {
        "user": settings.PG_USER,
        "password": settings.PG_PASSWORD.get_secret_value(),
        "database": settings.PG_DATABASE,
        "host": settings.PG_HOST,
        "port": settings.PG_PORT,
        "timeout": 60,
        "command_timeout": 5,
        **pgbouncer_setup,
    }
    pool = await asyncpg.create_pool(
        **connection_params,
        min_size=10,
        max_size=20,
        max_queries=50000,
        max_inactive_connection_lifetime=300.0,
    )
    logger.info("Пул подключений к БД asyncpg создан")
    return pool


async def get_pg_connection(
    request: Request,
) -> AsyncGenerator[asyncpg.Connection, None]:
    logger.debug("Получаю подключение к БД из пула")
    pool: asyncpg.Pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        yield conn
    logger.debug("Подключение к БД возвращено в пул")
