from typing import Annotated
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Depends
from fastapi import HTTPException

from settings import settings
from logger import logger

from db import setup_asyncpg_pool
from db import get_pg_connection


DB_VERSION_FAIL_MESSAGE = "Не удалось получить версию БД"


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)],
):
    try:
        logger.debug("Выполняю запрос на получение версии БД")
        return await conn.fetchval("SELECT version()", timeout=5)
    except Exception as ex:
        logger.error(f"Ошибка при получении версии БД: {ex}")
        raise HTTPException(
            status_code=500,
            detail=DB_VERSION_FAIL_MESSAGE,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Инициализация ресурсов")

    try:
        app.state.db_pool = await setup_asyncpg_pool(
            has_pgbouncer=settings.HAS_PGBOUNCER,
        )
    except Exception as ex:
        logger.critical(f"Ошибка при инициализации пула подключений к БД: {ex}")
        raise RuntimeError("Не удалось инициализировать пул подключений к БД") from ex

    yield

    try:
        logger.info("Закрываю пул подключений к БД")
        await app.state.db_pool.close()
        logger.info("Пул подключений к БД закрыт")
    except Exception as ex:
        logger.critical(f"Ошибка при закрытии пула подключений к БД: {ex}")


def register_routes(app: FastAPI):
    logger.info("Регистрирую маршруты")
    router = APIRouter(prefix="/api")
    router.add_api_route(
        path="/db_version",
        endpoint=get_db_version,
        methods=["GET"],
        responses={
            200: {"description": "Успешный ответ с версией БД"},
            500: {"description": "Внутренняя ошибка сервера"},
        },
        response_model=str,
        summary="Получить версию базы данных",
        description="Возвращает строку с версией PostgreSQL.",
        tags=["База данных"],
    )
    app.include_router(router)
    logger.info("Маршруты зарегистрированы")


def create_app() -> FastAPI:
    logger.info("Создаю FastAPI приложение")
    app = FastAPI(
        title="e-Comet",
        lifespan=lifespan,
    )
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
