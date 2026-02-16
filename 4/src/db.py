from abc import ABC
from abc import abstractmethod
from logging import Logger

import clickhouse_connect


class IDataBaseRepository(ABC):
    @abstractmethod
    async def get_views(self, campaign_id: int) -> dict[str, list[tuple[int, int]]]:
        pass

    @abstractmethod
    async def init(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class ClickHouseDataBaseRepository(IDataBaseRepository):
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

    async def get_views(self, campaign_id: int) -> dict[str, list[tuple[int, int]]]:
        query = """
                    SELECT
                        phrase,
                        arrayReverse(
                            arrayFilter(
                                x -> x.2 > 0,
                                arrayMap(
                                    (hour_val, diff_val) -> (hour_val, diff_val),
                                    hours,
                                    arrayDifference(views_array)
                                )
                            )
                        ) AS views_by_hour
                    FROM
                        (
                            SELECT
                                phrase,
                                groupArray(h) AS hours,
                                groupArray(max_v) AS views_array
                            FROM
                                (
                                    SELECT
                                        phrase,
                                        toHour(dt) AS h,
                                        max(views) AS max_v
                                    FROM
                                        phrases_views
                                    WHERE
                                        campaign_id = {campaign_id:Int32}
                                        AND toDate(dt) = today()
                                    GROUP BY
                                        phrase,
                                        h
                                    ORDER BY
                                        h ASC
                                )
                            GROUP BY
                                phrase
                        )
                """

        parameters = {"campaign_id": campaign_id}
        result = await self.client.query(query, parameters=parameters)
        stats = dict()
        for row in result.named_results():
            stats[row["phrase"]] = row["views_by_hour"]
        return stats

    async def close(self) -> None:
        if not hasattr(self, "_client"):
            return None
        await self._client.close()
