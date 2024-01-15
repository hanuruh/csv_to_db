import psycopg2
from psycopg2.extras import execute_values


class Database:
    def __init__(self):
        self._database_connection = psycopg2.connect(host='localhost', dbname='postgres', port=5432)

    def close(self) -> None:
        self._database_connection.close()

    def query(self, sql: str, params=None) -> list[tuple]:
        with self._database_connection.cursor() as cursor:
            cursor.execute(sql, vars=params)
            rows = cursor.fetchall()
            return rows

    def insert(self, sql: str, params=None, get_return_value=False) -> tuple:
        with self._database_connection.cursor() as cursor:
            cursor.execute(sql, params)

            if get_return_value:
                try:
                    result = cursor.fetchone()

                    if result:
                        return result[0]
                except:
                    pass

    def execute_batch(self, sql: str, chunk: list[tuple]) -> None:
        with self._database_connection.cursor() as cursor:
            execute_values(cursor, sql, chunk, page_size=1000000)

    def commit(self) -> None:
        self._database_connection.commit()
