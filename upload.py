import psycopg2
import os
from psycopg2.extras import _cursor, _connection
import sys
import openpyxl

DB_PASS = os.environ.get('POSTGRES_PASS')
DB_PORT = os.environ.get('POSTGRES_PORT')
DB_NAME = 'upload'
USER = 'postgres'
TABLE_NAME = 'endpoint_names'


class Upload:
    connect: _connection
    cursor: _cursor
    filepath: str


    def create_table_if_not_exists(self) -> None:
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ( id serial primary key, endpoint_id INTEGER UNIQUE , endpoint_name VARCHAR (50));")
        self.connect.commit()


    def __init__(self, filepath: str):
        self.connect = psycopg2.connect(database=DB_NAME, user=USER, password=DB_PASS)
        self.filepath = filepath
        self.cursor = self.connect.cursor()
        self.create_table_if_not_exists()


    def run(self):
        wb_obj = openpyxl.load_workbook(self.filepath)
        one, two = wb_obj.active.columns
        one = one[1:]
        two = two[1:]
        existing_ids = self._existing_endpoint_ids()

        for col1, col2 in zip(one, two):
            if col1.internal_value is None:
                continue

            _id = int(col1.internal_value)
            value = str(col2.internal_value)

            if _id in existing_ids.keys():
                if existing_ids.get(_id, [1]) == value:
                    continue
                else:
                    self._update_in_db(_id, value)
            else:
                self._insert_in_db(_id, value)

        self.connect.commit()


    def _existing_endpoint_ids(self):
        try:
            self.cursor.execute(f'SELECT endpoint_id, endpoint_name from {TABLE_NAME}')
            return {id: name for id, name in self.cursor.fetchall()}
        except TypeError:
            return set()


    def _insert_in_db(self, _id: int, value: str):
        self.cursor.execute(f"INSERT INTO {TABLE_NAME} (endpoint_id, endpoint_name) VALUES ({_id}, '{value}')")


    def _update_in_db(self, _id: int, value: str):
        self.cursor.execute(f"UPDATE {TABLE_NAME} SET endpoint_name = '{value}' WHERE endpoint_id = {_id}")


assert len(sys.argv) == 2, 'only 1 argument'
# path = "названия точек.xlsm"
upload = Upload(sys.argv[1])
upload.run()
