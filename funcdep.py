import sqlite3
import os


class DB:
    """
    Cette classe représente une base de données
    que l'on veut manipuler.
    """

    def __init__(self, db_name: str):
        self._db_name = db_name
        self._db_path = os.path.abspath(self._db_name)
        self.conn = sqlite3.connect(self._db_path)

    @property
    def get_db_name(self) -> str:
        return self._db_name

    def close(self):
        self.conn.close()
