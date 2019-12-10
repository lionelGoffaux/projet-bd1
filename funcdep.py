import utils
import sqlite3
import os


class DB:
    """
    Cette classe représente une base de données
    que l'on veut manipuler.
    """

    def __init__(self, db_name: str):
        self._name = db_name
        self._path = os.path.abspath(self._name)
        self.conn = sqlite3.connect(self._path)

    @property
    def name(self) -> str:
        return self._name

    @property
    def tables(self) -> list:
        c = self.conn.cursor()
        c.execute('SELECT name FROM sqlite_master WHERE type="table";')
        return [t[0] for t in c.fetchall()]

    def get_fields(self, table: str) -> list:
        c = self.conn.cursor()
        c.execute('PRAGMA table_info(' + table + ')')
        return [t[1] for t in c.fetchall()]

    def close(self):
        self.conn.close()
