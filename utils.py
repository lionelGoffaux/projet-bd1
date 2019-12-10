import functools
import sqlite3


def get_sql_statements(sql_file: str) -> list:
    result = ''
    with open(sql_file) as file:
        result = functools.reduce(lambda x, y: x + y.strip(), file.readlines()).replace('\n', '')

    return result.split(';')


def execute_sql_file(c: sqlite3.Cursor, sql_file: str) -> None:
    for s in get_sql_statements(sql_file):
        c.execute(s)
