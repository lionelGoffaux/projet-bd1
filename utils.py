import functools
import copy
import sqlite3


def list2str(l: list):
    if len(l) == 0:
        return ''
        
    return functools.reduce(lambda a, b: str(a)+' '+str(b), l)

def get_all_subset(attributes: list):
    res = [[]]

    if len(attributes) == 0:
        return res
    att_copy = copy.deepcopy(attributes)

    for att in attributes:
        att_copy.remove(att)
        for sub in get_all_subset(att_copy):
            res.append([att]+sub)

    return res

def get_sql_statements(sql_file: str) -> list:
    result = ''
    with open(sql_file) as file:
        result = functools.reduce(lambda x, y: x + y.strip(), file.readlines()).replace('\n', '')

    return result.split(';')

def execute_sql_file(c: sqlite3.Cursor, sql_file: str) -> None:
    for s in get_sql_statements(sql_file):
        c.execute(s)

def print_list(l: iter):
    print()
    for e in l:
        print('- ' + str(e))
    print()
