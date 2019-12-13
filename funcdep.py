import sqlite3
import os
import functools

import utils

class DB:
    """
    Cette classe représente une base de données
    que l'on veut manipuler.
    """

    def __init__(self, db_name: str):
        self._name = db_name
        self._path = os.path.abspath(self._name)
        self._conn = sqlite3.connect(self._path)

    @property
    def has_df_table(self):
        """True si la table des DF existe dans la base de données"""
        return 'FuncDep' in self.tables

    @property
    def name(self) -> str:
        return self._name

    @property
    def tables(self) -> list:
        c = self._conn.cursor()
        c.execute('SELECT name FROM sqlite_master WHERE type="table";')
        return [t[0] for t in c.fetchall()]

    def get_fields(self, table: str) -> list:

        # La table doit exister
        if table not in self.tables:
            raise UnknowTableError()

        # La table n'est pas celle des DF
        if table == 'FuncDep':
            raise DFTableError()

        c = self._conn.cursor()
        c.execute('PRAGMA table_info(' + table + ')')  # TODO: use prepare request 
        return [t[1] for t in c.fetchall()]

    def add_df(self, table: str, lhs: str, rhs:str):

        # La table doit exister
        if table not in self.tables:
            raise UnknowTableError()

        # La table n'est pas celle des DF
        if table == 'FuncDep':
            raise DFTableError()

        table_fields = self.get_fields(table)

        # Tous les champs de la prémise existent dans la table
        for field in lhs.split():
            if field not in table_fields:
                raise UnknowFieldsError()

        # La doit être singulière
        if len(rhs.split()) > 1:
            raise DFNotSingularError()

        # Le champ de déffini doit exister dans la table
        if rhs not in self.get_fields(table):
            raise UnknowFieldsError()

        # Le champ rhs ne doit pas être dans les champs lhs
        if rhs in lhs.split():
            raise RHSIncludeToLHSError()

        c = self._conn.cursor()

        # On crée la tables des DF si besoin
        if not self.has_df_table:
            utils.execute_sql_file(c, os.path.join('misc', 'init_df_table.sql'))

        try:
            c.execute('INSERT INTO `FuncDep` VALUES (?, ?, ?)', (table, lhs, rhs))
        except sqlite3.IntegrityError:
            raise DFAddTwiceError()

    def del_df(self, table: str, lhs: str, rhs: str):
        df =  (table, lhs, rhs)

        # La table doit exister
        if table not in self.tables:
            raise UnknowTableError()

        # La DF doit exister
        if df not in self.list_df():
            raise DFNotFoundError()

        c = self._conn.cursor()

        c.execute('DELETE FROM `FuncDep` WHERE `table` = ? AND `lhs` = ? AND `rhs` = ?', df)

    def list_df(self) -> list:
        c = self._conn.cursor()

        # La table doit exister
        if not self.has_df_table:
            return []
        
        c.execute('SELECT * FROM `FuncDep`')
        return c.fetchall()

    def list_table_df(self, table: str) -> list:
        # La table doit exister
        if table not in self.tables:
            raise UnknowTableError()

        c = self._conn.cursor()

        if not self.has_df_table:
            return []
        
        c.execute('SELECT * FROM `FuncDep` WHERE `table` = ?', (table,))
        return c.fetchall()

    def purge_df(self):
        c = self._conn.cursor()
        c.execute('DELETE FROM `FuncDep`')

    def _check_df_set(self, dfs: list) -> dict:
        c = self._conn.cursor()
        res = {}

        for df in dfs:
            unique_lhs = c.execute('SELECT DISTINCT ' + df[1].replace(' ', ', ') + ' FROM ' + df[0] + ';') 

            for lhs in unique_lhs:
                conditions = functools.reduce(lambda a, b : a + ' AND ' + b, ['{}={}'.format(f, v.__repr__()) for f, v in zip(df[1].split(), lhs)])

                c.execute('SELECT DISTINCT {} FROM {} WHERE {};'.format(df[2], df[0], conditions))
                assos = c.fetchall()

                res[df] = [ t  for t in c.execute('SELECT DISTINCT * FROM {} WHERE {};'.format(df[0], conditions)) ] if len(assos) > 1 else []

        return res

    def check_df(self) -> dict:
        """Vérifie si les DF sont respectées"""
        return self._check_df_set(self.list_df())

    def check_table_df(self, table: str) -> dict:
        """Vérifie si les DF sont respectées"""

        # La table doit exister
        if table not in self.tables:
            raise UnknowTableError()

        return self._check_df_set(self.list_table_df(table))

    def close(self):
        self._conn.commit()
        self._conn.close()


class UnknowTableError(Exception):
    pass


class UnknowFieldsError(Exception):
    pass


class DFNotFoundError(Exception):
    pass


class DFNotSingularError(Exception):
    pass


class DFAddTwiceError(Exception):
    pass


class DFTableError(Exception):
    pass

class RHSIncludeToLHSError(Exception):
    pass
